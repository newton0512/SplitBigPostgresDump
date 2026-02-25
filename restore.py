#!/usr/bin/env python3
"""
Idempotent restore of a database from split dump chunks.
Runs 01_preamble, 02_schema_*, 03_data_* (with row-count resume), 04_indexes, 05_functions, 06_views_triggers.
State in restore_state.json tracks rows loaded per data file so restore can resume.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from constants import (
    OUTPUT_DIR,
    PG_DATABASE,
    PG_HOST,
    PG_PORT,
    PG_USER,
    RESTORE_STATE_FILENAME,
)

# Chunk name patterns in execution order
PREAMBLE_GLOB = "01_preamble.sql"
SCHEMA_GLOB = "02_schema_*.sql"
DATA_GLOB = "03_data_*_part*.sql"
DATA_META_SUFFIX = ".meta.json"
INDEXES_GLOB = "04_indexes*.sql"
FUNCTIONS_GLOB = "05_functions*.sql"
VIEWS_TRIGGERS_GLOB = "06_views_triggers*.sql"

COPY_FROM_STDIN_RE = re.compile(r"^\s*COPY\s+.+FROM\s+stdin\s*;?\s*$", re.IGNORECASE)
OUTPUT_LINE_TERMINATOR = "\n"


def setup_logging(chunks_dir: Path) -> logging.Logger:
    logger = logging.getLogger("restore")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    log_path = chunks_dir / "restore.log"
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def load_restore_state(chunks_dir: Path) -> dict:
    p = chunks_dir / RESTORE_STATE_FILENAME
    if not p.exists():
        return {"data_files": {}}
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def save_restore_state(chunks_dir: Path, state: dict, logger: logging.Logger) -> None:
    p = chunks_dir / RESTORE_STATE_FILENAME
    with open(p, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    logger.debug("Restore state saved.")


def run_psql_file(
    chunks_dir: Path,
    path: Path,
    psql_cmd: list[str],
    logger: logging.Logger,
    *,
    stdin_from_file: bool = False,
) -> bool:
    """Run psql -f path, or pass file content via stdin (for docker exec). Returns True on success."""
    if stdin_from_file:
        sql_content = path.read_text(encoding="utf-8", errors="replace")
        cmd = psql_cmd
        logger.info("Executing %s (via stdin)", path.name)
        try:
            r = subprocess.run(
                cmd,
                cwd=str(chunks_dir),
                input=sql_content,
                capture_output=True,
                text=True,
                encoding="utf-8",
                timeout=3600,
            )
            if r.returncode != 0:
                logger.error("psql failed for %s: %s", path.name, r.stderr or r.stdout)
                return False
            return True
        except subprocess.TimeoutExpired:
            logger.error("psql timed out for %s", path.name)
            return False
        except Exception as e:
            logger.exception("psql error for %s: %s", path.name, e)
            return False
    else:
        cmd = psql_cmd + ["-f", str(path)]
        logger.info("Executing %s", path.name)
        try:
            r = subprocess.run(
                cmd, cwd=str(chunks_dir), capture_output=True, text=True, encoding="utf-8", timeout=3600
            )
            if r.returncode != 0:
                logger.error("psql failed for %s: %s", path.name, r.stderr or r.stdout)
                return False
            return True
        except subprocess.TimeoutExpired:
            logger.error("psql timed out for %s", path.name)
            return False
        except Exception as e:
            logger.exception("psql error for %s: %s", path.name, e)
            return False


def run_psql_copy_stdin(
    chunks_dir: Path,
    copy_header_line: str,
    data_lines: list[str],
    psql_cmd: list[str],
    logger: logging.Logger,
) -> bool:
    """Run psql -c 'COPY ... FROM stdin' and feed data_lines + '\\.' to stdin."""
    cmd = psql_cmd + ["-c", copy_header_line.rstrip()]
    data = OUTPUT_LINE_TERMINATOR.join(data_lines) + OUTPUT_LINE_TERMINATOR + "\\." + OUTPUT_LINE_TERMINATOR
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(chunks_dir),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        out, err = proc.communicate(input=data, timeout=3600)
        if proc.returncode != 0:
            logger.error("COPY stdin failed: %s", err or out)
            return False
        return True
    except subprocess.TimeoutExpired:
        proc.kill()
        logger.error("COPY stdin timed out")
        return False
    except Exception as e:
        logger.exception("COPY stdin error: %s", e)
        return False


def run_truncate_table(
    schema: str,
    table: str,
    psql_cmd: list[str],
    chunks_dir: Path,
    logger: logging.Logger,
) -> bool:
    """Run TRUNCATE schema.table before first COPY to avoid duplicate key when table already has data."""
    # Имя с кавычками на случай mixed case
    quoted = f'"{schema}"."{table}"'
    cmd = psql_cmd + ["-c", f"TRUNCATE TABLE {quoted} CASCADE;"]
    try:
        r = subprocess.run(cmd, cwd=str(chunks_dir), capture_output=True, text=True, encoding="utf-8", timeout=60)
        if r.returncode != 0:
            logger.warning("TRUNCATE %s failed (table may not exist yet): %s", quoted, (r.stderr or r.stdout).strip())
            return False
        logger.debug("TRUNCATE %s", quoted)
        return True
    except Exception as e:
        logger.warning("TRUNCATE %s error: %s", quoted, e)
        return False


def stream_copy_remaining(
    data_path: Path,
    meta: dict,
    rows_loaded: int,
    psql_cmd: list[str],
    logger: logging.Logger,
    *,
    truncate_before_first: bool = False,
    chunks_dir: Path | None = None,
) -> tuple[bool, int]:
    """
    For a data chunk file: skip first line (COPY header) and rows_loaded data lines,
    then run COPY ... FROM stdin with the remaining lines + '\\.'
    Returns (success, total_rows_loaded_after_this_run).
    """
    total_rows = meta.get("rows", 0)
    if rows_loaded >= total_rows:
        logger.info("Skip %s (already loaded %s/%s rows)", data_path.name, rows_loaded, total_rows)
        return True, rows_loaded

    with open(data_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        copy_header_line = f.readline()
        if not COPY_FROM_STDIN_RE.match(copy_header_line.strip()):
            logger.error("%s: first line is not COPY ... FROM stdin", data_path.name)
            return False, rows_loaded
        for _ in range(rows_loaded):
            line = f.readline()
            if not line:
                break
            if line.rstrip("\r\n") == "\\.":
                logger.warning("%s: hit \\. before expected; rows_loaded may be wrong", data_path.name)
                return False, rows_loaded
        remaining_lines: list[str] = []
        while True:
            line = f.readline()
            if not line:
                break
            norm = line.rstrip("\r\n")
            if norm == "\\.":
                break
            remaining_lines.append(line.rstrip("\r\n"))
        # remaining_lines now has only data rows (we consumed \\.)

    if not remaining_lines and rows_loaded == total_rows:
        return True, total_rows

    if truncate_before_first and chunks_dir is not None:
        schema_name = meta.get("schema", "public")
        table_name = meta.get("table", "")
        run_truncate_table(schema_name, table_name, psql_cmd, chunks_dir, logger)

    logger.info("Loading %s: rows %s..%s (%s rows)", data_path.name, rows_loaded + 1, rows_loaded + len(remaining_lines), len(remaining_lines))
    ok = run_psql_copy_stdin(Path(data_path.parent), copy_header_line, remaining_lines, psql_cmd, logger)
    if not ok:
        return False, rows_loaded
    return True, rows_loaded + len(remaining_lines)


def sorted_data_chunks(chunks_dir: Path) -> list[Path]:
    """Return 03_data_*_part*.sql paths sorted by (schema, table, part)."""
    paths = list(chunks_dir.glob("03_data_*_part*.sql"))
    paths = [p for p in paths if ".repaired" not in p.name and ".meta" not in p.name]
    paths.sort(key=lambda p: p.name)
    return paths


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Restore DB from split dump chunks (idempotent, with row resume). "
        "Target schema is taken from the dump (e.g. public); set connection via CLI, constants.py, or PGHOST/PGPORT/PGUSER/PGDATABASE. "
        "Password: PGPASSWORD or .pgpass.",
    )
    ap.add_argument("chunks_dir", nargs="?", default=OUTPUT_DIR, help="Каталог с чанками 01_*.sql, 02_*.sql, ...")
    ap.add_argument("--psql", default="psql", help="Путь к psql")
    ap.add_argument("--db", "-d", default=PG_DATABASE, help="База данных (по умолчанию: constants.PG_DATABASE или PGDATABASE)")
    ap.add_argument("--host", default=PG_HOST, help="Хост PostgreSQL (по умолчанию: constants.PG_HOST или PGHOST)")
    ap.add_argument("--port", "-p", default=PG_PORT, help="Порт (по умолчанию: constants.PG_PORT или PGPORT)")
    ap.add_argument("--user", "-U", default=PG_USER, help="Пользователь (по умолчанию: constants.PG_USER или PGUSER)")
    ap.add_argument("--docker", metavar="CONTAINER", help="Запускать psql через docker exec -i CONTAINER psql (PostgreSQL в Docker)")
    ap.add_argument("--reset", action="store_true", help="Сбросить состояние восстановления и начать с начала")
    args = ap.parse_args()

    chunks_dir = Path(args.chunks_dir)
    if not chunks_dir.is_dir():
        print("Chunks directory not found:", chunks_dir, file=sys.stderr)
        sys.exit(1)

    logger = setup_logging(chunks_dir)
    state = load_restore_state(chunks_dir)
    if args.reset:
        state = {"preamble_done": False, "schema_done": False, "data_files": {}}
        save_restore_state(chunks_dir, state, logger)
        logger.info("Restore state reset.")
    if "preamble_done" not in state:
        state["preamble_done"] = False
    if "schema_done" not in state:
        state["schema_done"] = False
    if "data_files" not in state:
        state["data_files"] = {}

    if args.docker:
        # psql внутри контейнера: не нужен psql на хосте, файлы передаём через stdin
        psql_cmd = ["docker", "exec", "-i", args.docker, "psql"]
        if args.db is not None:
            psql_cmd.extend(["-d", str(args.db)])
        if args.user is not None:
            psql_cmd.extend(["-U", str(args.user)])
        # -h/-p внутри контейнера обычно не нужны (подключение к localhost)
        use_stdin_for_files = True
        logger.info("Restore via Docker container: %s", args.docker)
    else:
        psql_cmd = [args.psql]
        if args.db is not None:
            psql_cmd.extend(["-d", str(args.db)])
        if args.host is not None:
            psql_cmd.extend(["-h", str(args.host)])
        if args.port is not None:
            psql_cmd.extend(["-p", str(args.port)])
        if args.user is not None:
            psql_cmd.extend(["-U", str(args.user)])
        # Проверка наличия psql на хосте
        psql_exe = args.psql
        if os.path.sep in psql_exe or (os.name == "nt" and ":" in psql_exe):
            if not Path(psql_exe).exists():
                logger.error(
                    "psql не найден: %s. Укажите полный путь или используйте --docker CONTAINER",
                    psql_exe,
                )
                sys.exit(1)
        else:
            found = shutil.which(psql_exe) or (shutil.which(psql_exe + ".exe") if os.name == "nt" else None)
            if not found:
                logger.error(
                    "psql не найден в PATH. Добавьте PostgreSQL/bin в PATH или укажите --docker ИМЯ_КОНТЕЙНЕРА"
                )
                sys.exit(1)
            psql_cmd[0] = found
        use_stdin_for_files = False

    logger.info(
        "Restore target: db=%s host=%s port=%s user=%s (schema from dump, e.g. public)",
        args.db or "(PGDATABASE)",
        args.host or "(PGHOST/localhost)" if not args.docker else "(inside container)",
        args.port or "(PGPORT/5432)" if not args.docker else "-",
        args.user or "(PGUSER)",
    )

    # 01_preamble (skip if already done on resume)
    if not state.get("preamble_done"):
        p = chunks_dir / PREAMBLE_GLOB
        if p.exists():
            if not run_psql_file(chunks_dir, p, psql_cmd, logger, stdin_from_file=use_stdin_for_files):
                sys.exit(1)
            state["preamble_done"] = True
            save_restore_state(chunks_dir, state, logger)
        else:
            logger.debug("No %s", PREAMBLE_GLOB)
            state["preamble_done"] = True
            save_restore_state(chunks_dir, state, logger)
    else:
        logger.info("Skipping 01_preamble (already done).")

    # 02_schema_* (skip if already done on resume)
    if not state.get("schema_done"):
        schema_files = sorted(chunks_dir.glob(SCHEMA_GLOB))
        for path in schema_files:
            if not run_psql_file(chunks_dir, path, psql_cmd, logger, stdin_from_file=use_stdin_for_files):
                sys.exit(1)
        state["schema_done"] = True
        save_restore_state(chunks_dir, state, logger)
    else:
        logger.info("Skipping 02_schema_* (already done).")

    # 03_data_* (with resume)
    data_files = sorted_data_chunks(chunks_dir)
    for data_path in data_files:
        meta_path = data_path.parent / (data_path.stem + ".meta.json")
        if not meta_path.exists():
            logger.warning("No meta for %s, skipping", data_path.name)
            continue
        with open(meta_path, encoding="utf-8") as mf:
            meta = json.load(mf)
        schema_name = meta.get("schema", "public")
        table_name = meta.get("table", "")
        schema_file = chunks_dir / f"02_schema_{schema_name}_{table_name}.sql"
        if not schema_file.exists():
            logger.warning(
                "Нет файла схемы %s для таблицы %s.%s — пропускаем загрузку данных. Таблица может создаваться в 01_preamble (расширение) или в другом 02_schema_*.sql. При необходимости создайте таблицу и запустите restore снова.",
                schema_file.name,
                schema_name,
                table_name,
            )
            continue
        key = data_path.name
        rows_loaded = state.get("data_files", {}).get(key, 0)
        part_num = meta.get("part", 1)
        truncate_before = part_num == 1 and rows_loaded == 0
        ok, new_count = stream_copy_remaining(
            data_path, meta, rows_loaded, psql_cmd, logger,
            truncate_before_first=truncate_before,
            chunks_dir=chunks_dir if truncate_before else None,
        )
        if not ok:
            logger.error(
                "Если ошибка «relation ... does not exist»: таблица %s.%s не создана до загрузки данных. Проверьте, что 02_schema_%s_%s.sql выполнился без ошибок и что схема/таблица не создаются в 01_preamble (например расширением).",
                schema_name,
                table_name,
                schema_name,
                table_name,
            )
            sys.exit(1)
        state.setdefault("data_files", {})[key] = new_count
        save_restore_state(chunks_dir, state, logger)

    # 04_indexes, 05_functions, 06_views_triggers
    for pattern in (INDEXES_GLOB, FUNCTIONS_GLOB, VIEWS_TRIGGERS_GLOB):
        for path in sorted(chunks_dir.glob(pattern)):
            if not path.is_file():
                continue
            if not run_psql_file(chunks_dir, path, psql_cmd, logger, stdin_from_file=use_stdin_for_files):
                sys.exit(1)

    logger.info("Restore finished successfully.")


if __name__ == "__main__":
    main()
