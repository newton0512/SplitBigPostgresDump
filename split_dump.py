#!/usr/bin/env python3
"""
Split a large PostgreSQL plain SQL dump into logical chunks.
Streams the dump in fixed-size blocks, parses by line, writes chunks to OUTPUT_DIR.
State is saved so processing can resume after interruption.
"""
from __future__ import annotations

import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

from constants import (
    CARRY_OVER_SIDE_FILENAME,
    DUMP_FILENAME,
    INPUT_DIR,
    MAX_CARRY_OVER_IN_STATE_JSON,
    MAX_DATA_CHUNK_BYTES,
    MAX_STATE_JSON_BYTES,
    OUTPUT_DIR,
    OUTPUT_LINE_TERMINATOR,
    READ_BLOCK_BYTES,
    SPLIT_LOG_FILENAME,
    STATE_FILENAME,
)

# Regex: COPY schema.table (col1, col2, ...) FROM stdin;
COPY_FROM_STDIN_RE = re.compile(
    r"^\s*COPY\s+(.+?)\s+\((.+?)\)\s+FROM\s+stdin\s*;?\s*$",
    re.IGNORECASE,
)

# CREATE TABLE [schema.]table
CREATE_TABLE_RE = re.compile(
    r"^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(.+?)\s*\(",
    re.IGNORECASE,
)

# Post-data section starters
CREATE_INDEX_RE = re.compile(r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX", re.IGNORECASE)
CREATE_FUNCTION_RE = re.compile(
    r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+",
    re.IGNORECASE,
)
CREATE_VIEW_RE = re.compile(r"^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|MATERIALIZED\s+VIEW)", re.IGNORECASE)
CREATE_TRIGGER_RE = re.compile(r"^\s*CREATE\s+TRIGGER", re.IGNORECASE)
CREATE_RULE_RE = re.compile(r"^\s*CREATE\s+RULE", re.IGNORECASE)
ALTER_TABLE_OWNER_RE = re.compile(r"^\s*ALTER\s+TABLE\s+", re.IGNORECASE)
SELECT_PG_RE = re.compile(r"^\s*SELECT\s+pg_catalog\.pg_", re.IGNORECASE)

# Max size for post-data chunk files (same as data for simplicity; can split later)
MAX_POSTDATA_CHUNK_BYTES = MAX_DATA_CHUNK_BYTES


def _normalize_line(s: str) -> str:
    """Strip CR/LF for comparison (e.g. end-of-COPY line)."""
    return s.rstrip("\r\n")


def _parse_table_ref(ref: str) -> tuple[str, str]:
    """Parse 'schema.table' or 'table' into (schema, table)."""
    ref = ref.strip().strip('"')
    if "." in ref:
        parts = ref.rsplit(".", 1)
        return parts[0].strip('"'), parts[1].strip('"')
    return "public", ref


def _parse_columns(cols: str) -> list[str]:
    """Parse column list from COPY header into list of names."""
    return [c.strip().strip('"') for c in cols.split(",")]


def setup_logging(out_dir: Path) -> logging.Logger:
    log_path = out_dir / SPLIT_LOG_FILENAME
    logger = logging.getLogger("split_dump")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
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


def load_state(out_dir: Path) -> dict[str, Any] | None:
    p = out_dir / STATE_FILENAME
    if not p.exists():
        return None
    if p.stat().st_size > MAX_STATE_JSON_BYTES:
        logging.getLogger(__name__).warning(
            "State file %s is too large (%s MB), not loading to avoid OOM; starting from beginning",
            p, p.stat().st_size // (1024 * 1024),
        )
        return None
    try:
        with open(p, encoding="utf-8") as f:
            raw = f.read()
        raw = raw.strip()
        if not raw:
            logging.getLogger(__name__).warning("State file %s is empty, starting from beginning", p)
            return None
        state = json.loads(raw)
        # Большой carry_over хранится в отдельном файле, чтобы state.json не раздувался до гигабайт
        carry_file = state.pop("carry_over_file", None)
        if carry_file == CARRY_OVER_SIDE_FILENAME:
            side_path = out_dir / carry_file
            if side_path.exists():
                with open(side_path, encoding="utf-8") as f:
                    state["carry_over"] = f.read()
        if "carry_over" not in state:
            state["carry_over"] = ""
        return state
    except (json.JSONDecodeError, OSError) as e:
        logging.getLogger(__name__).warning("State file %s invalid or unreadable (%s), starting from beginning", p, e)
        return None


def save_state(out_dir: Path, state: dict[str, Any], logger: logging.Logger) -> None:
    """Пишем во временный файл и атомарно заменяем state.json. Большой carry_over — в отдельный файл."""
    state = dict(state)
    carry = state.get("carry_over") or ""
    if len(carry) > MAX_CARRY_OVER_IN_STATE_JSON:
        side_path = out_dir / CARRY_OVER_SIDE_FILENAME
        side_tmp = out_dir / (CARRY_OVER_SIDE_FILENAME + ".tmp")
        with open(side_tmp, "w", encoding="utf-8") as f:
            f.write(carry)
            f.flush()
            os.fsync(f.fileno())
        os.replace(side_tmp, side_path)
        state["carry_over"] = ""
        state["carry_over_file"] = CARRY_OVER_SIDE_FILENAME
    else:
        state.pop("carry_over_file", None)
        side_path = out_dir / CARRY_OVER_SIDE_FILENAME
        if side_path.exists():
            try:
                side_path.unlink()
            except OSError:
                pass
    p = out_dir / STATE_FILENAME
    tmp = out_dir / (STATE_FILENAME + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)
    logger.debug("State saved: offset=%s phase=%s", state.get("last_processed_offset"), state.get("current_phase"))


def ensure_line_boundary(f, offset: int) -> int:
    """
    After seek(offset), we might be in the middle of \\r\\n.
    Read until we're at a line boundary and return the actual offset to use.
    """
    if offset == 0:
        return 0
    f.seek(offset)
    b = f.read(1)
    if not b:
        return offset
    if b == b"\n":
        return offset  # already at start of new line
    if b == b"\r":
        n = f.read(1)
        if n == b"\n":
            return f.tell()  # skip \\r\\n, now at boundary
        return offset  # lone \\r
    # In the middle of a line; skip to next newline
    while True:
        b = f.read(1)
        if not b:
            return f.tell()
        if b == b"\n":
            return f.tell()
        if b == b"\r":
            if f.read(1) == b"\n":
                return f.tell()
            f.seek(-1, 1)


def split_lines_from_block(carry_over: str, block: str) -> tuple[list[str], str]:
    """
    Split buffer (carry_over + block) into complete lines.
    Return (list of complete lines, new carry_over for incomplete line).
    """
    text = carry_over + block
    last_nl = text.rfind("\n")
    if last_nl == -1:
        return [], text
    complete = text[: last_nl + 1]
    carry_over_new = text[last_nl + 1 :]
    # Normalize \r\n to \n for splitting so one line per record
    normalized = complete.replace("\r\n", "\n")
    lines = normalized.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return lines, carry_over_new


def run() -> None:
    input_path = Path(INPUT_DIR) / DUMP_FILENAME
    out_dir = Path(OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    logger = setup_logging(out_dir)

    if not input_path.exists():
        logger.error("Input dump not found: %s", input_path)
        sys.exit(1)

    state = load_state(out_dir)
    start_offset = 0
    initial_carry_over = ""
    if state:
        start_offset = state.get("last_processed_offset", 0)
        initial_carry_over = state.get("carry_over", "") or ""
        logger.info("Resuming from offset %s (carry_over len=%s)", start_offset, len(initial_carry_over))

    # Open dump in binary mode
    with open(input_path, "rb") as f:
        start_offset = ensure_line_boundary(f, start_offset)
        f.seek(start_offset)

        # Open file handles we'll use (lazily opened)
        preamble_handle: Any = None
        schema_handle: Any = None
        indexes_handle: Any = None
        functions_handle: Any = None
        views_triggers_handle: Any = None
        data_handle: Any = None

        phase = state.get("current_phase", "preamble") if state else "preamble"
        inside_copy = state.get("inside_copy", False) if state else False
        current_schema = state.get("current_schema", "") if state else ""
        current_table = state.get("current_table", "") if state else ""
        current_part = state.get("current_part", 0) if state else 0
        current_columns = (state.get("current_columns") or []) if state else []
        current_data_rows = state.get("current_data_rows", 0) if state else 0
        current_data_bytes = state.get("current_data_bytes", 0) if state else 0
        indexes_part = state.get("indexes_part", 0) if state else 0
        functions_part = state.get("functions_part", 0) if state else 0
        views_triggers_part = state.get("views_triggers_part", 0) if state else 0

        def open_preamble() -> None:
            nonlocal preamble_handle
            if preamble_handle is None:
                p = out_dir / "01_preamble.sql"
                preamble_handle = open(p, "a", encoding="utf-8", newline="")
                logger.info("Writing to 01_preamble.sql")

        def open_schema(schema: str, table: str):
            """Close current schema file, open new one for this table. Returns the new file handle."""
            nonlocal schema_handle
            if schema_handle is not None:
                schema_handle.close()
                schema_handle = None
            p = out_dir / f"02_schema_{schema}_{table}.sql"
            new_handle = open(p, "a", encoding="utf-8", newline="")
            schema_handle = new_handle
            logger.info("Writing to 02_schema_%s_%s.sql", schema, table)
            return new_handle

        def open_data_part(schema: str, table: str, part: int, write_header: bool, copy_header_line: str) -> None:
            nonlocal data_handle, current_data_bytes, current_data_rows
            if data_handle is not None:
                data_handle.close()
                data_handle = None
            p = out_dir / f"03_data_{schema}_{table}_part{part:03d}.sql"
            data_handle = open(p, "a", encoding="utf-8", newline="")
            if write_header:
                data_handle.write(copy_header_line)
                current_data_bytes = len(copy_header_line.encode("utf-8"))
            current_data_rows = 0
            current_data_bytes = data_handle.tell() if hasattr(data_handle, "tell") else 0
            logger.info("Writing to 03_data_%s_%s_part%03d.sql", schema, table, part)

        def close_data_part_and_write_meta() -> None:
            nonlocal data_handle, current_schema, current_table, current_part, current_columns, current_data_rows
            if data_handle is None:
                return
            data_handle.write("\\." + OUTPUT_LINE_TERMINATOR)
            data_handle.close()
            data_handle = None
            meta_path = out_dir / f"03_data_{current_schema}_{current_table}_part{current_part:03d}.meta.json"
            meta = {
                "schema": current_schema,
                "table": current_table,
                "part": current_part,
                "rows": current_data_rows,
                "columns": current_columns,
            }
            with open(meta_path, "w", encoding="utf-8") as mf:
                json.dump(meta, mf, indent=2)
            logger.info("Closed part %s: rows=%s", current_part, current_data_rows)

        def open_indexes() -> None:
            nonlocal indexes_handle, indexes_part
            if indexes_handle is not None:
                indexes_handle.close()
                indexes_handle = None
            indexes_part += 1
            name = "04_indexes.sql" if indexes_part == 1 else f"04_indexes_part{indexes_part:03d}.sql"
            indexes_handle = open(out_dir / name, "a", encoding="utf-8", newline="")
            logger.info("Writing to %s", name)

        def open_functions() -> None:
            nonlocal functions_handle, functions_part
            if functions_handle is not None:
                functions_handle.close()
                functions_handle = None
            functions_part += 1
            name = "05_functions.sql" if functions_part == 1 else f"05_functions_part{functions_part:03d}.sql"
            functions_handle = open(out_dir / name, "a", encoding="utf-8", newline="")
            logger.info("Writing to %s", name)

        def open_views_triggers() -> None:
            nonlocal views_triggers_handle, views_triggers_part
            if views_triggers_handle is not None:
                views_triggers_handle.close()
                views_triggers_handle = None
            views_triggers_part += 1
            name = (
                "06_views_triggers.sql"
                if views_triggers_part == 1
                else f"06_views_triggers_part{views_triggers_part:03d}.sql"
            )
            views_triggers_handle = open(out_dir / name, "a", encoding="utf-8", newline="")
            logger.info("Writing to %s", name)

        # On resume inside COPY: reopen current data file for append
        if state and state.get("inside_copy") and state.get("current_data_file_path"):
            data_path = out_dir / Path(state["current_data_file_path"]).name
            if data_path.exists():
                data_handle = open(data_path, "a", encoding="utf-8", newline="")
                current_data_rows = state.get("current_data_rows", 0)
                current_data_bytes = state.get("current_data_bytes", 0)
                current_schema = state.get("current_schema", "public")
                current_table = state.get("current_table", "")
                current_part = state.get("current_part", 0)
                current_columns = state.get("current_columns") or []
                logger.info("Resumed appending to %s (rows so far=%s)", data_path.name, current_data_rows)

        carry_over = initial_carry_over
        bytes_processed = start_offset
        block_count = 0
        last_log_offset = start_offset

        try:
            while True:
                block = f.read(READ_BLOCK_BYTES)
                if not block:
                    break
                try:
                    decoded = block.decode("utf-8", errors="replace")
                except Exception as e:
                    logger.warning("UTF-8 decode error at offset %s: %s", bytes_processed, e)
                    decoded = block.decode("utf-8", errors="replace")
                bytes_processed += len(block)
                block_count += 1

                lines, carry_over = split_lines_from_block(carry_over, decoded)

                for line in lines:
                    norm = _normalize_line(line)
                    line_to_write = line.rstrip("\r\n") + OUTPUT_LINE_TERMINATOR
                    line_bytes = len(line_to_write.encode("utf-8"))

                    if inside_copy:
                        if norm == "\\.":
                            close_data_part_and_write_meta()
                            inside_copy = False
                            continue
                        if data_handle is not None:
                            data_handle.write(line_to_write)
                            current_data_rows += 1
                            current_data_bytes += line_bytes
                            if current_data_bytes >= MAX_DATA_CHUNK_BYTES:
                                close_data_part_and_write_meta()
                                current_part += 1
                                open_data_part(
                                    current_schema,
                                    current_table,
                                    current_part,
                                    write_header=True,
                                    copy_header_line=f"COPY {current_schema}.{current_table} ({','.join(current_columns)}) FROM stdin;\n",
                                )
                        continue

                    # Check COPY start
                    copy_match = COPY_FROM_STDIN_RE.match(norm)
                    if copy_match:
                        table_ref = copy_match.group(1).strip()
                        cols_str = copy_match.group(2)
                        schema, table = _parse_table_ref(table_ref)
                        columns = _parse_columns(cols_str)
                        if phase == "schema" and schema_handle is not None:
                            schema_handle.close()
                            schema_handle = None
                        phase = "data"
                        if data_handle is not None:
                            close_data_part_and_write_meta()
                        same_table = current_schema == schema and current_table == table
                        current_schema = schema
                        current_table = table
                        current_columns = columns
                        if not same_table or data_handle is None:
                            current_part = 1
                        else:
                            current_part += 1
                        open_data_part(schema, table, current_part, write_header=True, copy_header_line=line_to_write)
                        inside_copy = True
                        continue

                    # Post-data: CREATE INDEX / FUNCTION / VIEW / TRIGGER / RULE (только после секции data, иначе строки из CREATE TABLE могут ложно сработать)
                    if phase != "schema":
                        if CREATE_INDEX_RE.match(norm):
                            if phase != "post_data":
                                phase = "post_data"
                                if indexes_handle is None:
                                    open_indexes()
                            if indexes_handle is not None:
                                indexes_handle.write(line_to_write)
                            continue
                        if CREATE_FUNCTION_RE.match(norm):
                            if phase != "post_data":
                                phase = "post_data"
                            if functions_handle is None:
                                open_functions()
                            if functions_handle is not None:
                                functions_handle.write(line_to_write)
                            continue
                        if CREATE_VIEW_RE.match(norm) or CREATE_TRIGGER_RE.match(norm) or CREATE_RULE_RE.match(norm):
                            if phase != "post_data":
                                phase = "post_data"
                            if views_triggers_handle is None:
                                open_views_triggers()
                            if views_triggers_handle is not None:
                                views_triggers_handle.write(line_to_write)
                            continue

                    # CREATE TABLE — каждый в свой 02_schema_<schema>_<table>.sql с полным определением
                    create_table_match = CREATE_TABLE_RE.match(norm)
                    if create_table_match:
                        ref = create_table_match.group(1).strip()
                        schema, table = _parse_table_ref(ref)
                        if phase == "preamble":
                            if preamble_handle is not None:
                                preamble_handle.close()
                                preamble_handle = None
                            phase = "schema"
                        if phase == "schema":
                            schema_handle = open_schema(schema, table)  # явно присваиваем новый handle
                        if schema_handle is not None:
                            schema_handle.write(line_to_write)
                        continue

                    # ALTER TABLE (ownership etc.) - belongs to last schema table
                    if phase == "schema" and ALTER_TABLE_OWNER_RE.match(norm) and schema_handle is not None:
                        schema_handle.write(line_to_write)
                        continue

                    # SELECT pg_catalog.pg_... (sequence setval etc.) - только не в schema
                    if phase != "schema" and SELECT_PG_RE.match(norm):
                        if phase != "post_data":
                            phase = "post_data"
                            if views_triggers_handle is None:
                                open_views_triggers()
                        if views_triggers_handle is not None:
                            views_triggers_handle.write(line_to_write)
                        continue

                    # Preamble or other
                    if phase == "preamble":
                        open_preamble()
                        if preamble_handle is not None:
                            preamble_handle.write(line_to_write)
                    elif phase == "schema" and schema_handle is not None:
                        schema_handle.write(line_to_write)
                    elif phase == "post_data":
                        # Default post-data to views_triggers
                        if views_triggers_handle is None:
                            open_views_triggers()
                        if views_triggers_handle is not None:
                            views_triggers_handle.write(line_to_write)
                    else:
                        open_preamble()
                        if preamble_handle is not None:
                            preamble_handle.write(line_to_write)

                # Освобождаем большие объекты до следующего read, чтобы снизить пиковое потребление RAM
                del block, decoded, lines

                # Persist state periodically (after each block)
                state_to_save = {
                    "last_processed_offset": f.tell(),
                    "carry_over": carry_over,
                    "current_phase": phase,
                    "inside_copy": inside_copy,
                    "current_schema": current_schema,
                    "current_table": current_table,
                    "current_part": current_part,
                    "current_columns": current_columns,
                    "current_data_rows": current_data_rows,
                    "current_data_bytes": current_data_bytes,
                    "current_data_file_path": str(out_dir / f"03_data_{current_schema}_{current_table}_part{current_part:03d}.sql")
                    if data_handle
                    else None,
                    "indexes_part": indexes_part,
                    "functions_part": functions_part,
                    "views_triggers_part": views_triggers_part,
                }
                save_state(out_dir, state_to_save, logger)
                if bytes_processed - last_log_offset >= 100 * 1024 * 1024:  # every 100 MB
                    logger.info("Processed %s MB", bytes_processed // (1024 * 1024))
                    last_log_offset = bytes_processed

        finally:
            if inside_copy and data_handle is not None:
                close_data_part_and_write_meta()
            for h in (preamble_handle, schema_handle, indexes_handle, functions_handle, views_triggers_handle):
                if h is not None:
                    h.close()
            state_to_save = {
                "last_processed_offset": bytes_processed,
                "carry_over": carry_over,
                "current_phase": phase,
                "inside_copy": False,
                "current_schema": current_schema,
                "current_table": current_table,
                "current_part": current_part,
                "current_columns": current_columns,
                "current_data_rows": 0,
                "current_data_bytes": 0,
                "current_data_file_path": None,
                "indexes_part": indexes_part,
                "functions_part": functions_part,
                "views_triggers_part": views_triggers_part,
            }
            save_state(out_dir, state_to_save, logger)

    logger.info("Split finished. Total bytes processed: %s", bytes_processed)


if __name__ == "__main__":
    run()
