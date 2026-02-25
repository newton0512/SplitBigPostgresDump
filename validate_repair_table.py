#!/usr/bin/env python3
"""
Validate and repair data for a single table from split dump chunks.
Finds schema file and data part files, checks each row has correct column count,
pads or truncates as needed (unknown, random uuid, 0, false, epoch by type).
Writes repaired files as *.repaired.sql and logs stats.
"""
from __future__ import annotations

import argparse
import json
import logging
import re
import shutil
import sys
import time
import uuid
from pathlib import Path

from constants import OUTPUT_DIR, VALIDATE_REPAIR_LOG_FILENAME

OUTPUT_LINE_TERMINATOR = "\n"
COPY_DELIMITER = "\t"


def setup_logging(chunks_dir: Path) -> logging.Logger:
    logger = logging.getLogger("validate_repair")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    log_path = chunks_dir / VALIDATE_REPAIR_LOG_FILENAME
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


def parse_create_table_columns(schema_path: Path) -> list[tuple[str, str]]:
    """
    Parse CREATE TABLE file and return list of (column_name, type_upper).
    Type is normalized to a short form for default choice (e.g. INTEGER, TEXT, UUID).
    """
    text = schema_path.read_text(encoding="utf-8")
    # Find the first CREATE TABLE and its column list (between first ( and matching )).
    start = text.find("CREATE TABLE")
    if start == -1:
        return []
    paren = text.find("(", start)
    if paren == -1:
        return []
    depth = 1
    i = paren + 1
    begin = i
    while i < len(text) and depth > 0:
        if text[i] == "(":
            depth += 1
        elif text[i] == ")":
            depth -= 1
        i += 1
    col_defs = text[begin : i - 1]
    # Split by comma at top level (ignore commas inside parentheses)
    parts: list[str] = []
    depth = 0
    start = 0
    for j, c in enumerate(col_defs):
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
        elif c == "," and depth == 0:
            parts.append(col_defs[start:j].strip())
            start = j + 1
    if start < len(col_defs):
        parts.append(col_defs[start:].strip())

    result: list[tuple[str, str]] = []
    for p in parts:
        p = p.strip()
        if not p or p.upper().startswith("CONSTRAINT") or p.upper().startswith("PRIMARY") or p.upper().startswith("FOREIGN") or p.upper().startswith("CHECK"):
            continue
        # First token is column name (possibly quoted), rest is type and constraints
        if p.startswith('"'):
            end = p.find('"', 1)
            if end == -1:
                continue
            name = p[1:end]
            rest = p[end + 1 :].strip()
        else:
            tok = p.split(None, 1)
            if len(tok) < 2:
                continue
            name = tok[0].strip('"')
            rest = tok[1]
        # Type is the first word(s) of rest (e.g. "integer", "character varying(255)")
        type_match = re.match(r"^(\w+(?:\s+\w+)?)(?:\s*\([^)]*\))?", rest, re.IGNORECASE)
        type_str = (type_match.group(1) if type_match else rest.split()[0]).upper()
        result.append((name, type_str))
    return result


def type_to_default(type_upper: str, for_unique: bool = False) -> str:
    """Return default value string for a type (for COPY text format)."""
    if "INT" in type_upper or "SERIAL" in type_upper or "NUMERIC" in type_upper or "DECIMAL" in type_upper:
        return "0"
    if "UUID" in type_upper:
        return str(uuid.uuid4()) if for_unique else "00000000-0000-0000-0000-000000000000"
    if "BOOL" in type_upper:
        return "f"
    if "TIMESTAMP" in type_upper or "DATE" in type_upper or "TIME" in type_upper:
        return "1970-01-01 00:00:00"
    if "TEXT" in type_upper or "CHAR" in type_upper or "VARCHAR" in type_upper or "VARCHAR" in type_upper:
        return "unknown"
    return "unknown"


def split_copy_line(line: str) -> list[str]:
    """Split a COPY data line by tab (COPY default delimiter). Does not unescape."""
    return line.split(COPY_DELIMITER)


def repair_data_file(
    data_path: Path,
    meta_path: Path,
    columns_with_types: list[tuple[str, str]],
    chunks_dir: Path,
    logger: logging.Logger,
    in_place: bool = False,
) -> tuple[int, int]:
    """
    Read data file, validate/fix each row, write to .repaired.sql (or overwrite if in_place).
    Returns (rows_checked, rows_repaired).
    """
    with open(meta_path, encoding="utf-8") as mf:
        meta = json.load(mf)
    if columns_with_types:
        expected_cols = [c[0] for c in columns_with_types]
        type_by_idx = [c[1] for c in columns_with_types]
    else:
        expected_cols = meta.get("columns", [])
        type_by_idx = ["TEXT"] * len(expected_cols)
    expected_count = len(expected_cols)

    with open(data_path, "r", encoding="utf-8", newline="") as f:
        copy_header = f.readline()
        rows: list[str] = []
        while True:
            line = f.readline()
            if not line:
                break
            norm = line.rstrip("\r\n")
            if norm == "\\.":
                break
            rows.append(norm)

    repaired_count = 0
    repaired_lines: list[str] = []
    for row in rows:
        fields = split_copy_line(row)
        n = len(fields)
        if n < expected_count:
            # Pad with defaults
            for i in range(n, expected_count):
                t = type_by_idx[i] if i < len(type_by_idx) else "TEXT"
                fields.append(type_to_default(t, for_unique=("uuid" in t.lower() or "id" in expected_cols[i].lower())))
            repaired_count += 1
        elif n > expected_count:
            fields = fields[:expected_count]
            repaired_count += 1
        repaired_lines.append(COPY_DELIMITER.join(fields))

    out_path = data_path.parent / (data_path.stem + ".repaired.sql")
    if in_place:
        backup = data_path.with_suffix(data_path.suffix + ".bak")
        if data_path.exists():
            shutil.copy2(data_path, backup)
            logger.info("Backup written to %s", backup.name)
        out_path = data_path

    with open(out_path, "w", encoding="utf-8", newline="") as out:
        out.write(copy_header.rstrip("\r\n") + OUTPUT_LINE_TERMINATOR)
        for line in repaired_lines:
            out.write(line + OUTPUT_LINE_TERMINATOR)
        out.write("\\." + OUTPUT_LINE_TERMINATOR)

    if in_place and meta_path.exists():
        meta["rows"] = len(repaired_lines)
        with open(meta_path, "w", encoding="utf-8") as mf:
            json.dump(meta, mf, indent=2)

    return len(rows), repaired_count


def main() -> None:
    ap = argparse.ArgumentParser(description="Validate and repair data for one table in split dump chunks.")
    ap.add_argument("table", help="Table name (e.g. mytable)")
    ap.add_argument("--schema", "-s", default="public", help="Schema name (default: public)")
    ap.add_argument("chunks_dir", nargs="?", default=OUTPUT_DIR, help="Directory with 02_schema_*, 03_data_*")
    ap.add_argument("--in-place", action="store_true", help="Overwrite original data files (with .bak backup)")
    args = ap.parse_args()

    chunks_dir = Path(args.chunks_dir)
    if not chunks_dir.is_dir():
        print("Chunks directory not found:", chunks_dir, file=sys.stderr)
        sys.exit(1)

    logger = setup_logging(chunks_dir)
    schema_path = chunks_dir / f"02_schema_{args.schema}_{args.table}.sql"
    if not schema_path.exists():
        logger.error("Schema file not found: %s", schema_path)
        sys.exit(1)

    t0 = time.perf_counter()
    columns_with_types = parse_create_table_columns(schema_path)
    logger.info("Table %s.%s: %s columns from schema", args.schema, args.table, len(columns_with_types))

    data_pattern = f"03_data_{args.schema}_{args.table}_part*.sql"
    data_files = sorted(chunks_dir.glob(data_pattern))
    data_files = [p for p in data_files if ".repaired" not in p.name and ".meta" not in p.name]
    if not data_files:
        logger.warning("No data files found for %s.%s", args.schema, args.table)
        return

    total_checked = 0
    total_repaired = 0
    for data_path in data_files:
        meta_path = data_path.parent / (data_path.stem + ".meta.json")
        if not meta_path.exists():
            logger.warning("No meta for %s, skipping", data_path.name)
            continue
        t1 = time.perf_counter()
        checked, repaired = repair_data_file(
            data_path, meta_path, columns_with_types, chunks_dir, logger, in_place=args.in_place
        )
        elapsed = time.perf_counter() - t1
        total_checked += checked
        total_repaired += repaired
        logger.info("%s: checked=%s repaired=%s in %.2fs", data_path.name, checked, repaired, elapsed)

    elapsed_total = time.perf_counter() - t0
    logger.info("Done: total rows checked=%s repaired=%s in %.2fs", total_checked, total_repaired, elapsed_total)


if __name__ == "__main__":
    main()
