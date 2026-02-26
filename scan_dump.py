#!/usr/bin/env python3
"""
Scan a large PostgreSQL plain SQL dump in one pass (block-by-block).
Searches for markers (CREATE TABLE, COPY ... FROM stdin, CREATE INDEX, CREATE FUNCTION)
in full lines only; collects tables, indexes, functions, and tables that have data (no row counts).
Uses a bounded overlap between blocks so markers split at boundaries are still found.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from constants import DUMP_FILENAME, INPUT_DIR, OUTPUT_DIR, READ_BLOCK_BYTES

from split_dump import (
    COPY_FROM_STDIN_RE,
    CREATE_FUNCTION_RE,
    CREATE_INDEX_RE,
    CREATE_TABLE_RE,
    _normalize_line,
    _parse_table_ref,
)

SCAN_LOG_FILENAME = "scan_dump.log"
SCAN_RESULT_FILENAME = "scan_result.json"
LOG_PROGRESS_MB = 512
MAX_OVERLAP_BYTES = 256 * 1024  # 256 KB tail between blocks


def setup_logging(out_dir: Path) -> logging.Logger:
    log_path = out_dir / SCAN_LOG_FILENAME
    logger = logging.getLogger("scan_dump")
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


def _apply_markers(
    line: str,
    tables_seen: set[tuple[str, str]],
    tables: list[dict[str, str]],
    tables_with_data_seen: set[tuple[str, str]],
    tables_with_data: list[dict[str, str]],
    indexes_seen: set[str],
    indexes: list[str],
    functions_seen: set[str],
    functions: list[str],
) -> None:
    """Check line against markers and update collections (with dedup)."""
    norm = _normalize_line(line)

    copy_match = COPY_FROM_STDIN_RE.match(norm)
    if copy_match:
        table_ref = copy_match.group(1).strip()
        schema, table = _parse_table_ref(table_ref)
        key = (schema, table)
        if key not in tables_with_data_seen:
            tables_with_data_seen.add(key)
            tables_with_data.append({"schema": schema, "table": table})
        return

    create_table_match = CREATE_TABLE_RE.match(norm)
    if create_table_match:
        ref = create_table_match.group(1).strip()
        schema, table = _parse_table_ref(ref)
        key = (schema, table)
        if key not in tables_seen:
            tables_seen.add(key)
            tables.append({"schema": schema, "table": table})
        return

    if CREATE_INDEX_RE.match(norm):
        if norm not in indexes_seen:
            indexes_seen.add(norm)
            indexes.append(norm)
        return

    if CREATE_FUNCTION_RE.match(norm):
        if norm not in functions_seen:
            functions_seen.add(norm)
            functions.append(norm)
        return


def run(
    input_path: Path,
    output_path: Path,
    block_bytes: int,
    out_dir: Path,
    logger: logging.Logger,
) -> None:
    tables_seen: set[tuple[str, str]] = set()
    tables: list[dict[str, str]] = []
    tables_with_data_seen: set[tuple[str, str]] = set()
    tables_with_data: list[dict[str, str]] = []
    indexes_seen: set[str] = set()
    indexes: list[str] = []
    functions_seen: set[str] = set()
    functions: list[str] = []

    bytes_processed = 0
    last_log_mb = 0
    overlap = ""

    with open(input_path, "rb") as f:
        while True:
            block = f.read(block_bytes)
            if not block:
                break
            try:
                decoded = block.decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning("UTF-8 decode error at offset %s: %s", bytes_processed, e)
                decoded = block.decode("utf-8", errors="replace")
            bytes_processed += len(block)

            if bytes_processed // (1024 * 1024) >= last_log_mb + LOG_PROGRESS_MB:
                last_log_mb = bytes_processed // (1024 * 1024)
                logger.info("Processed %s MB", last_log_mb)

            buffer = overlap + decoded
            last_nl = buffer.rfind("\n")

            if last_nl == -1:
                overlap = buffer[-MAX_OVERLAP_BYTES:] if len(buffer) > MAX_OVERLAP_BYTES else buffer
                continue

            complete = buffer[: last_nl + 1]
            new_overlap = buffer[last_nl + 1 :]
            if len(new_overlap) > MAX_OVERLAP_BYTES:
                new_overlap = new_overlap[-MAX_OVERLAP_BYTES:]
            overlap = new_overlap

            normalized_complete = complete.replace("\r\n", "\n")
            for line in normalized_complete.split("\n"):
                line = line.strip()
                if not line:
                    continue
                _apply_markers(
                    line,
                    tables_seen,
                    tables,
                    tables_with_data_seen,
                    tables_with_data,
                    indexes_seen,
                    indexes,
                    functions_seen,
                    functions,
                )

    result = {
        "tables": tables,
        "indexes": indexes,
        "functions": functions,
        "tables_with_data": tables_with_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as out:
        json.dump(result, out, indent=2, ensure_ascii=False)

    logger.info("Scan complete. Processed %s bytes.", bytes_processed)
    print("--- Summary ---")
    print("Tables (CREATE TABLE):", len(tables))
    print("Indexes (CREATE INDEX):", len(indexes))
    print("Functions (CREATE FUNCTION/PROCEDURE):", len(functions))
    print("Tables with data (COPY):", len(tables_with_data), "(counts not computed)")
    print("Result written to:", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scan PostgreSQL dump for tables, indexes, functions, and tables with COPY data (marker-based, no row counts)."
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output JSON path (default: OUTPUT_DIR/scan_result.json)",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=None,
        help="Override INPUT_DIR from constants",
    )
    parser.add_argument(
        "--dump-filename",
        type=str,
        default=None,
        help="Override DUMP_FILENAME from constants",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir) if args.input_dir is not None else Path(INPUT_DIR)
    dump_filename = args.dump_filename if args.dump_filename is not None else DUMP_FILENAME
    input_path = input_dir / dump_filename

    output_dir = Path(OUTPUT_DIR)
    output_path = args.output if args.output is not None else output_dir / SCAN_RESULT_FILENAME
    if output_path.is_absolute():
        out_dir_for_log = output_path.parent
    else:
        out_dir_for_log = output_dir

    logger = setup_logging(out_dir_for_log)

    if not input_path.exists():
        logger.error("Input dump not found: %s", input_path)
        sys.exit(1)

    run(input_path, output_path, READ_BLOCK_BYTES, out_dir_for_log, logger)


if __name__ == "__main__":
    main()
