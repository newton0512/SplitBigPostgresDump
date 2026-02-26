#!/usr/bin/env python3
"""
Scan a large PostgreSQL plain SQL dump in one pass (same block-by-block reading as split_dump).
Collects: list of tables, indexes, functions, and tables with data (row counts).
Outputs JSON report and a short summary to stdout.
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
    split_lines_from_block,
)

SCAN_LOG_FILENAME = "scan_dump.log"
SCAN_RESULT_FILENAME = "scan_result.json"
LOG_PROGRESS_MB = 512


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


def run(
    input_path: Path,
    output_path: Path,
    block_bytes: int,
    logger: logging.Logger,
) -> None:
    tables_seen: set[tuple[str, str]] = set()
    tables: list[dict[str, str]] = []
    indexes: list[str] = []
    functions: list[str] = []
    tables_with_data: list[dict[str, str | int]] = []

    inside_copy = False
    current_schema = ""
    current_table = ""
    current_rows = 0

    bytes_processed = 0
    last_log_mb = 0

    with open(input_path, "rb") as f:
        carry_over = ""
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

            lines, carry_over = split_lines_from_block(carry_over, decoded)

            for line in lines:
                norm = _normalize_line(line)

                if inside_copy:
                    if norm == "\\.":
                        tables_with_data.append(
                            {"schema": current_schema, "table": current_table, "rows": current_rows}
                        )
                        inside_copy = False
                        continue
                    current_rows += 1
                    continue

                copy_match = COPY_FROM_STDIN_RE.match(norm)
                if copy_match:
                    table_ref = copy_match.group(1).strip()
                    current_schema, current_table = _parse_table_ref(table_ref)
                    current_rows = 0
                    inside_copy = True
                    continue

                create_table_match = CREATE_TABLE_RE.match(norm)
                if create_table_match:
                    ref = create_table_match.group(1).strip()
                    schema, table = _parse_table_ref(ref)
                    key = (schema, table)
                    if key not in tables_seen:
                        tables_seen.add(key)
                        tables.append({"schema": schema, "table": table})
                    continue

                if CREATE_INDEX_RE.match(norm):
                    indexes.append(norm)
                    continue

                if CREATE_FUNCTION_RE.match(norm):
                    functions.append(norm)
                    continue

    result = {
        "tables": tables,
        "indexes": indexes,
        "functions": functions,
        "tables_with_data": tables_with_data,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as out:
        json.dump(result, out, indent=2, ensure_ascii=False)

    total_data_rows = sum(t["rows"] for t in tables_with_data)
    logger.info("Scan complete. Processed %s bytes.", bytes_processed)
    print("--- Summary ---")
    print("Tables (CREATE TABLE):", len(tables))
    print("Indexes (CREATE INDEX):", len(indexes))
    print("Functions (CREATE FUNCTION/PROCEDURE):", len(functions))
    print("Tables with data (COPY):", len(tables_with_data))
    print("Total data rows:", total_data_rows)
    print("Result written to:", output_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan PostgreSQL dump and collect tables, indexes, functions, and data row counts.")
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

    run(input_path, output_path, READ_BLOCK_BYTES, logger)


if __name__ == "__main__":
    main()
