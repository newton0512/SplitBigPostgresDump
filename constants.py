# Paths and file names for dump split/restore (plan: split_postgresql_dump_and_repair)
INPUT_DIR = "/bckp"
OUTPUT_DIR = "/bckp_work"
DUMP_FILENAME = "dump-anon-db-test-20250905.sql"

# Max size of a single data chunk file (3 GB)
MAX_DATA_CHUNK_BYTES = 3 * 1024**3

# Read block size when streaming the dump (256 MB; safe for ~3.4 GB available RAM)
READ_BLOCK_BYTES = 256 * 1024 * 1024

# Output line terminator: normalize to LF for predictable restore and row counting
OUTPUT_LINE_TERMINATOR = "\n"

# State and meta file names
STATE_FILENAME = "state.json"
RESTORE_STATE_FILENAME = "restore_state.json"
SPLIT_LOG_FILENAME = "split_dump.log"
VALIDATE_REPAIR_LOG_FILENAME = "validate_repair.log"

# Restore: подключение к PostgreSQL (если None — используются переменные окружения PGHOST/PGPORT/PGUSER/PGDATABASE или аргументы CLI).
# Схема не задаётся здесь: восстановление идёт в те же схемы, что и в дампе (например public).
PG_HOST = None
PG_PORT = None
PG_USER = None
PG_DATABASE = None
