# Paths and file names for dump split/restore (plan: split_postgresql_dump_and_repair)
# local config
# INPUT_DIR = "bckp"
# OUTPUT_DIR = "bckp_work"
# DUMP_FILENAME = "dump.sql"

# server config
INPUT_DIR = "/bckp"
OUTPUT_DIR = "/bckp_work"
DUMP_FILENAME = "dump-anon-db-test-20250905.sql"

# Max size of a single data chunk file (3 GB)
MAX_DATA_CHUNK_BYTES = 3 * 1024**3

# Read block size when streaming the dump. На сервере с 3.8 GB RAM 256 MB давало OOM (~3.6 GB anon-rss);
# 64 MB держит пиковое потребление в пределах ~200–400 MB.
READ_BLOCK_BYTES = 64 * 1024 * 1024

# Output line terminator: normalize to LF for predictable restore and row counting
OUTPUT_LINE_TERMINATOR = "\n"

# State and meta file names
STATE_FILENAME = "state.json"
CARRY_OVER_SIDE_FILENAME = "state_carry_over.bin"  # большой carry_over пишем сюда, в JSON не кладём
MAX_CARRY_OVER_IN_STATE_JSON = 512 * 1024  # 512 KB; иначе state.json раздувается до гигабайт (одна длинная строка COPY)
MAX_STATE_JSON_BYTES = 100 * 1024 * 1024  # 100 MB; больший state.json не грузим (старая запись без side file), чтобы не OOM
RESTORE_STATE_FILENAME = "restore_state.json"
SPLIT_LOG_FILENAME = "split_dump.log"
VALIDATE_REPAIR_LOG_FILENAME = "validate_repair.log"

# Restore: подключение к PostgreSQL (если None — используются переменные окружения PGHOST/PGPORT/PGUSER/PGDATABASE или аргументы CLI).
# Схема не задаётся здесь: восстановление идёт в те же схемы, что и в дампе (например public).
PG_HOST = "localhost"
PG_PORT = 5432
PG_USER = "postgres"
PG_DATABASE = "pg_from_copy"
