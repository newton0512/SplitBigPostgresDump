# WorkWithDump

Разбиение большого дампа PostgreSQL (plain SQL) на логические чанки, идемпотентное восстановление с возобновлением по строкам и проверка/починка данных по таблицам.

## Назначение

- **split_dump.py** — потоково читает один большой дамп (например 302 ГБ) из `/bckp`, разбивает его на упорядоченные чанки в `/bckp_work` и сохраняет состояние для возобновления после сбоя.
- **restore.py** — восстанавливает БД из чанков в порядке 01 → 02 → 03 → 04 → 05 → 06 с учётом состояния по строкам, чтобы при сбое продолжать загрузку данных с нужной строки.
- **validate_repair_table.py** — для указанной таблицы находит файл схемы и чанки с данными, проверяет соответствие числа полей в строках схеме и при необходимости дополняет или обрезает строки, записывая результат в `.repaired.sql` или перезаписывая исходные файлы с бэкапом.

## Константы

Настройки в `constants.py`:

- `INPUT_DIR` — каталог с исходным дампом (по умолчанию `/bckp`).
- `OUTPUT_DIR` — каталог для чанков и состояния (по умолчанию `/bckp_work`).
- `DUMP_FILENAME` — имя файла дампа (например `dump-anon-db-test-20250905.sql`).
- `MAX_DATA_CHUNK_BYTES` — максимальный размер одного файла с данными (по умолчанию 3 ГБ).
- `READ_BLOCK_BYTES` — размер блока при потоковом чтении дампа (по умолчанию 64 МБ; при нехватке RAM уменьшите до 32 МБ).
- Восстановление: `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_DATABASE` — параметры подключения к PostgreSQL (если не заданы, используются переменные окружения или аргументы CLI). **Схема не настраивается** — объекты восстанавливаются в те же схемы, что и в дампе (например `public`).

## Структура чанков (в OUTPUT_DIR)

| Шаблон | Содержимое |
|--------|------------|
| `01_preamble.sql` | SET, комментарии, расширения, типы, последовательности — всё до первого CREATE TABLE |
| `02_schema_<schema>_<table>.sql` | Один файл на таблицу: CREATE TABLE и связанный DDL |
| `03_data_<schema>_<table>_partNNN.sql` | Данные COPY; несколько частей, если объём > 3 ГБ |
| `03_data_<schema>_<table>_partNNN.meta.json` | `schema`, `table`, `part`, `rows`, `columns` |
| `04_indexes*.sql` | CREATE INDEX |
| `05_functions*.sql` | CREATE FUNCTION/PROCEDURE |
| `06_views_triggers*.sql` | VIEW, TRIGGER и прочие post-data объекты |

## Использование

### 1. Разбиение дампа

На сервере, где лежит дамп (например `/bckp/dump-anon-db-test-20250905.sql`):

```bash
cd /path/to/WorkWithDump
python3 split_dump.py
```

- Дамп читается блоками по `READ_BLOCK_BYTES` (по умолчанию 64 МБ), чанки пишутся в `OUTPUT_DIR` (по умолчанию `/bckp_work`).
- Состояние сохраняется в `OUTPUT_DIR/state.json` (смещение, фаза, carry_over и т.д.).
- При ошибке достаточно запустить скрипт снова: он продолжит с `last_processed_offset` и `carry_over`.
- Логи: `OUTPUT_DIR/split_dump.log` и вывод в stdout.

### 2. Восстановление из чанков

Подключение к PostgreSQL задаётся через `constants.py` (`PG_HOST`, `PG_PORT`, `PG_USER`, `PG_DATABASE`), аргументы CLI или переменные окружения `PGHOST`, `PGPORT`, `PGUSER`, `PGDATABASE`. Пароль — `PGPASSWORD` или `~/.pgpass`. Восстановление идёт **в те же схемы, что в дампе** (например `public`), отдельно указать целевую схему нельзя.

```bash
python3 restore.py [chunks_dir] [--db ИМЯ_БД] [--host ХОСТ] [--port ПОРТ] [--user ПОЛЬЗОВАТЕЛЬ] [--reset]
# Если PostgreSQL в Docker (psql на хосте не нужен):
python3 restore.py bckp_work --docker ИМЯ_КОНТЕЙНЕРА --db pg_from_copy --user postgres
```

- Выполняются по порядку: `01_preamble.sql`, все `02_schema_*.sql`, затем каждый `03_data_*_part*.sql` (с учётом уже загруженных строк), затем `04_*`, `05_*`, `06_*`.
- Состояние: `chunks_dir/restore_state.json` (`preamble_done`, `schema_done`, `data_files`: имя файла → число загруженных строк).
- Для каждого файла с данными в COPY передаются только строки после `rows_loaded`.
- Опция `--reset` сбрасывает состояние восстановления и запускает всё с начала (порядок выполнения сохраняется).

### 3. Проверка и починка одной таблицы

После завершения split_dump:

```bash
python3 validate_repair_table.py ИМЯ_ТАБЛИЦЫ [--schema СХЕМА] [chunks_dir] [--in-place]
```

- Ищет `02_schema_<schema>_<table>.sql` и все `03_data_<schema>_<table>_part*.sql` с соответствующими `.meta.json`.
- Для каждого файла данных проверяет, что в каждой строке число полей (разделитель — табуляция) совпадает с числом колонок в схеме; при нехватке полей дополняет значениями по умолчанию (текст → `unknown`, uuid → случайный UUID, числа → `0`, boolean → `f`, timestamp → `1970-01-01 00:00:00`), при избытке — обрезает.
- Результат пишется в `03_data_<schema>_<table>_partNNN.repaired.sql` либо поверх исходных файлов при `--in-place` (создаются бэкапы `.bak`).
- Логи: `chunks_dir/validate_repair.log` и stdout, с количеством строк и замером времени.

## Файлы состояния и метаданных

- **Состояние split_dump** (`state.json`): `last_processed_offset`, `carry_over`, `current_phase`, `inside_copy`, `current_schema`, `current_table`, `current_part`, `current_columns`, `current_data_rows`, `current_data_bytes`, `current_data_file_path`, `indexes_part`, `functions_part`, `views_triggers_part`.
- **Состояние восстановления** (`restore_state.json`): `preamble_done`, `schema_done`, `data_files` (соответствие имени файла данных и числа загруженных строк).
- **Метаданные данных** (`03_data_*_partNNN.meta.json`): `schema`, `table`, `part`, `rows`, `columns` (список имён колонок из заголовка COPY).

## Низкое потребление памяти (OOM)

На машинах с небольшим объёмом RAM (например 3–4 ГБ) скрипт разбиения может быть убит OOM-killer. Сделано:

- По умолчанию `READ_BLOCK_BYTES = 64 * 1024 * 1024` (64 МБ) вместо 256 МБ — пиковое потребление заметно ниже.
- После обработки каждого блока большие объекты явно удаляются (`del block, decoded, lines`), чтобы не держать лишние ссылки до следующей итерации.

Если OOM всё ещё возникает, в `constants.py` уменьшите `READ_BLOCK_BYTES` до 32 МБ или меньше. После сбоя достаточно снова запустить `split_dump.py` — продолжение идёт с `state.json`.

## Окончания строк

В дампе могут встречаться и CRLF, и LF. Разбиение учитывает оба варианта как границу строки, а **все выходные чанки записываются только с LF**, чтобы восстановление и подсчёт строк были однозначными.
