[![PyPI version](https://img.shields.io/pypi/v/garmin-health-data.svg)](https://pypi.org/project/garmin-health-data/)
[![Python versions](https://img.shields.io/pypi/pyversions/garmin-health-data.svg)](https://pypi.org/project/garmin-health-data/)
[![CI](https://github.com/diegoscarabelli/garmin-health-data/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/diegoscarabelli/garmin-health-data/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Downloads](https://img.shields.io/pepy/dt/garmin-health-data.svg?color=orange)](https://pepy.tech/project/garmin-health-data)

A single CLI command downloads your complete Garmin Connect health and activity data as local files and loads them into a SQLite database for analysis — extract and process in one pass, or split the two stages with `--extract-only` / `--process-only` for backup-only or replay workflows. Ships a self-contained Garmin Connect client (`garmin_health_data/garmin_client/`) that handles SSO authentication and API access. The well-structured and documented schema makes the database straightforward to analyze, and particularly effective as a data source for AI agents.

**Adapted from the Garmin pipeline in [OpenETL](https://github.com/diegoscarabelli/openetl)**, a comprehensive ETL framework with Apache Airflow and PostgreSQL/TimescaleDB. This standalone version of the [OpenETL Garmin data pipeline](https://github.com/diegoscarabelli/openetl/tree/main/dags/pipelines/garmin) provides the same data extraction and modeling scheme without requiring Airflow or PostgreSQL infrastructure.

## Features

- 🏥 **Comprehensive data**: a single `garmin extract` command downloads sleep, HRV, stress, body battery, heart rate, respiration, VO2 max, training metrics, and FIT activity files (time-series, laps, splits) as local files and loads them into a SQLite database in one pass.
- 👥 **Multi-account**: one database across multiple Garmin Connect accounts (e.g. family members). Run `garmin auth` once per account; extraction discovers and processes them automatically.
- 🛡️ **Resilient pipeline**: four-folder lifecycle (`ingest/process/storage/quarantine`), auto-resume from the last update, crash recovery, and per-date / per-data-type / per-activity / per-FileSet failure isolation. Original files are preserved on disk for offline backup and post-mortem inspection.
- 🔐 **Self-contained Garmin client**: bundled SSO/MFA login client, with no third-party Garmin Connect client library dependency.
- 🖥️ **Cross-platform**: macOS, Linux, Windows. Python 3.10+.

## Requirements

- Python 3.10 or higher
- Garmin Connect account
- Internet connection for data extraction

## Quick Start

### Installation

```bash
pip install garmin-health-data
```

### First-Time Setup

```bash
# Authenticate with Garmin Connect (one-time setup)
garmin auth
```

You'll be prompted for your Garmin Connect email and password. Your credentials are used only to obtain OAuth tokens. After login, your Garmin user ID is auto-detected and tokens are stored in `~/.garminconnect/<user_id>/`.

### Extract Your Data

```bash
# Extract all available data
garmin extract

# View database statistics
garmin info
```

That's it! `garmin extract` saved your raw downloaded files under `garmin_files/storage/` (kept on disk as an offline backup) and loaded them into a local SQLite database (`garmin_data.db`) for analysis.

## Usage

### Authentication & multi-account

```bash
# Interactive authentication (one-time setup, run once per account)
garmin auth

# If you have MFA enabled, you'll be prompted for your code
```

`garmin auth` performs a fresh login and stores OAuth tokens locally. Tokens auto-refresh transparently as long as you extract at least once every 30 days, so you typically only run `garmin auth` once per account or after a long pause. `garmin extract` checks for existing tokens and only prompts for authentication if they're missing.

For multiple accounts (e.g. family members), authenticate each in turn — they all extract into the same database:

```bash
garmin auth --email user1@example.com --password pass1
garmin auth --email user2@example.com --password pass2
```

Tokens are stored in per-account subdirectories and discovered automatically:

```text
~/.garminconnect/
├── 12345678/              # Account 1 tokens
│   └── garmin_tokens.json
└── 87654321/              # Account 2 tokens
    └── garmin_tokens.json
```

All discovered accounts are extracted sequentially when running `garmin extract`, with per-account error isolation (one failing account doesn't block others).

> Login strategies, token rotation, and the 30-45s anti-rate-limit pause: see [Reference](#reference).

### Extracting data

```bash
# Auto-detect range (resumes from last update, or last 30 days if empty)
garmin extract

# Specific date range
garmin extract --start-date 2024-01-01 --end-date 2024-12-31

# Specific data types
garmin extract --data-types SLEEP --data-types HEART_RATE --data-types ACTIVITY

# Specific accounts (comma- or repeat-style)
garmin extract --accounts 12345678,87654321

# Custom database location
garmin extract --db-path ~/my-garmin-data.db

# Backup-only: download files, do not load into the DB
garmin extract --extract-only

# Process files already in ingest/, skip the API
garmin extract --process-only
```

> Full flag table, file lifecycle, retries, and date-handling rules: see [Reference](#reference).

### Common workflows

**Initial extraction** — first run, no flags, gets the last 30 days:

```bash
$ garmin extract
📅 Using default start date: 2024-11-20 (30 days ago)
📆 Date range: 2024-11-20 to 2024-12-20
✅ Extracted 1,234 files
```

**Weekly resume** — same command, just new data since the last run:

```bash
$ garmin extract
📅 Auto-detected start date: 2024-12-21 (day after last update)
📆 Date range: 2024-12-21 to 2024-12-27
✅ Extracted 87 files  # Only new data!
```

**Catching up after a gap** — same command, fills the missing window automatically:

```bash
$ garmin extract
📅 Auto-detected start date: 2024-12-28 (day after last update)
📆 Date range: 2024-12-28 to 2025-01-10
✅ Extracted 156 files  # Automatically fills the gap
```

**Managing disk usage** — `activity_ts_metric` (per-second sensor data from FIT files) accounts for ~93% of typical database size. Two commands let you control its long-tail growth:

```bash
# Aggregate older per-second sensor data into 60s buckets, preserving trends.
$ garmin downsample --end-date 2025-01-01 --time-grain 60s

# Then drop the per-second source rows, keeping the buckets for analysis.
$ garmin prune --end-date 2025-01-01

# Or do both automatically before each extraction (cron-friendly).
$ garmin extract \
    --downsample-older-than 90d --downsample-grain 60s \
    --prune-older-than 1y
```

`downsample` writes to a separate `activity_ts_metric_downsampled` table; `prune` only deletes from `activity_ts_metric`. Activity rows, splits, laps, agg metrics, paths, sleep, and biometric series are never touched. See the [retention reference](#retention-prune-downsample-migrate-cascade) for full flag details.

### Inspecting your data

```bash
# Show row counts and last update dates per table
garmin info

Last Update Dates:
   • Activity: 2024-12-18          # Haven't exercised in 2 days
   • Body Battery: 2024-12-20       # Up to date
   • Floors: 2024-12-20             # Up to date
   • Heart Rate: 2024-12-20         # Up to date
   • Sleep: 2024-12-20              # Up to date
   • Steps: 2024-12-20              # Up to date
   • Stress: 2024-12-20             # Up to date
   ...

# Check a specific database
garmin info --db-path ~/my-garmin-data.db

# Verify database integrity (expected schema table count + SQLite PRAGMA integrity_check)
garmin verify
```

The data lives in a single SQLite file (default `./garmin_data.db`). Query it with `sqlite3`, [DuckDB](https://duckdb.org/), [pandas.read_sql](https://pandas.pydata.org/docs/reference/api/pandas.read_sql.html), or any other SQLite-compatible tool. See [Data Catalog](#data-catalog) for the table layout.

## Reference

### `extract` command flags

| Flag | Type | Purpose |
| --- | --- | --- |
| `--start-date YYYY-MM-DD` | Inclusive | Auto-detected from the database if omitted (day after the latest stored data, or 30 days ago for an empty DB). |
| `--end-date YYYY-MM-DD` | Exclusive (except same-day = inclusive) | Defaults to today. |
| `--data-types NAME` | Repeatable, e.g. `--data-types SLEEP --data-types HEART_RATE` | Filter to specific [data types](#data-types). All types extracted if omitted. |
| `--accounts ID` | Repeatable or comma-separated | Filter to specific Garmin user IDs (`--accounts 12345 --accounts 67890` or `--accounts 12345,67890`). All discovered accounts extracted if omitted. |
| `--db-path PATH` | File path | SQLite database file. Defaults to `./garmin_data.db`. |
| `--extract-only` | Flag | Download to `garmin_files/ingest/` and stop; do not load into the DB. |
| `--process-only` | Flag | Skip the API; load whatever is currently in `garmin_files/ingest/`. Does not require authentication. Mutually exclusive with `--extract-only`. |
| `--downsample-older-than DURATION` | Optional, requires `--downsample-grain` | Before extracting, downsample `activity_ts_metric` rows for activities with `start_ts < today - DURATION`. Accepts `90d`, `6m`, `1y`. |
| `--downsample-grain GRAIN` | Required when `--downsample-older-than` is set | Bucket grain for the auto downsample (e.g., `60s`, `5m`). |
| `--prune-older-than DURATION` | Optional | Before extracting (and after the auto downsample, if both are set), delete `activity_ts_metric` rows for activities with `start_ts < today - DURATION`. |

<details>
<summary><strong>File lifecycle</strong></summary>

By default, every extracted file is preserved on disk in a four-folder lifecycle next to the SQLite database (e.g. `./garmin_files/` for the default `./garmin_data.db`):

- `ingest/`: newly extracted files awaiting processing.
- `process/`: files currently being loaded into the database (in-flight).
- `storage/`: files successfully loaded into the database (kept as offline backup).
- `quarantine/`: files that failed processing (kept for inspection or retry).

This mirrors the openetl pipeline pattern. State transitions are filesystem moves: extract writes to `ingest/`, the CLI bulk-moves `ingest/` → `process/` before parsing, then per-FileSet routes successful files to `storage/` and failed ones to `quarantine/`.

**Crash recovery:** if a run crashes, files left in `process/` are automatically moved back to `ingest/` at the start of the next run, so no work is lost.

**Concurrency (macOS / Linux):** an advisory lock (`garmin_files/.lock`, via `fcntl.flock`) prevents two simultaneous `garmin extract` runs from racing on file moves. A second invocation aborts immediately with a clear message until the first finishes. If a run crashes hard the lock is released automatically by the OS (no stale-lock cleanup needed). On Windows `fcntl` is unavailable, so the lock degrades to a no-op and a one-line warning is printed; serialise concurrent invocations manually.

**Inspecting quarantine:** look in `garmin_files/quarantine/` to see which files failed processing, fix the underlying issue (parser bug, malformed payload, etc.), then move the files back to `garmin_files/ingest/` and run `garmin extract --process-only`.

**Pipeline stages**: the full pipeline (extract → process) runs by default. `--extract-only` writes to `ingest/` and stops; `--process-only` skips the API and consumes whatever is in `ingest/`. The two flags are mutually exclusive. `--process-only` does not auto-detect dates (there are no dates to fetch).

</details>

<details>
<summary><strong>Failure handling &amp; retries</strong></summary>

A single transient failure does not abort the run. Failures are isolated and reported at four levels:

- **Per-date in extraction**: if the API fails for one day (e.g. SLEEP for 2024-03-15), the loop logs the failure and continues with the next day.
- **Per-data-type in extraction**: if a whole data type fails (e.g. a missing endpoint), other data types for the same account still run.
- **Per-activity in extraction**: a parse error or download failure on one activity does not abort the activity download loop.
- **Per-FileSet in processing**: each `(account, day)` group of files is loaded in its own database transaction. A failed group's files move to `quarantine/`; remaining groups load normally.

**Retries with backoff**: every Garmin API call is wrapped in a 4-attempt retry loop (2s → 8s → 30s exponential backoff) for transient network errors (`GarminConnectionError`, `requests.exceptions.ConnectionError`, `requests.exceptions.Timeout`, `socket.gaierror`). Most DNS hiccups and brief outages absorb silently before the per-date isolation layer ever sees them. Application errors (parse failures, `ValueError`, etc.) are not retried — they propagate immediately to the appropriate isolation layer.

**End-of-run summary**: every recorded failure is listed at the end of the run, grouped by data type, so you always know exactly what was skipped and can target a re-run with explicit `--start-date` / `--end-date`.

</details>

<details>
<summary><strong>Date range behavior &amp; auto-detection</strong></summary>

`--start-date` and `--end-date` define the extraction window:

- `--start-date`: **Inclusive**, data from this date is included.
- `--end-date`: **Exclusive**, data from this date is NOT included (except when start and end are the same day, then inclusive).
- Example: `--start-date 2024-01-01 --end-date 2024-01-31` extracts Jan 1-30 (31st excluded).
- Example: `--start-date 2024-01-15 --end-date 2024-01-15` extracts Jan 15 only (same-day inclusive).

**Auto-detection** runs whenever `--start-date` is omitted:

1. **First run (empty database)**: extracts the last 30 days.
2. **Subsequent runs (existing data)**: queries 10 core time-series tables (sleep, heart_rate, activity, stress, body_battery, steps, respiration, floors, intensity_minutes, training_readiness), takes the **maximum** date across them, and starts from the day after.

Using the maximum (rather than per-table latest) means each automatic run covers all data types up to the most recent extraction, even if some types have no rows for some days (e.g. no activities recorded, no training readiness calculated). This keeps the resume logic simple, predictable, and free of redundant API calls.

**Example:** if your database has sleep data through Dec 20 but activities only through Dec 18 (you didn't exercise on Dec 19-20), the next extraction starts from Dec 21. Sleep data for Dec 19-20 was already extracted, no activity data exists for those days, and the Dec 21 run picks up everything.

</details>

<details>
<summary><strong>Authentication internals</strong></summary>

`garmin auth` uses a self-contained SSO client (`garmin_health_data/garmin_client/`) that tries five login strategies in order until one succeeds:

1. Portal web login via `curl_cffi` (TLS browser fingerprint impersonation, 30-45s pre-submit delay).
2. Portal web login via `requests` (30-45s pre-submit delay).
3. Mobile portal login via `curl_cffi` (mobile TLS impersonation, 30-45s pre-submit delay).
4. Mobile login via `requests` (30-45s pre-submit delay).
5. Widget login via `curl_cffi` (last resort — 429s reliably under current Cloudflare config, kept for future use).

**If you see a 30-45 second pause during `garmin auth`, this is normal.** The delay is a deliberate Cloudflare WAF countermeasure — submitting credentials too quickly triggers a 429 rate limit. Tokens obtained are DI OAuth2 Bearer tokens; no session cookies or password are stored after the initial login.

If all five strategies are exhausted without success (uncommon — typically only during Garmin-side outages), `garmin auth` exits with an error. Wait a few minutes and retry.

**Token lifecycle:**

- Access tokens (~18h) auto-refresh transparently using the refresh token (30 days, rotates on each use). As long as you extract at least once within 30 days, tokens stay valid indefinitely.
- `garmin auth` always performs a fresh login and refreshes tokens, even if valid ones already exist.
- `garmin extract` checks for existing tokens and only prompts for authentication if they're missing.
- After login, your Garmin user ID is auto-detected and tokens are stored in `~/.garminconnect/<user_id>/`.

</details>

<details>
<summary><strong>Duplicate prevention &amp; reprocessing</strong></summary>

Duplicates are prevented through a three-tier approach:

1. **FIT activity metrics** (time-series, laps, splits): delete+insert pattern. Existing rows are deleted and fresh data re-inserted in the same transaction, handling added/removed laps or records between reprocesses. The `ts_data_available` flag tracks whether time-series data exists.
2. **JSON wellness time-series** (heart rate, sleep movement, stress, body battery, etc.): `INSERT...ON CONFLICT DO NOTHING` for idempotent upserts.
3. **Main records** (activities, sleep, user profile): `INSERT...ON CONFLICT DO UPDATE` to refresh existing records with new data.

This means you can safely:

- **Reprocess dates** without creating duplicate time-series points.
- **Backfill missing data** by re-extracting date ranges.
- **Retry failed extractions** without manual cleanup.

</details>

### Retention: `prune`, `downsample`, `migrate-cascade`

`activity_ts_metric` (per-second sensor data from FIT files) is the only table whose long-run growth typically matters; on a representative database it accounts for ~93% of disk usage. The retention commands target it directly and leave every other table untouched.

#### Time-range conventions

Both `prune` and `downsample` use the same date-range semantics as `extract`:

- `--end-date YYYY-MM-DD`: **required**, **exclusive** (activities on this date are not affected).
- `--start-date YYYY-MM-DD`: **optional**, **inclusive**. Omit to operate on everything before `--end-date`.
- **Same-day special case**: when start and end are the same calendar day, that day is included.
- Range is interpreted against `activity.start_ts`.

#### `garmin prune`

Deletes rows from `activity_ts_metric` for activities in range. Activity rows themselves, splits, laps, agg metrics, paths, sleep details, biometric series, and the downsampled buckets table are all preserved. By default, prints the matching row count and prompts before deleting.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--end-date YYYY-MM-DD` | Required, exclusive | End of the range. |
| `--start-date YYYY-MM-DD` | Optional, inclusive | Omit for "everything before `--end-date`". |
| `--accounts ID` | Repeatable or comma-separated | Scope to specific Garmin user IDs. |
| `--db-path PATH` | File path | Defaults to `./garmin_data.db`. |
| `--dry-run` | Flag | Report row count without deleting. |
| `--yes` / `-y` | Flag | Skip the confirmation prompt. |

#### `garmin downsample`

Aggregates `activity_ts_metric` rows into time-bucketed records in `activity_ts_metric_downsampled` (a separate table). Source rows are not modified, so `downsample` and `prune` compose: downsample first to preserve trends, then prune to reclaim disk.

**Bucket alignment** is activity-start-relative, so buckets never span activity boundaries. **Activity-level replace semantics**: re-running for an activity with a different `--time-grain` cleanly replaces its prior buckets; activities whose source rows have been pruned are excluded from the replace set so their existing buckets survive untouched.

**Per-metric strategy** is decided automatically based on the metric name:

| Strategy | Applies to | Storage |
| --- | --- | --- |
| `AGGREGATE` (default) | Instantaneous numeric metrics: `heart_rate`, `power`, `cadence`, `speed`, `enhanced_altitude`, `temperature`, all left/right pedal-balance metrics, etc. | avg in `value`, plus `min_value` / `max_value` |
| `LAST` | Cumulative metrics: `distance`, `accumulated_power`, plus future `accumulated_*` / `total_*` (heuristic). | last-in-bucket value; min/max NULL |
| `SKIP` | GPS coordinates: `position_lat`, `position_long` (already materialized in `activity_path`). | not downsampled |

The strategy table is printed before any write so you can verify the classification.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--end-date YYYY-MM-DD` | Required, exclusive | End of the range. |
| `--start-date YYYY-MM-DD` | Optional, inclusive | Omit for "everything before `--end-date`". |
| `--time-grain GRAIN` | Required, format `^([1-9][0-9]*)(s\|m)$` | Bucket width. Examples: `30s`, `60s`, `1m`, `5m`, `15m`, `60m`. Hours intentionally not supported (use minutes). |
| `--accounts ID` | Repeatable or comma-separated | Scope to specific Garmin user IDs. |
| `--db-path PATH` | File path | Defaults to `./garmin_data.db`. |
| `--dry-run` | Flag | Print the strategy table and counts without writing. |
| `--yes` / `-y` | Flag | Skip the confirmation prompt. |

#### `garmin migrate-cascade`

One-shot retrofit of `ON DELETE CASCADE` onto the 16 child FKs (10 activity-children + 6 sleep-children) in pre-2.8 databases. SQLite has no `ALTER TABLE` for changing FK actions, so each affected child table is rebuilt via the standard 12-step recreate dance.

The 2.8 retention features only delete from one childless table (`activity_ts_metric`), so cascade is not required for them. Cascade ships now as an enabler for future expansion to multi-table retention; running this migration on an existing DB is optional but recommended.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--db-path PATH` | File path | Defaults to `./garmin_data.db`. |
| `--dry-run` | Flag | Plan the migration without modifying the database. |
| `--no-backup` | Flag | Skip the pre-migration backup. Default copies the DB to `<db>.bak.<timestamp>`. |

The command is **idempotent** (skips tables that already have cascade), runs a pre-flight `PRAGMA foreign_key_check` (refuses to migrate a database with existing FK violations), and is marked for removal in a future major version once enough users have run it.

## Data Catalog

### Data Types

| Data Type | Description | Frequency |
|-----------|-------------|-----------|
| **SLEEP** | Sleep stages, HRV, SpO2, restlessness, scores | Per session |
| **HEART_RATE** | Continuous heart rate measurements | 2-min intervals |
| **STRESS** | Stress levels throughout the day | 3-min intervals |
| **RESPIRATION** | Breathing rate measurements | 2-min intervals |
| **TRAINING_READINESS** | Readiness scores and factors | Daily |
| **TRAINING_STATUS** | VO2 max, load balance, ACWR | Daily |
| **STEPS** | Step counts and activity levels | 15-min intervals |
| **FLOORS** | Floors climbed and descended | 15-min intervals |
| **INTENSITY_MINUTES** | Moderate/vigorous activity minutes | 15-min intervals |
| **ACTIVITIES_LIST** | Detailed activity summaries | Per activity |
| **EXERCISE_SETS** | Per-set strength training data: reps, weight, ML-classified exercise name | Per activity |
| **PERSONAL_RECORDS** | All-time bests across sports | As achieved |
| **RACE_PREDICTIONS** | Predicted race times | Periodic updates |
| **USER_PROFILE** | Demographics, fitness metrics | Periodic updates |
| **ACTIVITY** | Binary FIT files with detailed time-series sensor data | Per activity |

### Database Schema

The SQLite database contains 33 tables organized by category. The complete schema is defined in [garmin_health_data/tables.ddl](garmin_health_data/tables.ddl) following the same pattern as the [openetl project](https://github.com/diegoscarabelli/openetl). The schema includes inline documentation comments for all tables and columns, which are preserved in the SQLite database itself:

```bash
# View schema for a specific table
sqlite3 ~/garmin_data.db "SELECT sql FROM sqlite_master WHERE type='table' AND name='personal_record';"

# View all table schemas
sqlite3 ~/garmin_data.db "SELECT sql FROM sqlite_master WHERE type='table';"
```

The schema is automatically created when you initialize the database.

<details>
<summary><strong>SQLite adaptations</strong></summary>

The database schema has been adapted from the original PostgreSQL/TimescaleDB [schema in OpenETL](https://github.com/diegoscarabelli/openetl/blob/main/dags/pipelines/garmin/tables.ddl) to be fully compatible with SQLite, while preserving all relationships and data integrity. Key adaptations:

- **Removed PostgreSQL schemas** — SQLite doesn't support schemas; all tables live in the default namespace.
- **Converted SERIAL to AUTOINCREMENT** — PostgreSQL `SERIAL` types converted to SQLite `INTEGER PRIMARY KEY AUTOINCREMENT`.
- **Replaced TimescaleDB hypertables** — time-series tables use regular SQLite tables with indexes on timestamp columns for efficient queries.
- **SQLite-compatible upsert syntax** — uses SQLite's `INSERT ... ON CONFLICT` for handling duplicate records.
- **JSON over JSONB** — PostgreSQL `JSONB` columns (e.g., `activity_path.path_json`) are stored in SQLite as `JSON`/TEXT. CHECK constraints rely on SQLite JSON functions (`json_valid`, `json_type`, `json_array_length`), which are commonly available in SQLite 3.9+ but depend on the SQLite library bundled with your Python/runtime environment. If `CREATE TABLE` fails with errors about missing `json_valid` or `json_type`, verify JSON support first:

  ```bash
  python - <<'PY'
  import sqlite3
  print("SQLite version:", sqlite3.sqlite_version)
  with sqlite3.connect(":memory:") as conn:
      print("json_valid available:", conn.execute("SELECT json_valid('[]')").fetchone()[0] == 1)
  PY
  ```

- **Preserved all relationships** — all foreign key relationships and table structures maintained.

These adaptations ensure the standalone application maintains complete feature parity with the OpenETL Garmin pipeline while using a zero-configuration SQLite database.

</details>

<details>
<summary><strong>Table structure</strong></summary>

**User & Profile (2 tables)**

```
user (root table)
└── user_profile (fitness profile, physical characteristics)
```

*Foreign keys: `user_profile` → `user.user_id`*

**Activities (11 tables)**

```
activity (main activity records)
├── activity_lap_metric (lap-by-lap metrics)
├── activity_path (eagerly materialized GPS path as JSON array)
├── activity_split_metric (split data)
├── activity_ts_metric (time-series sensor data)
├── cycling_agg_metrics (cycling-specific aggregates)
├── running_agg_metrics (running-specific aggregates)
├── strength_exercise (per-exercise aggregates: sets, reps, volume, duration)
├── strength_set (per-set data: reps, weight, ML-classified exercise name)
├── swimming_agg_metrics (swimming-specific aggregates)
└── supplemental_activity_metric (additional activity metrics)
```

*Foreign keys: `activity` → `user.user_id`; all child tables → `activity.activity_id`*

**Sleep Metrics (7 tables)**

```
sleep (main sleep sessions)
├── sleep_level (discrete sleep stage intervals)
├── sleep_movement (movement during sleep)
├── sleep_restless_moment (restless periods)
├── spo2 (blood oxygen saturation)
├── hrv (heart rate variability)
└── breathing_disruption (breathing events)
```

*Foreign keys: `sleep` → `user.user_id`; all child tables → `sleep.sleep_id`*

**Health Time-Series (7 tables)**

```
heart_rate (continuous heart rate measurements)
stress (stress level readings)
body_battery (energy level tracking)
respiration (breathing rate data)
steps (step counts and activity levels)
floors (floors climbed/descended)
intensity_minutes (activity intensity tracking)
```

*Foreign keys: all tables → `user.user_id`*

**Training Metrics (4 tables)**

```
vo2_max (VO2 max estimates)
acclimation (heat/altitude acclimation)
training_load (training load metrics)
training_readiness (daily readiness scores)
```

*Foreign keys: all tables → `user.user_id`*

**Records & Predictions (2 tables)**

```
personal_record (personal bests)
race_predictions (predicted race times)
```

*Foreign keys: all tables → `user.user_id`. Note: `personal_record.activity_id` column exists but has no FK constraint (allows processing PRs before the linked activity is extracted).*

</details>

## Privacy & Security

- **Your credentials never leave your machine**: they're only used to obtain OAuth tokens, stored locally in `~/.garminconnect/<user_id>/`. On Unix-like systems, token directories and files are locked to owner-only access (0o700 directories, 0o600 files); on Windows, standard user-profile permissions apply.
- **All data stays on your machine**: no cloud services involved.
- **No analytics or tracking**: this tool doesn't send any data anywhere except querying the Garmin Connect API directly.

## Comparison With Other Tools

**[garmin-health-data](https://github.com/diegoscarabelli/garmin-health-data)** is designed for comprehensive data extraction with a well-structured relational schema that supports both human-powered analytics and LLM-powered analysis via agents querying the locally created SQLite file. It extracts complete FIT file data with per-second activity metrics, 1-minute sleep intervals, and sport-specific tables for detailed analysis. The normalized 33-table schema with explicit SQL constraints ensures data integrity and makes it easy to understand relationships for complex queries, power zone analysis, running dynamics, and long-term trend studies.

**[garmy](https://github.com/bes-dev/garmy)** is optimized for programmatic access to the Garmin Connect API, particularly useful for AI assistant integration via its built-in MCP (Model Context Protocol) server. It enables real-time interaction with Claude Desktop or custom chatbots for quick daily insights and summaries. However, it's limited to API-provided metrics (daily aggregates only, no FIT file access), making deep analytics or granular time-series analysis impossible. Best suited for lightweight health monitoring apps that prioritize AI integration over comprehensive data collection.

**[garmindb](https://github.com/tcgoetz/GarminDB)** is a mature and well-documented tool, but has been functionally superseded by garmin-health-data. While it pioneered local Garmin data extraction, it offers less comprehensive schemas (missing power meter data, limited FIT metrics) and uses implicit duplicate handling at the ORM level rather than explicit database constraints. For new projects requiring detailed data extraction and analysis, garmin-health-data is the recommended choice.

**Want the full data pipeline with Airflow, scheduled updates, and TimescaleDB?**
Check out [OpenETL's Garmin pipeline](https://github.com/diegoscarabelli/openetl/tree/main/dags/pipelines/garmin).

| Feature | garmin-health-data | garmindb | garmy | garminexport | garmin-fetch |
|---------|-------------------|----------|-------|--------------|--------------|
| **Interface** | CLI | CLI | CLI + Python API + MCP | CLI | GUI |
| **Setup complexity** | ✅ Single command | ⚠️ Config file + 2 commands | ✅ Single command | ✅ Single command | ⚠️ Manual setup |
| **Storage** | SQLite database | SQLite database | SQLite (optional) | File export | Excel export |
| **Cross-platform** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Health metrics (sleep, HRV, stress)** | ✅ Comprehensive | ⚠️ Basic coverage | ⚠️ Basic coverage | ❌ Activities only | ❌ Activities only |
| **Sleep data granularity** | ✅ 7 tables, 1-min intervals | ⚠️ 2 tables, less granular | ⚠️ 1 table, daily aggregate | ❌ | ❌ |
| **FIT file time-series data** | ✅ All metrics (EAV schema) | ⚠️ Limited (~10 core fields) | ❌ API-only (no FIT files) | ❌ | ❌ |
| **Power meter & advanced metrics** | ✅ Full support | ❌ Not captured | ❌ API limitations | ❌ | ❌ |
| **Database schema quality** | ✅ Normalized, 33 tables | ⚠️ ~31 tables, mixed normalization | ❌ Very simple | N/A | N/A |
| **Duplicate prevention** | ✅ Explicit SQL ON CONFLICT | ⚠️ ORM merge (undocumented) | ✅ ORM merge + sync tracking | N/A | N/A |
| **Auto-resume** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Active maintenance** | ✅ | ✅ | ✅ | ✅ | ⚠️ Limited |

<details>
<summary><strong>Schema deep-dive: garmin-health-data vs garmindb vs garmy</strong></summary>

#### Activity Time-Series Data

**garmin-health-data** uses a flexible EAV (Entity-Attribute-Value) schema in the `activity_ts_metric` table:

- **Schema**: `(activity_id, timestamp, name, value, units)`.
- **Captures ALL FIT file metrics**: heart rate, power, cadence, GPS coordinates, advanced running dynamics (ground contact time, vertical oscillation, stride length), cycling power metrics (left/right balance, pedal smoothness), swimming metrics, and more.
- **Future-proof**: automatically handles any new metrics Garmin adds without requiring schema changes.
- **Example**: a cycling activity with a power meter captures `power`, `left_right_balance`, `left_pedal_smoothness`, `right_pedal_smoothness`, `left_torque_effectiveness`, `right_torque_effectiveness`, etc.

**garmindb** uses a fixed column schema in the `ActivityRecords` table:

- **Only ~10 predefined columns**: `hr`, `cadence`, `speed`, `distance`, `altitude`, `temperature`, `position_lat`, `position_long`, `rr`.
- **Missing critical data**: no power data, no advanced running/cycling dynamics, no device-specific metrics.
- **Limited extensibility**: requires schema changes and code updates to add new metrics.

**garmy** (API-only approach):

- **No per-second activity data**: API provides only aggregated summaries (avg/max HR, duration, training load).
- **No FIT file access**: cannot capture detailed time-series metrics that exist only in device files.

#### Sport-Specific Metrics

**garmin-health-data** provides dedicated tables for each sport:

- `running_agg_metrics`: running cadence, vertical oscillation, ground contact time, stride length, VO2 max.
- `cycling_agg_metrics`: power metrics (avg/max/normalized), cadence, pedal dynamics, FTP.
- `swimming_agg_metrics`: stroke count, SWOLF, pool length, stroke type.
- `strength_exercise`: per-exercise aggregates (sets, reps, volume, duration, max weight) from the activities list.
- `strength_set`: per-set granular data (set type, duration, reps, weight, ML-classified exercise name/category) from the exercise sets API endpoint.

**garmindb** uses activity-type tables:

- `StepsActivities`, `PaddleActivities`, `CycleActivities`, `ClimbingActivities`.
- Less comprehensive sport-specific metrics.

**garmy** uses basic activity records:

- `activities`: simple table with activity name, duration, avg HR, training load.
- **No sport-specific metrics**: API doesn't provide detailed power/cadence/dynamics data.

#### Sleep Data Granularity

**garmin-health-data** provides comprehensive sleep tracking with 7 tables:

- `sleep`: main sleep session with scores and metadata.
- `sleep_level`: variable-length intervals classifying each segment of the night as Deep, Light, REM, or Awake.
- `sleep_movement`: 1-minute interval movement data throughout sleep.
- `hrv`: 5-minute interval heart rate variability measurements.
- `spo2`: 1-minute interval blood oxygen saturation.
- `breathing_disruption`: event-based breathing disruption timestamps.
- `sleep_restless_moment`: event-based restless moment timestamps.

**garmindb** uses only 2 tables:

- `Sleep`: main sleep session data.
- `SleepEvents`: sleep events (less granular than garmin-health-data's separate time-series tables).

**garmy** uses 1 table with daily aggregates:

- `daily_health_metrics`: single row per day with summary columns (total hours, deep/light/REM percentages).
- **No per-minute data**: cannot analyze sleep cycles, movement patterns, or SpO2 fluctuations throughout the night.

#### Health Time-Series Organization

**garmin-health-data** uses separate normalized tables for each metric type:

- Each metric type (`heart_rate`, `stress`, `body_battery`, `respiration`, `steps`, `floors`, `intensity_minutes`) has its own table.
- Consistent schema: `(user_id, timestamp, value)` plus metric-specific fields.
- Optimized for time-series queries and analysis.

**garmindb** uses a mixed approach:

- Some monitoring tables for specific metrics.
- Wide `DailySummary` table containing many aggregated metrics in a single row.
- Less optimized for granular time-series analysis.

**garmy** uses normalized tables optimized for API sync:

- `daily_health_metrics`: wide table (~50 columns) for daily summaries.
- `timeseries`: high-frequency data when available from API (heart rate, stress, body battery).
- `sync_status`: tracks which metrics have been synced for each date.

#### Update Strategy & Data Integrity

**garmin-health-data** uses explicit conflict resolution for idempotent reprocessing:

- **Updatable data** (activities, user profile, training status): uses `ON CONFLICT UPDATE` to refresh data when reprocessing.
- **Immutable time-series** (heart rate, sleep movement, stress): uses `ON CONFLICT DO NOTHING` to prevent duplicates.
- **FIT activity metrics** (time-series, laps, splits): uses delete+insert for idempotent reprocessing. The `ts_data_available` flag tracks time-series data availability.
- **Latest flags**: manages `latest=True` flags for `user_profile`, `personal_record`, `race_predictions` to track most recent values.
- **Referential integrity**: explicit foreign key relationships with cascade deletes.
- **Fully idempotent**: safe to reprocess the same date range multiple times without creating duplicate data.

**garmindb** update strategy:

- Uses SQLAlchemy `session.merge()` operations via `insert_or_update()` and `s_insert_or_update()` methods.
- Handles duplicates at the ORM level rather than explicit SQL constraints.
- Implementation detail not documented in README or schema documentation.
- Idempotency behavior exists but is implicit rather than guaranteed at database level.

**garmy** update strategy:

- Uses SQLAlchemy `session.merge()` for upserts + `sync_status` table for tracking.
- **Sync-aware**: tracks which metrics have been synced for each date to avoid redundant API calls.
- **Status tracking**: records `pending`, `completed`, `failed`, or `skipped` status per metric/date.

</details>

## Contributing

Contributions are welcome! Please note:

- **Data extraction and processing logic** is synchronized with the [openetl Garmin pipeline](https://github.com/diegoscarabelli/openetl/tree/main/dags/pipelines/garmin).
- **For changes to extraction/processing logic**, please contribute to openetl first, as this application is a wrapper that provides a standalone CLI.
- **For CLI-specific features, documentation, or packaging improvements**, feel free to contribute directly here.

Please feel free to submit a Pull Request.

## Support

- **Issues**: [GitHub Issues](https://github.com/diegoscarabelli/garmin-health-data/issues)
- **Discussions**: [GitHub Discussions](https://github.com/diegoscarabelli/garmin-health-data/discussions)
