[![PyPI version](https://img.shields.io/pypi/v/garmin-health-data.svg)](https://pypi.org/project/garmin-health-data/)
[![Python versions](https://img.shields.io/pypi/pyversions/garmin-health-data.svg)](https://pypi.org/project/garmin-health-data/)
[![CI](https://github.com/diegoscarabelli/garmin-health-data/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/diegoscarabelli/garmin-health-data/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Downloads](https://img.shields.io/pepy/dt/garmin-health-data.svg?color=orange)](https://pepy.tech/project/garmin-health-data)

A single CLI command downloads your complete Garmin Connect health and activity data as local files and loads them into a SQLite database for analysis — extract and process in one pass, or split the two stages with `--extract-only` / `--process-only` for backup-only or replay workflows. Ships a self-contained Garmin Connect client (`garmin_health_data/garmin_client/`) that handles SSO authentication and API access. The well-structured and documented schema makes the database straightforward to analyze, and particularly effective as a data source for AI agents.

**Adapted from the Garmin pipeline in [OpenETL](https://github.com/diegoscarabelli/openetl)**, a comprehensive ETL framework with Apache Airflow and PostgreSQL/TimescaleDB. This standalone version of the [OpenETL Garmin data pipeline](https://github.com/diegoscarabelli/openetl/tree/main/dags/pipelines/garmin) provides the same data extraction and modeling scheme without requiring Airflow or PostgreSQL infrastructure.

<!-- markdownlint-disable-next-line MD034 -->
https://github.com/user-attachments/assets/65023665-fa7a-4bbf-85d6-c3d4a3145171

## Features

- 🏥 **Comprehensive data**: a single `garmin extract` command downloads sleep, HRV, stress, body battery, heart rate, respiration, VO2 max, training metrics, menstrual cycle, and activity files in FIT or TCX format (time-series, laps, splits) as local files and loads them into a SQLite database in one pass.
- 👥 **Multi-account**: one database across multiple Garmin Connect accounts (e.g. family members). Run `garmin auth` once per account; extraction discovers and processes them automatically.
- 🛡️ **Resilient pipeline**: four-folder lifecycle (`ingest/process/storage/quarantine`), auto-resume from the last update, crash recovery, and per-date / per-data-type / per-activity / per-FileSet failure isolation. Original files are preserved on disk for offline backup and post-mortem inspection.
- 🗜️ **Bounded disk usage**: `garmin downsample` aggregates per-second sensor data into time-bucketed records, and `garmin prune` deletes the source rows. Together they let you run a multi-year history without unbounded growth (`activity_ts_metric` is ~93% of typical DB size).
- 🔐 **Self-contained Garmin client**: bundled SSO/MFA login client, with no third-party Garmin Connect client library dependency.
- 🖥️ **Cross-platform**: macOS, Linux, Windows. Python 3.10+.

## Requirements

- Python 3.10 or higher
- SQLite 3.35.0 or higher
- Garmin Connect account
- Internet connection for data extraction

Verify the Python/SQLite versions:

```bash
python3 -c "import sys, sqlite3; print(sys.version.split()[0], sqlite3.sqlite_version)"
```

If SQLite is below 3.35.0, install a newer Python via [pyenv](https://github.com/pyenv/pyenv), [conda](https://docs.conda.io/), or [python.org](https://www.python.org/downloads/) — they all bundle a recent SQLite.

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

## Upgrading from 2.8.x or earlier

> **Skip this section if you just installed `garmin-health-data` for the first time.** New installs get the current schema automatically when `garmin extract` (or `garmin init`) creates the database, so neither action below is needed. This section is only for users who installed an earlier version, ran `garmin extract` against it, and are now upgrading the package on top of that existing `garmin_data.db`.

Two one-time actions for upgrading users. Both are independent; you can skip either if it doesn't apply.

### Retrofit cascade FKs (recommended for everyone)

Pre-2.9 databases have no `ON DELETE CASCADE` action on the activity-child or sleep-child foreign keys. The 2.9 retention features still work without cascade (they only delete from one childless table), but cascade ships now as an enabler for future expansion to multi-table retention. Run once after upgrading:

```bash
garmin migrate-cascade
```

The command writes a backup file (`garmin_data.db.bak.<timestamp>`) by default, runs a pre-flight `PRAGMA foreign_key_check` to refuse to migrate a corrupted DB, and is idempotent (safe to run twice). Pass `--dry-run` first if you want to preview, or `--no-backup` for a backup-managed-elsewhere setup.

### Backfill empty sleep detail tables (recommended if you tracked sleep)

A bug from 2.7.0 through 2.8.0 left the per-night detail tables (`sleep_level`, `sleep_movement`, `sleep_restless_moment`, `spo2`, `hrv`, `breathing_disruption`) **silently empty for every user** ([#52](https://github.com/diegoscarabelli/garmin-health-data/issues/52), fixed in 2.9). The `sleep` summary table was always populated correctly. Once on 2.9, repopulate the detail tables with one of:

**Option A — replay the SLEEP files you already have on disk** (fastest, offline, no API calls):

```bash
# Move the SLEEP JSONs from the storage archive back into ingest/.
mv garmin_files/storage/*_SLEEP_*.json garmin_files/ingest/
garmin extract --process-only
```

**Option B — re-extract from Garmin Connect** (use this if `storage/` was pruned or partial):

```bash
garmin extract --data-types SLEEP --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

Both paths are idempotent: the `sleep` summary upsert will reuse existing rows, and the six detail tables will populate retroactively. Re-running over already-detail-loaded nights is a no-op.

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

### Managing disk usage

`activity_ts_metric` (per-second sensor data parsed from FIT or TCX activity files) accounts for ~93% of typical database growth. Two commands let you control it without touching any summary, sleep, or biometric tables:

- **`garmin downsample`** aggregates per-second readings into time buckets and writes them to a separate `activity_ts_metric_downsampled` table. Source rows are never modified by this command.
- **`garmin prune`** deletes per-second source rows for activities in a date range. The downsampled buckets created above survive the prune, so the two compose: downsample first to preserve trends as low-resolution archive, then prune to reclaim disk.

#### Manual one-off run

```bash
# Bucket older per-second data into 60-second averages.
garmin downsample --end-date 2025-01-01 --time-grain 60s

# Then drop the per-second source rows, keeping the buckets for analysis.
garmin prune --end-date 2025-01-01
```

The same date-range conventions as `extract` apply: `--end-date` is required and exclusive, `--start-date` is optional and inclusive (omit to operate on everything before `--end-date`); when start and end are the same day, that single day is included. Both commands accept `--accounts` to scope to specific Garmin user IDs.

#### Cron-friendly automation

For unattended runs, `extract` accepts opt-in retention flags that act on the post-extraction database state:

```bash
# Daily cron entry: extract new data, downsample anything older than 90 days,
# delete per-second rows older than 1 year.
garmin extract \
    --downsample-older-than 90d --downsample-grain 60s \
    --prune-older-than 1y
```

The cutoff is computed as `today - DURATION` (`90d`, `6m`, `1y` are all valid). Retention runs after extraction processing, inside the same lifecycle lock, so concurrent invocations cannot race.

#### Safety rails

Both standalone commands print a row-count preview before any write. `garmin downsample` also prints a per-metric strategy table so you can verify how each metric will be handled (averaged, last-in-bucket, or skipped) before committing.

- `--dry-run` reports what would change and exits without writing.
- `--yes` / `-y` skips the interactive confirmation prompt for scripted use.

> Full flag tables, the per-metric strategy registry, bucket alignment rules: see [Retention reference](#retention-prune-downsample-migrate-cascade).

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

### Commands at a glance

| Command | What it does | Section |
|---|---|---|
| [`garmin auth`](#garmin-auth) | Log into Garmin Connect and store OAuth tokens. Run once per account. | [auth](#garmin-auth) |
| [`garmin extract`](#garmin-extract) | Download data from Garmin Connect and load it into the SQLite database. The default workflow. Supports rolling-window auto retention via opt-in flags. | [extract](#garmin-extract) |
| [`garmin info`](#garmin-info) | Show row counts, last-update dates, and DB size. Read-only. | [info](#garmin-info) |
| [`garmin verify`](#garmin-verify) | Check schema integrity and run SQLite's `PRAGMA integrity_check`. Read-only. | [verify](#garmin-verify) |
| [`garmin downsample`](#garmin-downsample) | Aggregate `activity_ts_metric` into time-bucketed records in `activity_ts_metric_downsampled`. Source rows are not modified. | [retention](#retention-prune-downsample-migrate-cascade) |
| [`garmin prune`](#garmin-prune) | Delete `activity_ts_metric` rows for activities in a date range. The disk-reclaim partner of `downsample`. | [retention](#retention-prune-downsample-migrate-cascade) |
| [`garmin migrate-cascade`](#garmin-migrate-cascade) | One-shot retrofit of `ON DELETE CASCADE` onto pre-2.9 databases. Run once after upgrading from 2.8.x or earlier. | [retention](#retention-prune-downsample-migrate-cascade) |
| [`garmin migrate-multisport`](#garmin-migrate-multisport) | Add `activity.parent_activity_id` and relax the `(user_id, start_ts)` uniqueness to a partial index (multi-sport support). Runs automatically on `extract`; available for explicit control (e.g. `--dry-run`). | — |

All commands accept `--db-path PATH` (defaults to `./garmin_data.db`). Run any command with `--help` to see its full flag list.

### `garmin auth`

```bash
garmin auth
garmin auth --email user@example.com --password '...'
```

Performs a fresh interactive login and stores OAuth tokens in `~/.garminconnect/<user_id>/`. Run once per Garmin Connect account; tokens auto-refresh as long as you extract at least once every 30 days. The `--email` / `--password` flags can also be supplied via the `GARMIN_EMAIL` / `GARMIN_PASSWORD` environment variables. See the [Authentication internals](#authentication-internals) collapsible below for the login-strategy waterfall and the 30-45s anti-rate-limit pause explanation.

### `garmin extract`

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

**Retroactively-editable data:** `MENSTRUAL_CYCLE_DAY` is re-fetched over a trailing 90-day window on every run, independent of the auto-detected start (the start is only ever extended backward, so explicit full-history ranges are unaffected). When a period start is edited after the fact in Garmin Connect, Garmin recomputes `day_in_cycle` for every following day; re-fetching a fixed recent window keeps those already-stored rows in sync via the upsert instead of leaving them stale. Days outside any cycle window return empty payloads and are skipped, so the extra calls are cheap.

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

Duplicates are prevented through a four-tier approach:

1. **Activity metrics from FIT or TCX files** (time-series, laps, splits) **and per-day menstrual cycle tags** (symptoms, moods, discharge): delete+insert pattern. Existing rows are deleted and fresh data re-inserted in the same transaction. For activity metrics this handles added/removed laps or records between reprocesses; the `ts_data_available` flag tracks whether time-series data exists. For menstrual cycle tags, it ensures user-removed symptoms/moods on Garmin Connect propagate to the local DB on the next extract. TCX is parsed for activities uploaded to Garmin Connect from older devices or third-party apps; splits are FIT-only (TCX has no split concept).
2. **JSON wellness time-series** (heart rate, sleep movement, stress, body battery, etc.): `INSERT...ON CONFLICT DO NOTHING` for idempotent upserts.
3. **Main records** (activities, sleep, user profile, menstrual cycle day, observed menstrual cycle summaries): `INSERT...ON CONFLICT DO UPDATE` to refresh existing records with new data.
4. **Predicted menstrual cycle summaries** (`menstrual_cycle_summary` rows with `predicted_cycle = TRUE`): wipe-and-replace per extract. Garmin recomputes its projected start dates as new data is logged, so a pure upsert would accumulate stale predicted rows whose `start_date` PK no longer matches the latest projection. The processor `DELETE`s all predicted rows for the user before inserting the new set; observed (logged) cycles use pattern 3 and survive untouched.

This means you can safely:

- **Reprocess dates** without creating duplicate time-series points.
- **Backfill missing data** by re-extracting date ranges.
- **Retry failed extractions** without manual cleanup.

</details>

### `garmin info`

```bash
garmin info
garmin info --db-path ~/my-garmin-data.db
```

Read-only. Prints database file path and size, per-table row counts for the major tables, and the latest update date observed across the 10 core time-series tables. Useful to confirm an extraction landed and to spot tables that haven't been refreshed recently.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--db-path PATH` | File path | SQLite database file. Defaults to `./garmin_data.db`. |

Exits with code 1 and a "run `garmin extract`" hint if the database file does not exist.

### `garmin verify`

```bash
garmin verify
garmin verify --db-path ~/my-garmin-data.db
```

Read-only. Counts the tables present in the database, compares against the expected schema count, and runs SQLite's `PRAGMA integrity_check`. Useful as a smoke test after a manual schema change, a backup restore, or a `garmin migrate-cascade` run.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--db-path PATH` | File path | SQLite database file. Defaults to `./garmin_data.db`. |

Exits with code 1 if the schema integrity check fails or the database does not exist.

### Retention: `prune`, `downsample`, `migrate-cascade`

`activity_ts_metric` (per-second sensor data parsed from FIT or TCX activity files) is the only table whose long-run growth typically matters; on a representative database it accounts for ~93% of disk usage. The retention commands target it directly and leave every other table untouched.

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

One-shot retrofit of `ON DELETE CASCADE` onto the 16 child FKs (10 activity-children + 6 sleep-children) in pre-2.9 databases. SQLite has no `ALTER TABLE` for changing FK actions, so each affected child table is rebuilt via the standard 12-step recreate dance.

The 2.9 retention features only delete from one childless table (`activity_ts_metric`), so cascade is not required for them. Cascade ships now as an enabler for future expansion to multi-table retention; running this migration on an existing DB is optional but recommended.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--db-path PATH` | File path | Defaults to `./garmin_data.db`. |
| `--dry-run` | Flag | Plan the migration without modifying the database. |
| `--no-backup` | Flag | Skip the pre-migration backup. Default copies the DB to `<db>.bak.<timestamp>`. |

The command is **idempotent** (skips tables that already have cascade), runs a pre-flight `PRAGMA foreign_key_check` (refuses to migrate a database with existing FK violations), and is marked for removal in a future major version once enough users have run it.

#### `garmin migrate-multisport`

Brings a pre-2.12 `activity` table up to date for multi-sport support (#72): adds the nullable `parent_activity_id` column and replaces the table-level `UNIQUE (user_id, start_ts)` constraint with a partial unique index scoped to non-leg rows (`WHERE parent_activity_id IS NULL`). `CREATE TABLE IF NOT EXISTS` cannot alter an existing table and SQLite cannot drop a table constraint in place, so the table is rebuilt via the standard recreate dance, preserving all rows (existing rows get `parent_activity_id = NULL`) and child foreign keys.

This **runs automatically at the start of `garmin extract`** (under the lifecycle lock, backing up and rebuilding only on the first run), so most users never invoke it directly. It is exposed for explicit control — e.g. a `--dry-run` preview.

| Flag | Type | Purpose |
| --- | --- | --- |
| `--db-path PATH` | File path | Defaults to `./garmin_data.db`. |
| `--dry-run` | Flag | Plan the migration without modifying the database. |
| `--no-backup` | Flag | Skip the pre-migration backup. Default copies the DB to `<db>.bak.<timestamp>`. |

The command is **idempotent** (a database that already has `parent_activity_id` is skipped) and runs a pre-flight `PRAGMA foreign_key_check`.

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
| **MENSTRUAL_CYCLE_DAY** | Per-day cycle state: phase, day-in-cycle, period length, plus the user's symptoms / moods / discharge tags, flow, sex drive, sexual activity, notes, ovulation flag on logged days | Per day inside any observed or predicted cycle window |
| **MENSTRUAL_CYCLE_SUMMARY** | Per-cycle summaries (observed + predicted): start date, period length | Per cycle |
| **BODY_COMPOSITION** | Scale weigh-ins: weight, BMI, body fat %, body water %, bone mass, muscle mass | Per weigh-in |
| **RUNNING_TOLERANCE** | Biomechanical running load: daily impact load, running distance, tolerated load ceiling (requires a compatible watch) | Daily |
| **ACTIVITIES_LIST** | Detailed activity summaries | Per activity |
| **EXERCISE_SETS** | Per-set strength training data: reps, weight, ML-classified exercise name | Per activity |
| **PERSONAL_RECORDS** | All-time bests across sports | As achieved |
| **RACE_PREDICTIONS** | Predicted race times | Periodic updates |
| **USER_PROFILE** | Demographics, fitness metrics | Periodic updates |
| **ACTIVITY** | FIT (binary) or TCX (XML) activity files with detailed time-series sensor data | Per activity |

### Database Schema

The SQLite database contains 39 tables organized by category. The complete schema is defined in [garmin_health_data/tables.ddl](garmin_health_data/tables.ddl) following the same pattern as the [openetl project](https://github.com/diegoscarabelli/openetl). The schema includes inline documentation comments for all tables and columns, which are preserved in the SQLite database itself:

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
- **JSON over JSONB** — PostgreSQL `JSONB` columns (e.g., `activity_path.path_json`) are stored in SQLite as `JSON`/TEXT. CHECK constraints rely on SQLite JSON functions (`json_valid`, `json_type`, `json_array_length`). The global SQLite >= 3.35 requirement under [Requirements](#requirements) is necessary but not sufficient: JSON1 functions are enabled by default in modern CPython builds but can be omitted in some custom or stripped-down SQLite builds. If `CREATE TABLE` fails with errors about missing `json_valid` or `json_type`, verify JSON support:

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

**Activities (12 tables)**

```
activity (main activity records)
├── activity_lap_metric (lap-by-lap metrics)
├── activity_path (eagerly materialized GPS path as JSON array)
├── activity_split_metric (split data)
├── activity_ts_metric (time-series sensor data, per-second)
├── activity_ts_metric_downsampled (time-bucketed aggregates of activity_ts_metric, populated by `garmin downsample`)
├── cycling_agg_metrics (cycling-specific aggregates)
├── running_agg_metrics (running-specific aggregates)
├── strength_exercise (per-exercise aggregates: sets, reps, volume, duration)
├── strength_set (per-set data: reps, weight, ML-classified exercise name)
├── swimming_agg_metrics (swimming-specific aggregates)
└── supplemental_activity_metric (additional activity metrics)
```

*Foreign keys: `activity` → `user.user_id`; all child tables → `activity.activity_id`*

Multi-sport activities (duathlon/triathlon) are stored as a family: the parent `multi_sport` row plus one `activity` row per leg (swim/bike/run and transitions), each linking back to the parent via a nullable `activity.parent_activity_id` (self-referential FK, `ON DELETE CASCADE`). Each leg carries its own sport-specific aggregates (`running_agg_metrics` / `cycling_agg_metrics` / `swimming_agg_metrics`). Because a leg can legitimately share a start instant with an independently-recorded standalone activity of the same event, `(user_id, start_ts)` uniqueness is enforced by a partial index that excludes leg rows (`WHERE parent_activity_id IS NULL`).

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

**Health Time-Series (8 tables)**

```
heart_rate (continuous heart rate measurements)
stress (stress level readings)
body_battery (energy level tracking)
respiration (breathing rate data)
steps (step counts and activity levels)
floors (floors climbed/descended)
intensity_minutes (activity intensity tracking)
body_composition (scale weigh-ins: weight, BMI, body fat, etc.)
```

*Foreign keys: all tables → `user.user_id`*

**Training Metrics (5 tables)**

```
vo2_max (VO2 max estimates)
acclimation (heat/altitude acclimation)
training_load (training load metrics)
training_readiness (daily readiness scores)
running_tolerance (daily biomechanical running load and tolerated ceiling)
```

*Foreign keys: all tables → `user.user_id`*

**Records & Predictions (2 tables)**

```
personal_record (personal bests)
race_predictions (predicted race times)
```

*Foreign keys: all tables → `user.user_id`. Note: `personal_record.activity_id` column exists but has no FK constraint (allows processing PRs before the linked activity is extracted).*

**Cycle Tracking (3 tables)**

```
menstrual_cycle_day (daily cycle log: phase, day-in-cycle, flow, notes, ovulation flag)
├── menstrual_cycle_tag (symptoms / moods / discharge tagged on a day)
menstrual_cycle_summary (per-cycle summaries: start date, period length, predicted flag)
```

*Foreign keys: `menstrual_cycle_day` → `user.user_id`; `menstrual_cycle_tag` → `menstrual_cycle_day(user_id, date)` ON DELETE CASCADE; `menstrual_cycle_summary` → `user.user_id`. Per-cycle summaries from Garmin's calendar endpoint include both user-logged cycles and Garmin's projections of upcoming cycles (predicted_cycle column). Predicted rows are wiped and re-inserted on every extract because Garmin recomputes projections as new data is logged; observed rows are upserted by `(user_id, start_date)`.*

</details>

## Privacy & Security

- **Your credentials never leave your machine**: they're only used to obtain OAuth tokens, stored locally in `~/.garminconnect/<user_id>/`. On Unix-like systems, token directories and files are locked to owner-only access (0o700 directories, 0o600 files); on Windows, standard user-profile permissions apply.
- **All data stays on your machine**: no cloud services involved.
- **No analytics or tracking**: this tool doesn't send any data anywhere except querying the Garmin Connect API directly.
- **Sensitive cycle-tracking fields**: when `MENSTRUAL_CYCLE_DAY` is extracted, the per-day log includes the user's freeform `notes`, `sexual_activity`, and `sex_drive` fields alongside the cycle-state scalars. These are captured for completeness so the local database is a faithful mirror of what Garmin Connect stores. They never leave the machine, but they are more sensitive than HR or steps; reviewers of the `garmin_data.db` file should know they're there.

## Comparison With Other Tools

**[garmin-health-data](https://github.com/diegoscarabelli/garmin-health-data)** is designed for comprehensive data extraction with a well-structured relational schema that supports both human-powered analytics and LLM-powered analysis via agents querying the locally created SQLite file. It extracts complete FIT file data with per-second activity metrics, 1-minute sleep intervals, and sport-specific tables for detailed analysis. The normalized 38-table schema with explicit SQL constraints ensures data integrity and makes it easy to understand relationships for complex queries, power zone analysis, running dynamics, and long-term trend studies.

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
| **Database schema quality** | ✅ Normalized, 38 tables | ⚠️ ~31 tables, mixed normalization | ❌ Very simple | N/A | N/A |
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
