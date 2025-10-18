Extract your complete Garmin Connect health and activity data to a local SQLite database.

**Adapted from the Garmin pipeline in [OpenETL](https://github.com/diegoscarabelli/openetl)**, a comprehensive ETL framework with Apache Airflow and PostgreSQL/TimescaleDB. This standalone version of the [OpenETL Garmin data pipeline](https://github.com/diegoscarabelli/openetl/tree/main/dags/pipelines/garmin) provides the same data extraction and modeling scheme without requiring Airflow or PostgreSQL infrastructure. Built on [python-garminconnect](https://github.com/cyberjunky/python-garminconnect) for Garmin Connect API usage and [Garth](https://github.com/matin/garth) for OAuth authentication.

## Features

- ⚡ **Zero Configuration**: Single command to get started.
- 🖥️ **Cross-Platform**: Works on macOS, Linux, Windows.
- 💾 **Local Storage**: SQLite database - your data stays on your machine.
- 🏥 **Comprehensive Health Data**: Sleep, HRV, stress, body battery, heart rate, respiration, VO2 max, training metrics.
- 🏃 **Activity Data**: FIT files with detailed time-series metrics, lap data, split data.
- 🔄 **Auto-Resume**: Automatically detects last update and syncs new data.

## Requirements

- Python 3.9 or higher
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

You'll be prompted for your Garmin Connect email and password. Your credentials are used only to obtain OAuth tokens, which are stored locally in `~/.garminconnect/`.

### Extract Your Data

```bash
# Extract all available data
garmin extract

# View database statistics
garmin info
```

That's it! Your data is now in a local SQLite database (`garmin_data.db`).

## Usage

### Authentication

```bash
# Interactive authentication (one-time setup)
garmin auth

# If you have MFA enabled, you'll be prompted for your code
```

- `garmin auth` always performs a fresh login and refreshes your OAuth tokens, even if valid tokens already exist.
- Tokens are stored locally in `~/.garminconnect/` and are valid for approximately 1 year.
- You typically only need to run `garmin auth` once initially or when tokens expire.
- `garmin extract` automatically checks for existing tokens and only prompts for authentication if they're missing.
- **Recommendation:** Run `garmin auth` once for initial setup, then just use `garmin extract` for regular data extraction.

### Data Extraction

```bash
# Auto-detect date range (extracts from last update to today)
garmin extract

# Specify custom date range
garmin extract --start-date 2024-01-01 --end-date 2024-12-31

# Extract specific data types only
garmin extract --data-types SLEEP --data-types HEART_RATE --data-types ACTIVITY

# Use custom database location
garmin extract --db-path ~/my-garmin-data.db
```

#### Date Range Behavior

The date range parameters `--start-date` and `--end-date` define the period for data extraction:

- `--start-date`: **Inclusive**, data from this date is included.
- `--end-date`: **Exclusive**, data from this date is NOT included (except when start and end dates are the same, then inclusive).
- Example: `--start-date 2024-01-01 --end-date 2024-01-31` extracts Jan 1-30 (31st excluded).
- Example: `--start-date 2024-01-15 --end-date 2024-01-15` extracts Jan 15 only (same-day inclusive).

#### Automatic Date Detection

One of the key features of garmin-health-data is that you can run `garmin extract` anytime without specifying dates, and it automatically continues from where it left off:

1. **First Run (Empty Database)**
   - Extracts the last 30 days of data.
   - Creates your initial database.

2. **Subsequent Runs (Existing Data)**
   - Queries 10 core time-series tables (sleep, heart_rate, activity, stress, body_battery, steps, respiration, floors, intensity_minutes, training_readiness).
   - Finds the **most recent (maximum) date** across these tables.
   - Automatically starts from the day after this maximum date.
   - Extracts up to today.

This approach assumes that each automatic extraction covers all data types up to the maximum date, even if some specific data types have no data for certain days (e.g., no activities recorded, no training readiness calculated). Using the maximum date ensures:
- Only new data is extracted (efficient, no redundant API calls).
- Gaps in specific data types are automatically filled when available.
- Simple, predictable behavior for users.

**Example:** 

If your database has sleep data through Dec 20th but activities only through Dec 18th (you didn't exercise on Dec 19-20), the next extraction starts from Dec 21st. This is correct because:
- Sleep data for Dec 19-20 was already extracted.
- No activity data exists for Dec 19-20 (you didn't exercise).
- The Dec 21st extraction will get all available data for that day.

### Duplicate Prevention & Reprocessing

This package prevents duplicates through a three-tier approach:

1. **FIT Activity Time-Series**: Tracks processed files with `ts_data_available` flag. Skips already-processed files automatically on re-run.
2. **JSON Wellness Time-Series**: Uses `INSERT...ON CONFLICT DO NOTHING` for idempotent upserts. Reprocessing the same date won't create duplicates.
3. **Main Records (activities, sleep)**: Uses `INSERT...ON CONFLICT DO UPDATE` to update existing records with new data.

This means you can safely:
- **Reprocess dates** without creating duplicate time-series points
- **Backfill missing data** by re-extracting date ranges
- **Retry failed extractions** without manual cleanup

**GarminDB comparison**: GarminDB uses SQLAlchemy `session.merge()` operations (via `insert_or_update()` methods) that handle duplicates at the ORM level. However, this behavior is not explicitly documented. `garmin-health-data` uses explicit SQL-level `ON CONFLICT` clauses that make idempotency guarantees clear and verifiable at the database level.

#### Data Types

You can limit extraction to specific data types using the `--data-types` parameter. If omitted, all data types are extracted. The `--data-types` parameter accepts the exact values from the "Data Type" column in the [Data Types](#data-types) table below (e.g., `SLEEP`, `HEART_RATE`, `ACTIVITY`, `STRESS`, etc.).

### View Database Info

```bash
# Show statistics and last update dates
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

# Check specific database
garmin info --db-path ~/my-garmin-data.db
```

Next `garmin extract` will start from 2024-12-21 (the day after the maximum date, 2024-12-20), ensuring all data types are updated.

### Example Workflow

```bash
# Week 1: Initial extraction
$ garmin extract
📅 Using default start date: 2024-11-20 (30 days ago)
📆 Date range: 2024-11-20 to 2024-12-20
✅ Extracted 1,234 files

# Week 2: Automatic resume (just run the same command!)
$ garmin extract
📅 Auto-detected start date: 2024-12-21 (day after last update)
📆 Date range: 2024-12-21 to 2024-12-27
✅ Extracted 87 files  # Only new data!

# Week 3: Missed a few days? No problem!
$ garmin extract
📅 Auto-detected start date: 2024-12-28 (day after last update)
📆 Date range: 2024-12-28 to 2025-01-10
✅ Extracted 156 files  # Automatically fills the gap
```

## Data Types

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
| **PERSONAL_RECORDS** | All-time bests across sports | As achieved |
| **RACE_PREDICTIONS** | Predicted race times | Periodic updates |
| **USER_PROFILE** | Demographics, fitness metrics | Periodic updates |
| **ACTIVITY** | Binary FIT files with detailed time-series sensor data | Per activity |

## Database Schema

The SQLite database contains 29 tables organized by category. The complete schema is defined in [garmin_health_data/tables.ddl](garmin_health_data/tables.ddl) following the same pattern as the [openetl project](https://github.com/diegoscarabelli/openetl). The schema includes inline documentation comments for all tables and columns, which are preserved in the SQLite database.

**Viewing inline documentation:**

```bash
# View schema for a specific table
sqlite3 ~/garmin_data.db "SELECT sql FROM sqlite_master WHERE type='table' AND name='personal_record';"

# View all table schemas
sqlite3 ~/garmin_data.db "SELECT sql FROM sqlite_master WHERE type='table';"
```

The schema is automatically created when you initialize the database.

### SQLite Adaptations

The database schema has been adapted from the original PostgreSQL/TimescaleDB [schema in OpenETL](https://github.com/diegoscarabelli/openetl/blob/main/dags/pipelines/garmin/tables.ddl) to be fully compatible with SQLite, while preserving all relationships and data integrity. Key adaptations include:

- **Removed PostgreSQL schemas** - SQLite doesn't support schemas, all tables are in the default namespace.
- **Converted SERIAL to AUTOINCREMENT** - PostgreSQL `SERIAL` types converted to SQLite `INTEGER PRIMARY KEY AUTOINCREMENT`.
- **Replaced TimescaleDB hypertables** - Time-series tables use regular SQLite tables with indexes on timestamp columns for efficient queries.
- **SQLite-compatible upsert syntax** - Uses SQLite's `INSERT ... ON CONFLICT` for handling duplicate records.
- **Preserved all relationships** - All foreign key relationships and table structures maintained.

These adaptations ensure the standalone application maintains complete feature parity with the OpenETL Garmin pipeline while using a zero-configuration SQLite database.

### Table Structure

**User & Profile (2 tables)**
```
user (root table)
└── user_profile (fitness profile, physical characteristics)
```
*Foreign keys: `user_profile` → `user.user_id`*

**Activities (8 tables)**
```
activity (main activity records)
├── activity_lap_metric (lap-by-lap metrics)
├── activity_split_metric (split data)
├── activity_ts_metric (time-series sensor data)
├── cycling_agg_metrics (cycling-specific aggregates)
├── running_agg_metrics (running-specific aggregates)
├── swimming_agg_metrics (swimming-specific aggregates)
└── supplemental_activity_metric (additional activity metrics)
```
*Foreign keys: `activity` → `user.user_id`; all child tables → `activity.activity_id`*

**Sleep Metrics (6 tables)**
```
sleep (main sleep sessions)
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
├── acclimation (heat/altitude acclimation)
├── training_load (training load metrics)
└── training_readiness (daily readiness scores)
```
*Foreign keys: all tables → `user.user_id`*

**Records & Predictions (2 tables)**
```
personal_record (personal bests)
race_predictions (predicted race times)
```
*Foreign keys: all tables → `user.user_id`; `personal_record` → `activity.activity_id` (optional)*

## Privacy & Security

- **Your credentials never leave your machine**: they're only used to obtain OAuth tokens via [garth](https://github.com/matin/garth), stored locally in `~/.garminconnect/`.
- **All data stays on your machine**: no cloud services involved.
- **No analytics or tracking**: this tool doesn't send any data anywhere except querying the Garmin Connect API using the wrapper [python-garminconnect](https://github.com/cyberjunky/python-garminconnect).

## Comparison With Other Tools

**[garmin-health-data](https://github.com/diegoscarabelli/garmin-health-data)** is designed for comprehensive data extraction with a well-structured relational schema that supports both human-powered analytics and LLM-powered analysis via agents querying the locally created SQLite file. It extracts complete FIT file data with per-second activity metrics, 1-minute sleep intervals, and sport-specific tables for detailed analysis. The normalized 29-table schema with explicit SQL constraints ensures data integrity and makes it easy to understand relationships for complex queries, power zone analysis, running dynamics, and long-term trend studies.

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
| **Sleep data granularity** | ✅ 6 tables, 1-min intervals | ⚠️ 2 tables, less granular | ⚠️ 1 table, daily aggregate | ❌ | ❌ |
| **FIT file time-series data** | ✅ All metrics (EAV schema) | ⚠️ Limited (~10 core fields) | ❌ API-only (no FIT files) | ❌ | ❌ |
| **Power meter & advanced metrics** | ✅ Full support | ❌ Not captured | ❌ API limitations | ❌ | ❌ |
| **Database schema quality** | ✅ Normalized, 29 tables | ⚠️ ~31 tables, mixed normalization | ❌ Very simple | N/A | N/A |
| **Duplicate prevention** | ✅ Explicit SQL ON CONFLICT | ⚠️ ORM merge (undocumented) | ✅ ORM merge + sync tracking | N/A | N/A |
| **Auto-resume** | ✅ | ✅ | ✅ | ✅ | ❌ |
| **Active maintenance** | ✅ | ✅ | ✅ | ✅ | ⚠️ Limited |

### Schema Comparison: garmin-health-data vs garmindb vs garmy

#### Activity Time-Series Data

**garmin-health-data** uses a flexible EAV (Entity-Attribute-Value) schema in the `activity_ts_metric` table:
- **Schema**: `(activity_id, timestamp, name, value, units)`.
- **Captures ALL FIT file metrics**: heart rate, power, cadence, GPS coordinates, advanced running dynamics (ground contact time, vertical oscillation, stride length), cycling power metrics (left/right balance, pedal smoothness), swimming metrics, and more.
- **Future-proof**: Automatically handles any new metrics Garmin adds without requiring schema changes.
- **Example**: A cycling activity with a power meter captures `power`, `left_right_balance`, `left_pedal_smoothness`, `right_pedal_smoothness`, `left_torque_effectiveness`, `right_torque_effectiveness`, etc.

**garmindb** uses a fixed column schema in the `ActivityRecords` table:
- **Only ~10 predefined columns**: `hr`, `cadence`, `speed`, `distance`, `altitude`, `temperature`, `position_lat`, `position_long`, `rr`.
- **Missing critical data**: No power data, no advanced running/cycling dynamics, no device-specific metrics.
- **Limited extensibility**: Requires schema changes and code updates to add new metrics.

**garmy** (API-only approach):
- **No per-second activity data**: API provides only aggregated summaries (avg/max HR, duration, training load).
- **No FIT file access**: Cannot capture detailed time-series metrics that exist only in device files.

#### Sport-Specific Metrics

**garmin-health-data** provides dedicated tables for each sport:
- `running_agg_metrics`: Running cadence, vertical oscillation, ground contact time, stride length, VO2 max.
- `cycling_agg_metrics`: Power metrics (avg/max/normalized), cadence, pedal dynamics, FTP.
- `swimming_agg_metrics`: Stroke count, SWOLF, pool length, stroke type.

**garmindb** uses activity-type tables:
- `StepsActivities`, `PaddleActivities`, `CycleActivities`, `ClimbingActivities`
- Less comprehensive sport-specific metrics

**garmy** uses basic activity records:
- `activities`: Simple table with activity name, duration, avg HR, training load.
- **No sport-specific metrics**: API doesn't provide detailed power/cadence/dynamics data.

#### Sleep Data Granularity

**garmin-health-data** provides comprehensive sleep tracking with 6 tables:
- `sleep`: Main sleep session with scores and metadata.
- `sleep_movement`: 1-minute interval movement data throughout sleep.
- `hrv`: 5-minute interval heart rate variability measurements.
- `spo2`: 1-minute interval blood oxygen saturation.
- `breathing_disruption`: Event-based breathing disruption timestamps.
- `sleep_restless_moment`: Event-based restless moment timestamps.

**garmindb** uses only 2 tables:
- `Sleep`: Main sleep session data.
- `SleepEvents`: Sleep events (less granular than garmin-health-data's separate time-series tables).

**garmy** uses 1 table with daily aggregates:
- `daily_health_metrics`: Single row per day with summary columns (total hours, deep/light/REM percentages).
- **No per-minute data**: Cannot analyze sleep cycles, movement patterns, or SpO2 fluctuations throughout the night.

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
- `daily_health_metrics`: Wide table (~50 columns) for daily summaries.
- `timeseries`: High-frequency data when available from API (heart rate, stress, body battery).
- `sync_status`: Tracks which metrics have been synced for each date.

#### Update Strategy & Data Integrity

**garmin-health-data** uses explicit conflict resolution for idempotent reprocessing:
- **Updatable data** (activities, user profile, training status): Uses `ON CONFLICT UPDATE` to refresh data when reprocessing.
- **Immutable time-series** (heart rate, sleep movement, stress): Uses `ON CONFLICT DO NOTHING` to prevent duplicates.
- **FIT activity time-series**: Uses `ts_data_available` flag check to skip reprocessing, preventing duplicate records entirely.
- **Latest flags**: Manages `latest=True` flags for `user_profile`, `personal_record`, `race_predictions` to track most recent values.
- **Referential integrity**: Explicit foreign key relationships with cascade deletes.
- **Fully idempotent**: Safe to reprocess the same date range multiple times without creating duplicate data.

**garmindb** update strategy:
- Uses SQLAlchemy `session.merge()` operations via `insert_or_update()` and `s_insert_or_update()` methods.
- Handles duplicates at the ORM level rather than explicit SQL constraints.
- Implementation detail not documented in README or schema documentation.
- Idempotency behavior exists but is implicit rather than guaranteed at database level.

**garmy** update strategy:
- Uses SQLAlchemy `session.merge()` for upserts + `sync_status` table for tracking.
- **Sync-aware**: Tracks which metrics have been synced for each date to avoid redundant API calls.
- **Status tracking**: Records `pending`, `completed`, `failed`, or `skipped` status per metric/date.

## Contributing

Contributions are welcome! Please note:

- **Data extraction and processing logic** is synchronized with the [openetl Garmin pipeline](https://github.com/diegoscarabelli/openetl/tree/main/dags/pipelines/garmin)
- **For changes to extraction/processing logic**, please contribute to openetl first, as this application is a wrapper that provides a standalone CLI
- **For CLI-specific features, documentation, or packaging improvements**, feel free to contribute directly here

Please feel free to submit a Pull Request.

## Support

- **Issues**: [GitHub Issues](https://github.com/diegoscarabelli/garmin-health-data/issues)
- **Discussions**: [GitHub Discussions](https://github.com/diegoscarabelli/garmin-health-data/discussions)
