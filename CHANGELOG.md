# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.8.0] - 2026-05-02

### Added

- **`BODY_COMPOSITION` data type**: scale weigh-ins from a connected smart scale (e.g. Index S2) or manual weight entries. Captures weight, BMI, body fat %, body water %, bone mass, muscle mass, physique rating, visceral fat, metabolic age, and `source_type` (e.g. `INDEX_SCALE`, `MANUAL`). Persisted to a new `body_composition` table keyed by `(user_id, timestamp)` so multiple weigh-ins per day are preserved; weight and bone/muscle mass stored in grams to match the existing `user_profile.weight` convention. Insert-only with `ON CONFLICT DO NOTHING` (measurements are immutable). Contributed by @amanusk in #49.
- **`sample_pk` column on `body_composition`**: nullable `BIGINT` capturing Garmin's stable per-sample identifier (`samplePk` from the API), with a non-unique index. Provides a stable handle for reconciling rows against deletions made in Garmin Connect (e.g. user removes a bad weigh-in). Nullable because manual entries lack the field.

### Fixed

- **`get_body_composition` saved one useless JSON file per day for users with no scale data**. The Garmin `/weight-service/weight/daterangesnapshot` endpoint returns a populated wrapper dict on no-data days (`startDate`, `endDate`, an empty `dateWeightList`, and a `totalAverage` of nulls) rather than an empty response. The extractor's generic `if data:` truthiness check saw the wrapper as truthy and wrote a file. The API client now collapses the empty-wrapper shape to `None` so the extractor short-circuits, matching the contract of other RANGE-typed endpoints (e.g. `ACTIVITIES_LIST`).

### Changed

- **`_process_body_composition` now warns when an entry has neither `timestampGMT` nor `date`**: previously such entries were silently skipped. A yellow `⚠️ Skipping body composition entry with no timestamp` warning matches the convention in `_process_training_readiness` / `_process_floors` and surfaces silent data loss in the run log.
- **API module docstring** (`garmin_client/api.py`): bumped endpoint count from 15 to 16 and renamed the "Range activities" bucket to "Range data" to accurately describe both activity-related and wellness range endpoints.
- **README**: added `BODY_COMPOSITION` to the data types table and the Health Time-Series table-structure section (now 8 tables); bumped the total table count from 33 to 34 across the schema overview, project pitch, and comparison matrix.

## [2.7.4] - 2026-04-30

### Fixed

- **`garmin info` and `garmin verify` rejected a missing default database with an unhelpful Click validator error** (`Invalid value for '--db-path': Path 'garmin_data.db' does not exist.`). The function bodies already contained a friendlier "Database not found, run `garmin extract`" fallback, but it was unreachable because `type=click.Path(exists=True)` rejected the input first. The `exists=True` constraint has been removed so the in-function check runs; both commands now exit 1 (so scripts can detect the failure) and both print the "run `garmin extract`" hint (previously only `info` did). Fixed in #47.
- **PyPI new-version hint did not appear on bare `garmin`.** The version check is wired into the `@click.group()` callback, which Click does not invoke when no subcommand is supplied. Added `invoke_without_command=True` so the hint fires on bare invocations as well; help is rendered manually in that case to preserve the existing user-visible behavior. Fixed in #47.

## [2.7.3] - 2026-04-30

### Fixed

- **`garmin extract` crashed with `sqlite3.OperationalError: near ".": syntax error` on a fresh database.** The `INSERT INTO user ... ON CONFLICT (user_id) DO NOTHING` statement in `GarminProcessor._ensure_user_exists()` was authored as a triple-quoted string passed to `sqlalchemy.text(...)`. `docformatter` misidentified that string as a docstring and "normalized" it by appending a period after `DO NOTHING`, producing invalid SQL. The path was only reachable when the `user` table was empty, so existing installs were unaffected and CI didn't catch it. The SQL is now written as implicitly concatenated regular string literals so docformatter leaves it untouched. Reported by @nakor in #44, fixed in #45.

## [2.7.2] - 2026-04-28

### Fixed

- **Per-account partial failures dropped on account-level crash** in the multi-account `extract()` loop. `all_failures.extend(extractor.failures)` lived inside the `try` block on the success path only, so any per-date / per-data-type / per-activity failures captured BEFORE an account-level crash (e.g. an exception in `extract_fit_activities` after `extract_garmin_data` already recorded several per-day failures) were silently lost from the end-of-run summary. The merge moves to a `finally:` block so partial failures are always preserved, regardless of whether the account also crashed. Mirrors the same fix already in place in the openetl Garmin pipeline. New regression test (`test_partial_failures_preserved_when_account_crashes`) guards against the regression.

## [2.7.1] - 2026-04-28

### Fixed

- **Windows CI flake on `test_refreshes_stale_cache`**: `_read_cached()` computed `age = time.time() - st_mtime` and treated the cache as stale only when `age >= CACHE_TTL_SECONDS`. On Windows the NTFS mtime resolution is finer than `time.time()`, so a file written immediately before the check could have an mtime slightly *after* the current clock; the resulting negative age made a `TTL=0` test (and any TTL+race) treat the cache as fresh. Negative ages are now clamped to `0.0`, restoring the intended "stale at TTL=0" behavior.

## [2.7.0] - 2026-04-27

### Added

- **File lifecycle**: every extracted file is preserved on disk in a four-folder pipeline (`garmin_files/{ingest,process,storage,quarantine}/`) next to the database, mirroring the openetl pattern. State transitions are filesystem moves: extract writes to `ingest/`, the CLI bulk-moves to `process/` before parsing, then per-FileSet routes successful files to `storage/` and failed files to `quarantine/` ([#35](https://github.com/diegoscarabelli/garmin-health-data/issues/35)).
- **Crash recovery**: files left in `process/` from a crashed run are auto-moved back to `ingest/` at the start of the next run, so no extracted work is lost.
- **Concurrent-run protection**: `fcntl.flock` advisory lock on `garmin_files/.lock` prevents two simultaneous `garmin extract` runs from racing on file moves. A second invocation aborts immediately with a clear message; the lock is released automatically by the OS on process death.
- **API retries with exponential backoff**: every Garmin API call (per-day data, activity-list fetch, activity download, exercise-sets fetch) is wrapped in a 4-attempt retry loop (2s → 8s → 30s) for transient network errors (`GarminConnectionError`, `requests.exceptions.ConnectionError`, `requests.exceptions.Timeout`, `socket.gaierror`). Most DNS hiccups and brief outages absorb silently; only persistent failures reach the per-date / per-activity isolation layer ([#33](https://github.com/diegoscarabelli/garmin-health-data/issues/33)).
- **`--extract-only` flag**: download files into `ingest/` and stop, without loading them into the database. Useful for backup-only workflows or for manual inspection.
- **`--process-only` flag**: skip the API entirely and process whatever is currently in `ingest/`. Useful for retrying after a parsing fix, or for processing files that arrived from elsewhere. Does not require Garmin authentication.
- **End-of-run summary**: every per-data-type / per-date / per-activity extraction failure is listed at the end of the run, grouped for readability, so users always know what was skipped.
- **PyPI version-update hint**: every `garmin` command checks the latest version on PyPI (cached for 24h in `~/.cache/garmin-health-data/version-check.json`, opt-out with `GARMIN_NO_VERSION_CHECK=1`) and prints a one-line upgrade hint when a newer release is available. Network failures, malformed responses, and missing cache files are silently swallowed so the check never aborts a command.

### Changed

- **Per-date extraction isolation**: a transient API failure on one date is logged and recorded; extraction continues with the next date.
- **Per-data-type extraction isolation**: a structural failure for one data type is logged and recorded; extraction continues with the next data type for the same account.
- **Per-activity extraction isolation**: any exception during one activity download is logged with the activity ID; the activity-download loop continues. The activity-list (`get_activities_by_date`) call is wrapped so a list-fetch failure records an `ACTIVITIES_LIST` failure cleanly.
- **Per-FileSet processing isolation**: each FileSet runs in its own SQLAlchemy session inside try/except (mirrors openetl's `_try_process_file_set`). A bad FileSet is rolled back and moved to `quarantine/`; subsequent FileSets continue normally.
- **`extract_fit_activities` reads `ACTIVITIES_LIST` from disk**: the registry loop's saved JSON in `ingest/` is consumed directly, so the `get_activities_by_date` endpoint is hit at most once per run. Falls back to a live API call if the file is missing.
- **Renamed `_process_day_by_day` → `_extract_day_by_day`**: the function does extraction (API call + write JSON), not processing.

### Fixed

- **`UNIQUE constraint failed: activity_ts_metric` on FIT files with sub-second sampling** ([#36](https://github.com/diegoscarabelli/garmin-health-data/issues/36)): the FIT record-frame parser now reads the optional `fractional_timestamp` field paired with `timestamp` and combines them, so high-frequency devices (e.g. Fenix 7 at 2Hz smart-recording) get distinct rows per sub-second sample instead of colliding on the `(activity_id, timestamp, name)` unique key. Belt-and-suspenders: if duplicates remain (FIT files without `fractional_timestamp` that emit multiple frames within the same whole second), they are coalesced in Python before bulk insert with the last value winning, instead of aborting the activity.
- **Makefile `format` target accepts docformatter exit code 3**: `docformatter --in-place` exits 3 to signal "files modified"; the `format` target accepts both exit 1 and exit 3 as non-fatal. The pre-commit hook passes on the first run after editing any docstring.

## [2.6.1] - 2026-04-17

### Fixed

- **SQLite parameter limit safety**: `upsert_model_instances` now automatically splits large batches into chunks so the total parameter count stays within SQLite's `SQLITE_MAX_VARIABLE_NUMBER` limit (999 on pre-3.32.0 builds). Previously, a single INSERT with many rows on wide tables (e.g., Sleep at 73 columns) could exceed the limit and fail. The conservative floor of 999 guarantees safety across all supported platforms.

## [2.6.0] - 2026-04-17

### Changed

- **SQLAlchemy 2.0 ORM migration** ([#30](https://github.com/diegoscarabelli/garmin-health-data/pull/30)): Migrated all legacy SQLAlchemy 1.4 patterns to native 2.0 style, aligning runtime code with the `sqlalchemy>=2.0` dependency declared since v2.0.3.
  - Model base: `declarative_base()` replaced with `DeclarativeBase` subclass.
  - Sessions: `sessionmaker(bind=engine)` replaced with `Session(engine)` context manager.
  - Queries: all `session.query()` calls replaced with `session.execute(select(...))`.
  - Bulk deletes: `.filter_by(...).delete()` replaced with `session.execute(delete(...).where(...))`.
  - FIT metric bulk inserts: `bulk_save_objects()` replaced with core `insert()` to bypass the ORM identity map and avoid SQLite's RETURNING sentinel mismatch with `DateTime(timezone=True)` composite PKs. Column keys are precomputed once per model to avoid repeated `__table__.columns` iteration on large FIT files.
  - Strength exercise/set inserts: `bulk_save_objects()` replaced with `add_all()`.
  - Test assertions for delete statements now verify the target table and WHERE clause rather than just checking that a DELETE was executed.

### Fixed

- **Activity file format detection** ([#27](https://github.com/diegoscarabelli/garmin-health-data/pull/27)): Activity downloads containing non-FIT files (TCX, GPX, KML) no longer crash the application. Contributed by [@dillten](https://github.com/dillten).
  - Magic-byte detection identifies the actual file format from content (ANT+ FIT header, XML root elements) instead of assuming `.fit`.
  - Three-tier fallback chain: magic bytes, inner filename extension, `.bin` preservation for unrecognised formats.
  - Files are saved with the correct extension reflecting their detected format.
  - Non-FIT activity files are preserved on disk but excluded from FIT-specific processing, with clear warnings.
  - `FileSet.file_paths` now derived from matched files only, preventing `ValueError` when non-processable files sort before `.fit` files in mixed timestamp groups.
  - `GarminConnectionError` during activity download (e.g., 404 for manually-entered activities) is caught and skipped instead of aborting the entire extraction run.

## [2.5.0] - 2026-04-08

### Added

- **Vendored `garmin_client/` module** ([#25](https://github.com/diegoscarabelli/garmin-health-data/pull/25)): Replaced the `python-garminconnect` PyPI dependency with a self-contained `garmin_client/` module shipped directly in this package.
  - Five-strategy SSO fallback chain with `curl_cffi` TLS fingerprint impersonation: portal+cffi → portal+requests → mobile+cffi → mobile+requests → widget+cffi. Each strategy tries in order; the next is attempted on 429 or failure.
  - 30-45s randomized delay before the credential POST on strategies 1-4, visible at INFO log level (`"Portal login: waiting ~35s to avoid Cloudflare rate limiting..."`), so long auth runs no longer appear hung.
  - Runtime token refresh: access tokens (~18h) are auto-refreshed transparently when within 15 minutes of expiry or on a 401 retry. Refresh tokens (~30d) rotate on each use and are persisted back to disk immediately. The token chain stays alive indefinitely as long as extraction runs at least once within 30 days.
  - Atomic token writes: tokens are written to a PID-namespaced temp file and swapped in via `os.replace`, preventing truncated token stores on interrupted writes.
  - No external Garmin client library required. `curl-cffi` and `ua-generator` are now explicit runtime dependencies (previously transitive via `garminconnect`).
  - Token file format (`garmin_tokens.json`) and storage path (`~/.garminconnect/<user_id>/`) are unchanged. Existing tokens from v2.3.0+ do not require re-bootstrapping.
- **`sleep_level` table** ([#24](https://github.com/diegoscarabelli/garmin-health-data/pull/24)): New table populated from the `sleepLevels` array in the SLEEP JSON response. Each row is a contiguous interval during which a single discrete sleep stage (Deep, Light, REM, Awake) was detected, allowing reconstruction of the per-night sleep stages timeline shown in the Garmin Connect sleep view.
  - Stage codes (`stage`) and human-readable labels (`stage_label`) are sourced from the new `SleepStage` IntEnum in `constants.py`. Unknown stage codes are logged and skipped instead of failing the file.
  - Idempotent on `(sleep_id, start_ts)` via `INSERT ... ON CONFLICT DO NOTHING`.
  - Index on `stage` for cheap stage-distribution queries.
- New `SleepStage` IntEnum in `constants.py` mapping integer codes in `sleepLevels[*].activityLevel` to their human-readable names (`DEEP`, `LIGHT`, `REM`, `AWAKE`).

### Fixed

- **Python 3.10 compatibility for Garmin GMT timestamps** ([#24](https://github.com/diegoscarabelli/garmin-health-data/pull/24)): Several processors called `datetime.fromisoformat` directly on Garmin's single-digit fractional second format (e.g. `"2026-04-06T05:47:59.0"`), which Python 3.10's strict parser rejects with `ValueError`. New `_parse_garmin_iso` / `_parse_garmin_gmt` helpers on `GarminProcessor` normalize the fractional component to 6 digits and tolerate an optional trailing timezone designator (`Z` or `±HH:MM`). Applied to `sleep_level`, `sleep_movement`, `spo2`, `steps`, `floors`, `training_readiness`, and `strength_set` ingestion paths.

### Removed

- `python-garminconnect` runtime dependency ([#25](https://github.com/diegoscarabelli/garmin-health-data/pull/25)).

## [2.4.0] - 2026-04-06

### Added

- **`activity_path` table**: New table eagerly materializing GPS coordinate sequences from FIT files during processing. Each row stores an ordered `[longitude, latitude]` JSON array sorted ascending by timestamp, ready for deck.gl or any path-layer visualization. Populated automatically during FIT file processing via delete+insert for reprocessing idempotency. Activities without GPS samples (indoor workouts) have no row. Mirrors the `garmin.activity_path` table added to the openetl Garmin pipeline.
  - Three CHECK constraints enforce `path_json` integrity: valid JSON, array type, and `point_count` matching `json_array_length(path_json)`. Requires SQLite JSON1 support; JSON1 has been bundled with SQLite since 3.9, but availability in Python's built-in `sqlite3` module depends on the underlying SQLite build and may vary by environment.
  - Index on `point_count` for cheap filtering/sorting by track length.
- New constant `SEMICIRCLES_TO_DEGREES` in `constants.py` for Garmin FIT semicircle-to-decimal-degree conversion.

## [2.3.0] - 2026-04-03

### Changed

- **Upgrade to garminconnect >= 0.3.0** ([#19](https://github.com/diegoscarabelli/garmin-health-data/issues/19)): The upstream library replaced the `garth` authentication library with a native OAuth2 engine.
  - Removed `hasattr(garmin, "garth")` version guard and User-Agent override (both unnecessary with native OAuth2 and `curl_cffi` TLS fingerprint impersonation).
  - Token persistence: `garmin.garth.dump()` replaced with `garmin.client.dump()`.
  - Token file format changed from `oauth1_token.json` + `oauth2_token.json` to a single `garmin_tokens.json`. Existing tokens from garminconnect < 0.3.0 are not read by the new version; re-run `garmin auth` to bootstrap fresh tokens.
  - Token lifecycle: access tokens (~18h) are now auto-refreshed transparently using the refresh token (30 days, rotates on each use). As long as extraction runs at least once within 30 days, the token chain stays alive indefinitely.
  - `garmin auth` is now only needed for initial setup or recovery after 30+ days of inactivity (previously described as "approximately 1 year").

### Removed

- **Python 3.9 support**: garminconnect >= 0.3.0 requires Python >= 3.10. Minimum version bumped accordingly.
- `test_refresh_tokens_missing_garth_attribute` test (garminconnect 0.3.0 no longer has a `garth` attribute).

### Notes

- **Re-authentication required**: After upgrading, run `garmin auth` once per account to bootstrap tokens in the new format.

## [2.2.0] - 2026-04-01

### Added

- **Multi-account support**: Extract data from multiple Garmin Connect accounts into a single database.
  - Convention-based account discovery: scans `~/.garminconnect/` for numeric subdirectories (each is a user_id).
  - `garmin auth` auto-detects user ID and stores tokens in `~/.garminconnect/<user_id>/`.
  - `garmin extract` discovers and extracts all accounts sequentially with per-account error isolation.
  - New `--accounts` CLI option to filter which accounts to extract (comma-separated or repeated).
  - Legacy token layout (flat files at root) detected with migration warning.

### Fixed

- **SSO authentication**: Override garth's default User-Agent to avoid Cloudflare blocks during programmatic login.
- **Token file permissions**: `chmod 0o600` on token files after `garth.dump()` (garth uses default umask, leaving tokens world-readable).
- **Idempotent FIT metric reprocessing** ([#15](https://github.com/diegoscarabelli/garmin-health-data/pull/15)): Replaced the early-return guard on `activity_ts_metric`, `activity_split_metric`, and `activity_lap_metric` with a delete+insert pattern, preventing `UNIQUE` constraint violations on re-runs ([#14](https://github.com/diegoscarabelli/garmin-health-data/issues/14)). Also excludes `create_ts` from `Activity` and `Sleep` upsert update columns to preserve audit timestamps.

## [2.1.1] - 2026-04-01

### Fixed

- **Bug**: Authentication fails with `'Garmin' object has no attribute 'garth'` when using older or improperly installed `garminconnect` versions ([#13](https://github.com/diegoscarabelli/garmin-health-data/issues/13)).
  - Added a `hasattr` guard that checks for the `garth` attribute before accessing it, with a clear error message and upgrade instructions.
  - Token directory permissions tightened from `0o755` to `0o700`.
  - Auth failure messages now include the installed `garminconnect` version for easier debugging.

### Added

- Test coverage for the missing `garth` attribute scenario (`test_auth_extended.py`).

## [2.1.0] - 2026-03-27

### Added

- **Strength training exercise data** ([#11](https://github.com/diegoscarabelli/garmin-health-data/issues/11)): Per-exercise and per-set granular strength training data with two new tables and a new API data source.
  - `strength_exercise`: Per-exercise aggregates (sets, reps, volume, duration, max weight) derived from `summarizedExerciseSets` in the activities list.
  - `strength_set`: Per-set granular data (set type, duration, reps, weight, ML-classified exercise name/category) from the `/activity-service/activity/{id}/exerciseSets` API endpoint.
  - Extraction automatically fetches exercise sets for `strength_training` and `fitness_equipment` activity types alongside FIT file downloads.
  - Both tables use delete+insert for reprocessing since composite PK components can change.
  - `EXERCISE_SETS` registered as a new data type in `GarminDataRegistry`.
  - **Migration**: Seamless. New tables are created automatically on next `garmin extract` (existing data is untouched). To populate historical strength data, re-run extraction for past date ranges containing strength training activities.

## [2.0.3] - 2026-03-08

### Fixed

- **Bug**: Extractor did not function on Windows.
  - Remove incompatible char ':' from timestamp.
  - Use gettempdir() to get temp directory instead of hardcoding to /tmp.
  - Use POSIX-compatible DB URL.
  - Skip potentially problematic chmod on Windows.
  - **Impact**: Extractor now runs where it did not before.
  - **Migration**: Re-run `garmin-health-data extract`, which should now function.
- **Bug**: `garmin verify` command failed under SQLAlchemy 2.x with `sqlalchemy.exc.ArgumentError` ("Textual SQL expression ... should be explicitly declared as text(...)") due to a raw SQL string passed to `session.execute()` without a `text()` wrapper.

### Changed

- Pinned `black` to `==25.9.0` in dev dependencies to prevent formatting inconsistencies between local and CI environments.
- Bumped minimum `sqlalchemy` dependency from `>=1.4` to `>=2.0` (1.4 reached end-of-life in 2024).

### Added

- CLI test suite (`tests/test_cli.py`) with regression test for the SQLAlchemy `text()` compatibility issue.

## [2.0.2] - 2025-10-21

### Fixed

- **Bug**: Fixed extraction of sleep fields from incorrect JSON location causing NULL values in database.
  - `resting_heart_rate`, `hrv_status`, and `skin_temp_data_exists` were incorrectly being extracted from `dailySleepDTO` instead of the top-level JSON object.
  - These fields now correctly populate with data from Garmin Connect.
  - **Impact**: Existing sleep records with NULL values for these fields need to be reprocessed to populate the correct data.
  - **Migration**: Re-run `garmin-health-data process` for affected date ranges to update historical data.

## [2.0.1] - 2025-10-20

### Fixed

- **Critical**: Added missing `update_ts` column to `training_readiness` table in schema DDL.
  - Users on 2.0.0 will encounter `sqlite3.OperationalError: no such column: update_ts` when processing training readiness data.
  - Migration: Run `ALTER TABLE training_readiness ADD COLUMN update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;` or recreate database.

### Documentation

- Updated RELEASE.md instructions to match current GitHub release UI.

## [2.0.0] - 2025-10-19

### ⚠️ BREAKING CHANGES

**Database schema change: `insert_ts` renamed to `create_ts` in all tables.**

All timestamp columns previously named `insert_ts` have been renamed to `create_ts` for improved clarity and consistency with industry standards. This affects all 29 tables in the database.

**Existing databases are NOT compatible with version 2.0.0.** Users must delete their existing database and re-extract data with the new schema.

### Migration Path

**Recommended approach: Fresh database extraction**

```bash
# Backup your old database if you want to keep it
mv ~/.garmin/garmin_health_data.db ~/.garmin/garmin_health_data.db.v1_backup

# Delete the database to allow re-creation with new schema
rm ~/.garmin/garmin_health_data.db

# Re-extract all data with version 2.0.0
garmin extract --all --start-date 2020-01-01
```

All data can be re-downloaded from Garmin Connect. This is the cleanest upgrade path.

### Changed

- **BREAKING**: Renamed `insert_ts` to `create_ts` in all database tables for better semantic clarity.
- Updated SQLAlchemy models to use `create_ts`.
- Updated DDL schema file (`tables.ddl`) with `create_ts`.
- Updated all internal code references from `insert_ts` to `create_ts`.

## [1.1.0] - 2025-01-18

### Added

- DDL-based schema definition with inline SQL comments preserved in database.
- `garmin_health_data/tables.ddl` - Single source of truth for database schema.
- `CLAUDE.md` - Development guidelines and architecture documentation.
- SQLFluff configuration for SQL formatting (matching openetl standards).
- Inline SQL comments for all 29 tables and columns viewable via `sqlite_master`.
- Instructions in README.md for viewing schema documentation.

### Changed

- Personal records processing now continues with warning when activity doesn't exist (previously skipped).
- Database schema creation now executes DDL file instead of using SQLAlchemy metadata.
- SQLAlchemy models now used exclusively for ORM operations (not schema generation).
- Improved code formatting consistency across entire codebase.

### Removed

- Foreign key constraint on `personal_record.activity_id` to allow processing PRs before activities exist.

### Developer

- Added `sqlfluff>=2.0` to dev dependencies.
- Applied complete formatting standards from CLAUDE.md.
- All Python files now comply with 88 character line limit.
- Enhanced documentation in README.md Database Schema section.

### Notes

- No breaking changes for end users.
- Existing databases continue to work without modification.
- Optional: Re-initialize database to get inline comment documentation in schema.

## [1.0.1] - 2024-12-16

### Fixed

- Version consistency between package files.

## [1.0.0] - 2024-12-01

### Added

- Initial release.
- Extract Garmin Connect health data to local SQLite database.
- 29 tables for comprehensive health and fitness data.
- Automatic deduplication via SQL `ON CONFLICT` clauses.
- FIT file processing for detailed activity time-series data.
- Command-line interface with `garmin` command.
- Support for all major data types: activities, sleep, training metrics, wellness data.
- Flexible authentication with OAuth tokens.
- Comprehensive documentation and examples.

[Unreleased]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.4...HEAD
[2.7.4]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.3...v2.7.4
[2.7.3]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.2...v2.7.3
[2.7.2]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.1...v2.7.2
[2.7.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.0...v2.7.1
[2.7.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.6.1...v2.7.0
[2.6.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.6.0...v2.6.1
[2.6.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.5.0...v2.6.0
[2.5.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.4.0...v2.5.0
[2.4.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.1.1...v2.2.0
[2.1.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.3...v2.1.0
[2.0.3]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.2...v2.0.3
[2.0.2]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/diegoscarabelli/garmin-health-data/releases/tag/v1.0.0
