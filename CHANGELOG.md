# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.4.0] - 2026-04-06

### Added

- **`activity_path` table**: New table eagerly materializing GPS coordinate sequences from FIT files during processing. Each row stores an ordered `[longitude, latitude]` JSON array sorted ascending by timestamp, ready for deck.gl or any path-layer visualization. Populated automatically during FIT file processing via delete+insert for reprocessing idempotency. Activities without GPS samples (indoor workouts) have no row. Mirrors the `garmin.activity_path` table added to the openetl Garmin pipeline.
  - Three CHECK constraints enforce `path_json` integrity: valid JSON, array type, and `point_count` matching `json_array_length(path_json)`. Requires the SQLite JSON1 extension (bundled with SQLite since 3.9, enabled by default in Python's built-in `sqlite3` module).
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

[Unreleased]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.3.0...HEAD
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
