# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.1...HEAD
[2.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/diegoscarabelli/garmin-health-data/releases/tag/v1.0.0
