# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.1.0...HEAD
[1.1.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/diegoscarabelli/garmin-health-data/releases/tag/v1.0.0
