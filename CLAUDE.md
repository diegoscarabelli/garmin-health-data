# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## README.md

The most important source of truth is the [`/README.md`](README.md) file, which contains the latest information about the project. You MUST read and understand the `README.md` file in order to provide accurate and relevant responses and coding assistance.

The `README.md` file includes:
- **Introduction**: Project overview, objectives, and features for extracting Garmin Connect health data to a local SQLite database.
- **Features**: Comprehensive data extraction, automatic deduplication, FIT file processing, and flexible authentication.
- **Installation**: Installation instructions via pip or from source.
- **Quick Start**: Authentication setup and basic usage examples.
- **Usage**: Detailed commands for data extraction, initialization, and status checking.
- **Data Categories**: Complete list of available data types with extraction frequency.
- **Database Schema**: Schema documentation with inline comments, SQLite adaptations, and table structure.
- **Repository Structure**: Overview of code organization (`garmin_health_data/`, `tests/`).
- **Development**: Setup instructions for contributors.
- **Comparisons**: Feature comparison with other tools (garmindb, garmy, etc.).

## Database Schema Architecture

Following the openetl pattern, the database schema is defined in `garmin_health_data/tables.ddl`:

- **tables.ddl**: Single source of truth for database schema with inline SQL comments.
- **models.py**: SQLAlchemy ORM models for data processing ONLY (not schema generation).
- **db.py**: Executes tables.ddl to create schema automatically.

### Schema Documentation

Inline comments in `tables.ddl` are preserved in the SQLite database and can be viewed:
```bash
sqlite3 garmin_data.db "SELECT sql FROM sqlite_master WHERE type='table' AND name='activity';"
```

### Formatting DDL Files

Use SQLFluff to format the DDL file (configured in `pyproject.toml`):
```bash
python -m sqlfluff fix garmin_health_data/tables.ddl
```

### Making Schema Changes

When modifying the schema:
1. Update `tables.ddl` with new/modified table definitions.
2. Update corresponding SQLAlchemy model in `models.py` to match.
3. Format with SQLFluff.
4. Users must re-initialize the database or manually apply changes.

## Code Execution Environment

This project uses standard Python packaging with development dependencies specified in `pyproject.toml`.

### Installation

```bash
# Install package in development mode
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests with coverage
pytest

# Run specific test file
pytest tests/test_processor.py

# Run with verbose output
pytest -v
```

## Formatting Standards

This project follows the same code quality standards as openetl:

### Python Formatting

- **Black**: Line length 88, Python 3.9+ target.
- **Autoflake**: Remove unused imports and variables.
- **Docformatter**: Format docstrings consistently.
- **Type hints**: Required for all function parameters and return values.
- **Docstring format**: Use `:param` and `:return` format.

### SQL Formatting

- **SQLFluff**: Format SQL files (configured in `pyproject.toml`).
- **Dialect**: SQLite.
- **Style**: Leading commas, 88 character line length.

### Quality Checklist

Before completing any code changes:

- [ ] Module docstring present
- [ ] Imports properly organized and sorted, no unused imports
- [ ] All functions have type hints
- [ ] All docstrings use `:param` and `:return` format
- [ ] Proper spacing and blank lines throughout file
- [ ] Black-compliant format
- [ ] No trailing whitespace
- [ ] Consistent indentation (4 spaces)
- [ ] Periods at the end of ALL sentences (comments, docstrings, log messages)
- [ ] No line longer than 88 characters
- [ ] Final newline at end of file

### Automated Formatting

Format Python files:
```bash
python -m black garmin_health_data/
python -m autoflake --in-place --remove-all-unused-imports -r garmin_health_data/
python -m docformatter --in-place -r garmin_health_data/
```

Format SQL files:
```bash
python -m sqlfluff fix garmin_health_data/tables.ddl
```

## Testing Requirements

- **Coverage**: Maintain high test coverage for all new code.
- **Test naming**: Use `test_*` pattern for test functions.
- **Test organization**: Mirror source structure in `tests/` directory.
- **Fixtures**: Use pytest fixtures for common setup.

## Authentication Handling

When working with authentication code:
- Never hardcode credentials.
- Use environment variables or secure credential storage.
- Follow existing patterns in `garmin_health_data/auth.py`.
- Respect token expiration and refresh logic.

## Data Processing Patterns

When adding new data processors:
- Follow existing patterns in `garmin_health_data/processor.py`.
- Use SQLAlchemy models from `models.py`.
- Implement proper error handling and logging.
- Handle NULL values and missing data gracefully.
- Use upsert patterns for deduplication.

## CLI Development

When modifying CLI commands:
- Use Click framework (existing in `garmin_health_data/cli.py`).
- Provide helpful error messages.
- Include progress indicators for long-running operations.
- Follow existing command structure and naming conventions.
