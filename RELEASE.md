# Release Process for garmin-health-data

This document outlines the steps to release a new version of garmin-health-data to PyPI.

## Prerequisites

- [x] PyPI account created at https://pypi.org.
- [x] API token generated for PyPI.
- [x] `PYPI_API_TOKEN` secret configured in GitHub repository settings.
- [x] Build tools installed locally (for manual releases): `pip install --upgrade build twine`.

## Pre-Release Checklist

Before creating a release, ensure:

- [ ] All tests pass: `make test`.
- [ ] Code is properly formatted: `make check-format`.
- [ ] Version bumped in `pyproject.toml`.
- [ ] README.md is up to date.
- [ ] CHANGELOG updated (if exists).
- [ ] All changes committed to git.
- [ ] Working on main branch (or release branch).

## Automated Release (Recommended)

The repository uses GitHub Actions to automatically publish to PyPI when you create a GitHub Release.

### 1. Update Version

Edit `pyproject.toml` and update the version number:

```toml
version = "1.2.0"  # Update this line
```

### 2. Update CHANGELOG (if applicable)

Document the changes included in this release.

### 3. Commit and Push Changes

```bash
git add pyproject.toml CHANGELOG.md
git commit -m "Bump version to 1.2.0"
git push origin main
```

### 4. Create GitHub Release

Choose one of the following methods:

**Option A: GitHub Web Interface**
1. Navigate to https://github.com/diegoscarabelli/garmin-health-data/releases/new.
2. Click "Select tag" dropdown → Type `v1.2.0` → The UI will offer to create the new tag on publish.
3. Verify "Target" is set to `main`.
4. Enter release title: `v1.2.0`.
5. Add release notes describing the changes (or click "Generate release notes" for auto-generated notes).
6. Click "Publish release".

**Option B: GitHub CLI**
```bash
gh release create v1.2.0 --title "v1.2.0" --notes "Release notes here"
```

**Option C: Git Tag (requires manual release creation)**
```bash
git tag -a v1.2.0 -m "Release version 1.2.0"
git push origin v1.2.0
# Then create release on GitHub using this tag
```

### 5. Monitor Automated Publishing

The GitHub Actions workflow will automatically:
- Build the package.
- Validate with `twine check`.
- Publish to PyPI using the `PYPI_API_TOKEN` secret.

**Monitor progress:**
- Workflow runs: https://github.com/diegoscarabelli/garmin-health-data/actions
- Package on PyPI: https://pypi.org/project/garmin-health-data/

### 6. Verify Installation

Test the new release in a fresh environment:

```bash
pip install --upgrade garmin-health-data
garmin --version  # Should display the new version
garmin --help     # Verify functionality
```

## Manual Release (Alternative)

If you need to publish manually (e.g., for testing or if automated workflow fails):

### 1. Configure PyPI Credentials

Create `~/.pypirc` with your API token:

```ini
[pypi]
username = __token__
password = pypi-YOUR_PRODUCTION_TOKEN_HERE
```

Secure the file:
```bash
chmod 600 ~/.pypirc
```

### 2. Clean Previous Builds

```bash
rm -rf dist/ build/ *.egg-info
```

### 3. Build Distribution Packages

```bash
python -m build
# Or with project venv:
.venv/bin/python -m build
```

This creates:
- `dist/garmin_health_data-VERSION.tar.gz` (source distribution).
- `dist/garmin_health_data-VERSION-py3-none-any.whl` (wheel).

### 4. Check Package Quality

```bash
python -m twine check dist/*
```

### 5. Upload to Production PyPI

```bash
python -m twine upload dist/*
```

View at: https://pypi.org/project/garmin-health-data/

### 6. Verify Installation

```bash
# In a fresh environment
pip install --upgrade garmin-health-data
garmin --version
garmin --help
```

### 7. Create GitHub Release (Optional)

If you haven't already triggered the automated workflow:

```bash
# Tag the release
git tag -a v1.0.0 -m "Release version 1.0.0"

# Push the tag
git push origin v1.0.0
```

Then create a release on GitHub:
- Go to https://github.com/diegoscarabelli/garmin-health-data/releases/new.
- Select tag: v1.0.0.
- Release title: "v1.0.0".
- Add release notes.
- Publish release.

## Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.0.0).
- **MAJOR**: Incompatible API changes.
- **MINOR**: New functionality, backwards compatible.
- **PATCH**: Bug fixes, backwards compatible.

## CI/CD Pipeline

The repository uses GitHub Actions for continuous integration and delivery:

### Code Quality Checks (`ci.yml`)

Runs on every push to `main`/`develop` and on all PRs to `main`:

- **Linting and Formatting**: Uses `make check-format` to run:
  - `black` for Python code formatting.
  - `autoflake` for removing unused imports.
  - `docformatter` for docstring formatting.
  - `sqlfluff` for SQL linting.
  
- **Testing**: Runs test suite across matrix:
  - Operating Systems: Ubuntu, macOS, Windows.
  - Python Versions: 3.9, 3.10, 3.11, 3.12.
  - Uses `pytest` with coverage reporting.

**All CI checks must pass before creating a release.**

### Automated Publishing (`publish.yml`)

Triggered when you create a GitHub Release:

1. Checks out repository.
2. Sets up Python 3.12.
3. Installs build dependencies.
4. Builds source distribution and wheel.
5. Validates package with `twine check`.
6. Publishes to PyPI using `PYPI_API_TOKEN` secret.

## Troubleshooting

### Automated Workflow

**"publish.yml workflow failed"**
- Check GitHub Actions logs: https://github.com/diegoscarabelli/garmin-health-data/actions.
- Verify `PYPI_API_TOKEN` secret is correctly configured.
- Ensure version in `pyproject.toml` doesn't already exist on PyPI.

**"CI checks failed"**
- Run locally: `make check-format` and `make test`.
- Fix any linting or test failures before creating release.
- Push fixes and wait for CI to pass.

### Manual Upload

**"Package already exists"**
- Increment version in `pyproject.toml` and rebuild.
- PyPI doesn't allow re-uploading same version.

**"Invalid credentials"**
- Verify token in `~/.pypirc` starts with `pypi-`.
- Ensure file permissions: `chmod 600 ~/.pypirc`.

**"File already uploaded"**
- PyPI doesn't allow overwriting versions.
- Increment version and rebuild.

## Quick Reference

### Automated Release (Recommended)

```bash
# 1. Update version in pyproject.toml
# 2. Commit and push changes
git add pyproject.toml
git commit -m "Bump version to 1.2.0"
git push origin main

# 3. Create GitHub Release (triggers automated publish)
gh release create v1.2.0 --title "v1.2.0" --notes "Release notes"

# 4. Monitor: https://github.com/diegoscarabelli/garmin-health-data/actions
```

### Manual Release (Alternative)

```bash
# Run quality checks first
make test && make check-format

# Build and upload
rm -rf dist/ build/ *.egg-info
python -m build
python -m twine check dist/*
python -m twine upload dist/*

# Tag and create release
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0
```
