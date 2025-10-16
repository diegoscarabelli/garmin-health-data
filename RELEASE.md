# Release Process for garmin-health-data

This document outlines the steps to release a new version of garmin-health-data to PyPI.

## Prerequisites

- [x] PyPI account created at https://pypi.org
- [x] TestPyPI account created at https://test.pypi.org
- [x] API tokens generated for both PyPI and TestPyPI
- [x] Build tools installed: `pip install --upgrade build twine`

## Pre-Release Checklist

Before creating a release, ensure:

- [ ] All tests pass: `make test`
- [ ] Code is properly formatted: `make check-format`
- [ ] Version bumped in `pyproject.toml`
- [ ] README.md is up to date
- [ ] CHANGELOG updated (if exists)
- [ ] All changes committed to git
- [ ] Working on main branch (or release branch)

## Release Steps

### 1. Configure PyPI Credentials

Create `~/.pypirc` with your API tokens:

```ini
[distutils]
index-servers =
    pypi
    testpypi

[pypi]
username = __token__
password = pypi-YOUR_PRODUCTION_TOKEN_HERE

[testpypi]
repository = https://test.pypi.org/legacy/
username = __token__
password = pypi-YOUR_TEST_TOKEN_HERE
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
- `dist/garmin_health_data-VERSION.tar.gz` (source distribution)
- `dist/garmin_health_data-VERSION-py3-none-any.whl` (wheel)

### 4. Upload to TestPyPI

```bash
twine upload --repository testpypi dist/*
# Or with project venv:
.venv/bin/twine upload --repository testpypi dist/*
```

View at: https://test.pypi.org/project/garmin-health-data/

### 5. Test Installation from TestPyPI

```bash
# Create test environment
python -m venv test_install
source test_install/bin/activate

# Install from TestPyPI
pip install --index-url https://test.pypi.org/simple/ \
    --extra-index-url https://pypi.org/simple/ \
    garmin-health-data

# Test functionality
garmin --help
garmin --version

# Clean up
deactivate
rm -rf test_install
```

### 6. Upload to Production PyPI

Once testing is successful:

```bash
twine upload dist/*
# Or with project venv:
.venv/bin/twine upload dist/*
```

View at: https://pypi.org/project/garmin-health-data/

### 7. Create GitHub Release

```bash
# Tag the release
git tag -a v1.0.0 -m "Release version 1.0.0"

# Push the tag
git push origin v1.0.0
```

Then create a release on GitHub:
1. Go to https://github.com/diegoscarabelli/garmin-health-data/releases/new
2. Select tag: v1.0.0
3. Release title: "v1.0.0"
4. Add release notes
5. Publish release

### 8. Verify Production Installation

```bash
# In a fresh environment
pip install garmin-health-data

# Test
garmin --version
garmin --help
```

## Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.0.0)
- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality, backwards compatible
- **PATCH**: Bug fixes, backwards compatible

## Troubleshooting

**"Package already exists"**
- Increment version in `pyproject.toml` and rebuild

**"Invalid credentials"**
- Verify token in `~/.pypirc` starts with `pypi-`
- Ensure file permissions: `chmod 600 ~/.pypirc`

**"File already uploaded"**
- PyPI doesn't allow re-uploading same version
- Increment version and rebuild

**Dependencies fail on TestPyPI**
- Normal behavior - TestPyPI doesn't have all packages
- Use `--extra-index-url https://pypi.org/simple/` when installing

## Quick Reference

```bash
# Complete release workflow
make test && make check-format
rm -rf dist/ build/ *.egg-info
python -m build
twine upload --repository testpypi dist/*
# Test installation...
twine upload dist/*
git tag -a v1.0.0 -m "Release version 1.0.0"
git push origin v1.0.0
```
