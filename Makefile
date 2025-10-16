PYTHON = .venv/bin/python
PIP = .venv/bin/pip

venv:
	if [ ! -d ".venv" ]; then python -m venv .venv; fi
	$(PIP) install -q -e ".[dev]"

format: venv
	$(PYTHON) -m autoflake .
	$(PYTHON) -m docformatter --in-place . || { if [ $$? -eq 1 ]; then true; else exit 1; fi; }
	$(PYTHON) -m black -q .

check-format: venv
	@failed=0; \
	echo "Checking: autoflake"; $(PYTHON) -m autoflake --check . || { failed=1; }; \
	echo "Checking: docformatter"; $(PYTHON) -m docformatter --check . || { failed=1; }; \
	echo "Checking: black"; $(PYTHON) -m black --check . || { failed=1; }; \
	exit $$failed

test: venv
	$(PYTHON) -m pytest tests --cov

clean:
	rm -rf dist/ build/ *.egg-info

build: venv clean
	$(PIP) install -q --upgrade build twine
	$(PYTHON) -m build

upload-test: build
	$(PYTHON) -m twine upload --repository testpypi dist/*

upload: build
	$(PYTHON) -m twine upload dist/*

delete-venv:
	rm -rf .venv
