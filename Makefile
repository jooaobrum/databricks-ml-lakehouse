# Simplified Makefile with automatic version updating
PROJECT := databricks_ml_lakehouse
SCHEMA := olist
VERSION_FILE := version.txt
UV := uv
PYTHON := python3

# Check if version file exists, create if it doesn't
$(VERSION_FILE):
	@echo "Creating initial version file"
	@echo "0.0.1" > $(VERSION_FILE)

# Update version - increments the patch version
.PHONY: update-version
update-version: $(VERSION_FILE)
	@echo "Updating version..."
	@$(PYTHON) -c 'import os; \
		f="$(VERSION_FILE)"; \
		v=open(f).read().strip(); \
		parts=v.split("."); \
		parts[-1]=str(int(parts[-1])+1); \
		new_v=".".join(parts); \
		open(f,"w").write(new_v); \
		print(f"Version updated from {v} to {new_v}")'

# Create virtual environment
.PHONY: create-venv
create-venv:
	$(UV) venv -p 3.11 venv

# Install dependencies
.PHONY: install
install:
	$(UV) pip install -r pyproject.toml --all-extras --link-mode=copy --no-cache-dir
	$(UV) lock
	$(UV) pip install -e . --link-mode=copy --no-cache-dir

# Run linting
.PHONY: lint
lint:
	pre-commit run --all-files

# Clean build artifacts but preserve version
.PHONY: clean
clean:
	rm -rf **pycache** */__pycache__ dist build *.egg-info .pytest_cache
	find . -name "*.pyc" -delete

# Run tests
.PHONY: test
test:
	cd tests && $(UV) run pytest

# Build package with version update
.PHONY: build
build: update-version
	$(UV) build
	@echo "Build completed successfully with version $$(cat $(VERSION_FILE))"

# Copy wheel file to Databricks
.PHONY: update-whl-dbx
update-whl-dbx:
	@echo "Copying latest wheel file to Databricks..."
	@LATEST_WHL=$$(ls -t ./dist/*.whl | head -1) && \
		echo "Using wheel file: $$LATEST_WHL" && \
		databricks fs cp $$LATEST_WHL dbfs:/Volumes/uc_dev/$(SCHEMA)/packages/$(PROJECT)-latest-py3-none-any.whl --overwrite && \
		databricks fs cp $$LATEST_WHL dbfs:/Volumes/uc_qa/$(SCHEMA)/packages/$(PROJECT)-latest-py3-none-any.whl --overwrite && \
		databricks fs cp $$LATEST_WHL dbfs:/Volumes/uc_prod/$(SCHEMA)/packages/$(PROJECT)-latest-py3-none-any.whl --overwrite

# Full release process
.PHONY: release
release: clean update-version build update-whl-dbx

# Reset version
.PHONY: reset-version
reset-version:
	@echo "Resetting version..."
	@echo "0.0.1" > $(VERSION_FILE)

.PHONY: init-pre-commit
init-pre-commit:
	@echo "Running pre-commit..." && \
	pre-commit run --all-files

# Help command
.PHONY: help
help:
	@echo "Available targets:"
	@echo " create-venv  - Create Python 3.11 virtual environment"
	@echo " install      - Install dependencies from pyproject.toml"
	@echo " lint         - Run pre-commit hooks on all files"
	@echo " clean        - Remove build artifacts"
	@echo " test         - Run pytest"
	@echo " build        - Update version and build package"
	@echo " update-whl-dbx - Copy wheel file to Databricks"
	@echo " release      - Clean, update version, build, and copy to Databricks"
	@echo " update-version - Only update the version number"
	@echo " reset-version  - Reset version to 0.0.1"
	@echo " init-pre-commit  - Run pre-commits"
	@echo " help         - Show this help message"
	@echo ""
	@echo "Current version: $$(cat $(VERSION_FILE) 2>/dev/null || echo 'not set')"