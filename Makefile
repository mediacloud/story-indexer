ROOT_DIR := $(shell dirname "$(realpath $(firstword $(MAKEFILE_LIST)))")

all: install lint

## display this message
help:
	@echo  ''
	@echo  'Usage:'
	@echo  'make <target>'
	@echo  ''
	@echo  'Targets:'
	@awk '/^##/{c=substr($$0,3);next}c&&/^[[:alpha:]][[:alnum:]_-]+:/{print substr($$1,1,index($$1,":")),c}1{c=0}' $(MAKEFILE_LIST) | column -s: -t
	@echo  ''

## tidy up local dev environment
clean:
	rm -rf __pycache__ .mypy_cache

# generate and/or update requirement.txt 
upgrade-prod:
	python -m pip install --upgrade pip-tools pip
	python -m piptools compile \
		--strip-extras \
		pyproject.toml

# generate and/or update requirements-dev.txt (based on requirements.txt)
upgrade-dev:
	pre-commit autoupdate
	echo "--constraint $(ROOT_DIR)/requirements.txt" | \
		python -m piptools compile \
			--extra dev \
			--output-file requirements-dev.txt \
			pyproject.toml

## generate and/or update requirement.txt and requirements-dev.txt files
upgrade: upgrade-prod upgrade-dev

# install requirement.txt dependencies
install-deps-prod:
	python -m pip install -r requirements.txt

# install requirement-dev.txt dependencies
install-deps-dev:
	python -m pip install -r requirements-dev.txt

# install this app with dev extras
install-app-dev:
# --no-deps so that we don't reinstall un-pinned dependencies from pyproject
	python -m pip install --no-deps --editable ".[dev]"

## install all required dependencies for development
install: install-deps-dev install-app-dev

## check and format code
lint:
	pre-commit run --all-files

## run pytests
test:

.PHONY: all clean help install install-app-dev install-deps-dev install-deps-prod lint test upgrade upgrade-dev upgrade-prod
