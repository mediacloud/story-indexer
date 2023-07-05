ROOT_DIR := $(shell dirname "$(realpath $(firstword $(MAKEFILE_LIST)))")

all: install lint

clean:
	rm -rf __pycache__ .mypy_cache

upgrade-prod:
	python -m pip install --upgrade pip-tools pip
	python -m piptools compile \
		--strip-extras \
		pyproject.toml

upgrade-dev:
	pre-commit autoupdate
	echo "--constraint $(ROOT_DIR)/requirements.txt" | \
		python -m piptools compile \
			--extra dev \
			--output-file requirements-dev.txt \
			pyproject.toml

upgrade: upgrade-prod upgrade-dev

install-deps-prod:
	python -m pip install -r requirements.txt

install-deps-dev:
	python -m pip install -r requirements-dev.txt

# --no-deps so that we don't reinstall un-pinned dependencies from pyproject
install-app:
	python -m pip install --no-deps --editable .

install: install-deps-dev install-app

lint:
	pre-commit run --all-files

test:

.PHONY: all clean install install-app install-deps-dev install-deps-prod lint test upgrade upgrade-dev upgrade-prod
