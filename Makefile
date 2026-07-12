ROOT_DIR := $(shell dirname "$(realpath $(firstword $(MAKEFILE_LIST)))")

VENVDIR=venv
VENVBIN=$(VENVDIR)/bin
VENVDONE=$(VENVDIR)/.done
VENVPY=$(VENVBIN)/python

PIP=$(VENVPY) -m pip
PIP_COMPILE=$(VENVBIN)/pip-compile
PRE_COMMIT=$(VENVBIN)/pre-commit

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
	$(PRE_COMMIT) clean
	rm -rf venv __pycache__ .mypy_cache indexer/__pycache__

## update .pre-commit-config.yaml
update:
	$(PRE_COMMIT) autoupdate

## re-create requirements*.txt files
requirements: requirements.txt requirements-dev.txt requirements-pre.txt

# WISH: switch to "uv"? hopefully faster than pip-compile
#	avoid needing to install pip-tools!!

# build requirements to run
requirements.txt: $(PIP_COMPILE)
	$(PIP_COMPILE) \
		--strip-extras \
		pyproject.toml

# generate and/or update requirements-dev.txt (based on requirements.txt)
requirements-dev.txt: $(PIP_COMPILE) requirements.txt pyproject.toml
	echo "--constraint $(ROOT_DIR)/requirements.txt" | \
		$(PIP_COMPILE) \
		  	--strip-extras \
			--extra dev \
			--extra deploy \
			--output-file requirements-dev.txt \
			pyproject.toml

# generate and/or update requirements-pre.txt (based on requirements.txt)
requirements-pre.txt: $(PIP_COMPILE) requirements.txt pyproject.toml
	echo "--constraint $(ROOT_DIR)/requirements.txt" | \
		$(PIP_COMPILE) \
			--strip-extras \
			--extra pre-commit \
			--extra deploy \
			--output-file requirements-pre.txt \
			pyproject.toml

# avoids:
# circular dependency for building requirements-dev.txt,
# always installing pip-tools
$(VENVBIN)/pip-compile:
	$(PIP) install --upgrade pip-tools pip

## install all required dependencies for development
install: $(VENVDONE)

# .pre-commit-run.sh will reinstall from requirements-pre.txt when it changes
$(VENVDONE): $(VENVPY) requirements-dev.txt requirements-pre.txt
	$(PIP) install -r requirements-dev.txt
	$(PRE_COMMIT) install
	touch $(VENVDONE)

# create venv if not present:
# don't depend on VENVDIR: changes when VENVDONE created
$(VENVPY):
	python3 -m venv $(VENVDIR)

## check and format code
lint:	$(VENVDONE)
	$(PRE_COMMIT) run --all-files

## run pytests
test:

.PHONY: all clean help install install-app-dev install-deps-dev install-deps-prod lint test upgrade upgrade-dev upgrade-prod
