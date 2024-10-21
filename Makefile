ROOT_DIR := $(shell dirname "$(realpath $(firstword $(MAKEFILE_LIST)))")

VENVDIR=venv
VENVBIN=$(VENVDIR)/bin
VENVDONE=$(VENVDIR)/.done
VENVPY=$(VENVBIN)/python

PIP=$(VENVPY) -m pip
PIPTOOLS=$(VENVPY) -m piptools
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
	rm -rf __pycache__ .mypy_cache

# generate and/or update requirement.txt
upgrade-prod:
	$(PIP) install --upgrade pip-tools pip
	$(PIPTOOLS) compile \
		--strip-extras \
		pyproject.toml

# generate and/or update requirements-dev.txt (based on requirements.txt)
upgrade-dev:
	$(PRE_COMMIT) autoupdate
	echo "--constraint $(ROOT_DIR)/requirements.txt" | \
		$(PIPTOOLS) compile \
		  	--strip-extras \
			--extra dev \
			--output-file requirements-dev.txt \
			pyproject.toml

## generate and/or update requirement.txt and requirements-dev.txt files
upgrade: upgrade-prod upgrade-dev

# install requirement.txt dependencies
install-deps-prod:
	$(PIP) install -r requirements.txt

# install requirement-dev.txt dependencies
install-deps-dev:
	$(PIP) install -r requirements-dev.txt

# install this app with dev extras
install-app-dev:
# --no-deps so that we don't reinstall un-pinned dependencies from pyproject
	$(PIP) install --no-deps --editable ".[dev]"

## install all required dependencies for development
install: $(VENVPY) install-deps-dev install-app-dev
	$(PRE_COMMIT) install
	touch $(VENVDONE)

$(VENVDONE): requirements.txt requirements-dev.txt
	$(MAKE) install

# not depending on VENVDIR, which "changes" when VENVDONE created
$(VENVPY):
	python3 -m venv $(VENVDIR)

## check and format code
lint:	$(VENVDONE)
	$(PRE_COMMIT) run --all-files

## run pytests
test:

.PHONY: all clean help install install-app-dev install-deps-dev install-deps-prod lint test upgrade upgrade-dev upgrade-prod
