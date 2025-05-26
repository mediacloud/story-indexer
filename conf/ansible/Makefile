# Makefile to construct environment to run ansible
# (does NOT run scripts, yet, anyway!)

ES_ANSIBLE_REPO=https://github.com/mediacloud/$(ES_ROLE_NAME)

VENVDIR=venv
VENVBIN=$(VENVDIR)/bin
VENVDONE=$(VENVDIR)/.done

ALL=$(VENVDONE)

all:	$(ALL)

################ create venv
setup_venv: $(VENVDONE)

$(VENVDONE): $(VENVDIR) Makefile requirements.txt
	$(VENVBIN)/python3 -m pip install -r requirements.txt
	touch $(VENVDONE)

$(VENVDIR):
	python3 -m venv $(VENVDIR)

################ housekeeping

clean:
	rm -rf $(VENVDIR)
