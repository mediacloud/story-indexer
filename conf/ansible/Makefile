# Makefile to construct environment to run ansible
# (does NOT run scripts, yet, anyway!)

ROLESDIR=roles
ES_ANSIBLE_REPO=https://github.com/mediacloud/ansible-elasticsearch
ES_ROLE_NAME=mc.elasticsearch
ES_ROLE_DIR=$(ROLESDIR)/$(ES_ROLE_NAME)
ES_ROLE_DONE=$(ES_ROLE_DIR)/.done

VENVDIR=venv
VENVBIN=$(VENVDIR)/bin
VENVDONE=$(VENVDIR)/.done

ALL=$(VENVDONE) $(ES_ROLE_DIR)
CLEAN_REMOVE=$(ES_ROLE_DIR) $(VENVDIR)

all:	$(ALL)

################ clone elasticsearch-ansible role

$(ES_ROLE_DIR): $(ROLESDIR)
	cd $(ROLESDIR); git clone $(ES_ANSIBLE_REPO) $(ES_ROLE_NAME)

$(ROLESDIR):
	mkdir $(ROLESDIR)

################ create venv

$(VENVDONE): $(VENVDIR) Makefile requirements.txt
	$(VENVBIN)/python3 -m pip install -r requirements.txt
	touch $(VENVDONE)

$(VENVDIR):
	python3 -m venv $(VENVDIR)

################ housekeeping

clean:
	rm -rf $(CLEAN_REMOVE)
