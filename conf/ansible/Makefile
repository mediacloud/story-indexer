# Makefile to construct environment to run ansible
# (does NOT run scripts, yet, anyway!)

ROLESDIR=roles
ES_ROLE_NAME=ansible-elasticsearch
ES_ANSIBLE_REPO=https://github.com/mediacloud/$(ES_ROLE_NAME)
ES_ROLE_DIR=$(ROLESDIR)/$(ES_ROLE_NAME)
ES_ROLE_DONE=$(ES_ROLE_DIR)/.done

VENVDIR=venv
VENVBIN=$(VENVDIR)/bin
VENVDONE=$(VENVDIR)/.done

ALL=$(VENVDONE) $(ES_ROLE_DIR)

all:	$(ALL)

################ clone elasticsearch-ansible role

$(ES_ROLE_DIR): $(ROLESDIR)
	cd $(ROLESDIR); git clone $(ES_ANSIBLE_REPO)

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
	rm -rf $(VENVDIR)
	test "x`cd $(ES_ROLE_DIR); git diff --stat`" = 'x' && rm -rf $(ES_ROLE_DIR)
