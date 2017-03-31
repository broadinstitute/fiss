
PYTHON_HOME=$(shell ./findPython.sh)
$(info $(PYTHON_HOME))
DEST=$(PYTHON_HOME)
BIN_DIR=$(DEST)/bin                 # Python virtual environment here
PYTHON=$(DEST)/bin/python
PIP=$(DEST)/bin/pip

help:
	@echo
	@echo "test and install the firecloud command line tool"
	@echo
	@echo "Targets:"
	@echo
	@echo  "1. test                     Run nosetests for firecloud"
	@echo  "2. install                  Install locally with pip"
	@echo  "3. uninstall                Uninstall with pip"
	@echo  "4. publish                  Submit to PyPI"
	@echo

test:
	$(PYTHON) setup.py nosetests --verbosity=3 && \
	rm -rf build dist *.egg-info

test_fiss:
	$(PYTHON) setup.py nosetests --verbosity=3 -w firecloud/tests/highlevel_tests.py

install:
	$(PIP) install --upgrade .

reinstall:
	$(MAKE) uninstall
	$(MAKE) install

uninstall:
	$(PIP) uninstall -y firecloud

publish:
	$(PYTHON) setup.py sdist upload && \
	rm -rf build dist *.egg-info

image:
	docker build -t broadgdac/fiss .

clean:
	rm -rf build dist *.egg-info *~ */*~ *.pyc */*.pyc

.PHONY: help test test_fiss install release publish clean
