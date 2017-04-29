
PYTHON_HOME=$(shell ./findPython.sh)
ifeq ($(MAKELEVEL),0)
$(info Using Python from $(PYTHON_HOME))
endif
DEST=$(PYTHON_HOME)
BIN_DIR=$(DEST)/bin                 # Python virtual environment here
PYTHON=$(DEST)/bin/python
PIP=$(DEST)/bin/pip
VERBOSITY_NOSE=3
VERBOSITY_FISS=0
HIGHLEVEL_TESTS=--tests=firecloud/tests/highlevel_tests.py

help:
	@echo
	@echo "Build/test/install FISS for FireCloud. Needs GNUmake 3.81 or later"
	@echo
	@echo "Supported targets:"
	@echo
	@echo  "1. test                 Run all tests"
	@echo  "   test_cli             Run only high-level CLI tests"
	@echo  "   test_one             Run a single high-level CLI test"
	@echo  "2. install              Install locally with pip"
	@echo  "3. uninstall            Uninstall with pip"
	@echo  "4. publish              Submit to PyPI"
	@echo

test: invoke_tests

test_cli:
	@$(MAKE) invoke_tests TESTS=$(HIGHLEVEL_TESTS)

WHICH=
test_one:
	@# Example: make test_one WHICH=space_lock_unlock
	@# Example: make test_one WHICH=ping VERBOSITY_FISS=1 (shows API calls)
	@$(MAKE) invoke_tests TESTS=$(HIGHLEVEL_TESTS):TestFISS.test_$(WHICH)

invoke_tests:
	@FISS_TEST_VERBOSITY=$(VERBOSITY_FISS) \
	$(PYTHON) setup.py nosetests --verbosity=$(VERBOSITY_NOSE) \
	$(TESTS) \
	2>&1 | egrep -v "egg|nose|Using Python"

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

.PHONY: help test test_cli test_one install release publish clean
