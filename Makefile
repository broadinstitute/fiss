
# Makefile.inc: common definitions for use throughout the set of Makefiles
# in the FissFC build system.  GNU make 3.81 or later is required.

SHELL=/bin/bash
__FILE__=$(lastword $(MAKEFILE_LIST))
__PATH__=$(abspath $(dir $(__FILE__)))
ROOT=$(__PATH__)
PYTHON_HOME=$(shell $(ROOT)/util/findPython.sh python$(PYTHON_VER))
PYLINT=$(ROOT)/util/pylint_wrap.sh

DEST=$(PYTHON_HOME)
BIN_DIR=$(DEST)/bin                 # Python virtual environment here
PYTHON=$(DEST)/bin/python$(PYTHON_VER)
PIP=$(DEST)/bin/pip$(PYTHON_VER)
VERBOSITY_NOSE=3
VERBOSITY_FISS=0
HIGHLEVEL_TESTS=--tests=firecloud/tests/highlevel_tests.py
LOWLEVEL_TESTS=--tests=firecloud/tests/lowlevel_tests.py

$(info Now using Python from $(PYTHON))

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

test: lintify invoke_tests

test3:
	$(MAKE) -e PYTHON_VER=3 test

lintify:
	@echo Running lint to detect potential code problems earlier than at runtime
	@$(PYLINT) *.py firecloud/*.py

test_highlevel:
	@$(MAKE) invoke_tests TESTS=$(HIGHLEVEL_TESTS)

test_lowlevel:
	@$(MAKE) invoke_tests TESTS=$(LOWLEVEL_TESTS)

WHICH=
test_one:
	@# Example: make test_one WHICH=space_lock_unlock
	@# Example: make test_one WHICH=ping VERBOSITY_FISS=1 (shows API calls)
	@# Example: make test_one WHICH=sample_list REUSE_SPACE=true (see below)
	@$(MAKE) invoke_tests TESTS=$(HIGHLEVEL_TESTS):TestFISSHighLevel.test_$(WHICH)

# By default the tests create, populate & eventually delete a custom workspace.
# To change this, set REUSE_SPACE to any value, either here or on CLI; then,
# after running tests the space will be kept AND subsequent invocations will
# run faster by not re-creating the space and/or re-loading data into it

invoke_tests:
	@FISS_TEST_VERBOSITY=$(VERBOSITY_FISS) \
	$(PYTHON) setup.py nosetests --verbosity=$(VERBOSITY_NOSE) \
	$(TESTS) \
	2>&1 | egrep -v "egg|nose|Using Python"

LINTIFY=lintify
install: $(LINTIFY)
	$(PIP) install --upgrade .

reinstall:
	$(MAKE) uninstall
	$(MAKE) install

uninstall:
	$(PIP) uninstall -y firecloud

publish: clean
	$(PYTHON) setup.py sdist && \
	twine upload dist/* && \
	rm -rf build dist *.egg-info

image:
	docker build -t broadgdac/fiss .

clean:
	rm -rf build dist .eggs *.egg-info *~ */*~ *.pyc */*.pyc

.PHONY: help test test_cli test_one install release publish clean lintify
