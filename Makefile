
# Makefile.inc: common definitions for use throughout the set of Makefiles
# in the FissFC build system.  GNU make 3.81 or later is required.

SHELL=/bin/bash
__FILE__=$(lastword $(MAKEFILE_LIST))
__PATH__=$(abspath $(dir $(__FILE__)))
ROOT=$(__PATH__)
PYTHON_HOME=$(shell $(ROOT)/util/findPython.sh python$(PYTHON_VER))
PYLINT=$(ROOT)/util/pylint_wrap.sh
VERSION=$(shell sed -n 's/^__version__ *= *"\(.*\)"/\1/p' $(ROOT)/firecloud/__about__.py)

DEST=$(PYTHON_HOME)
BIN_DIR=$(DEST)/bin                 # Python virtual environment here
PYTHON=$(DEST)/bin/python$(PYTHON_VER)
PIP=$(PYTHON) -m pip
VERBOSITY_FISS=0
TEST_DIR=firecloud/tests
TESTS=*_tests.py
HIGHLEVEL_TESTS=highlevel_tests.py
LOWLEVEL_TESTS=lowlevel_tests.py
SMOKE_TESTS=smoke_tests.py

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
	@echo  "   test_smoke           Run only offline tests (no credentials needed)"
	@echo  "   test27, test314      Run lint and offline tests in a Python 2.7 or"
	@echo  "                        3.14 container (DOCKER_TESTS=test for all)"
	@echo  "2. install              Install locally with pip"
	@echo  "   install_test         Install locally, with test dependencies"
	@echo  "3. uninstall            Uninstall with pip"
	@echo  "4. release              Tag and push v<version> from master; GitHub"
	@echo  "                        Actions then publishes to PyPI and Docker Hub"
	@echo  "                        and creates the GitHub Release"
	@echo  "   publish              Submit to PyPI manually (fallback only)"
	@echo  "   image, push_image    Build/push the Docker image manually (fallback)"
	@echo

test: lintify invoke_tests

test3:
	$(MAKE) -e PYTHON_VER=3 test

lintify:
	@echo Running lint to detect potential code problems earlier than at runtime
	@PYTHON=$(PYTHON) $(PYLINT) *.py firecloud/*.py

test_cli: test_highlevel

test_highlevel:
	@$(MAKE) invoke_tests TESTS=$(HIGHLEVEL_TESTS)

test_lowlevel:
	@$(MAKE) invoke_tests TESTS=$(LOWLEVEL_TESTS)

test_smoke:
	@$(MAKE) invoke_tests TESTS=$(SMOKE_TESTS)

WHICH=
test_one:
	@# Example: make test_one WHICH=space_lock_unlock
	@# Example: make test_one WHICH=ping VERBOSITY_FISS=1 (shows API calls)
	@# Example: make test_one WHICH=sample_list REUSE_SPACE=true (see below)
	@FISS_TEST_VERBOSITY=$(VERBOSITY_FISS) \
	PYTHONPATH=$(TEST_DIR)$${PYTHONPATH:+:$$PYTHONPATH} \
	$(PYTHON) -m unittest -v highlevel_tests.TestFISSHighLevel.test_$(WHICH)

# By default the tests create, populate & eventually delete a custom workspace.
# To change this, set REUSE_SPACE to any value, either here or on CLI; then,
# after running tests the space will be kept AND subsequent invocations will
# run faster by not re-creating the space and/or re-loading data into it

# Tests are run from the repository root (test data paths are relative to it),
# against the source tree rather than any installed copy of firecloud
invoke_tests:
	@FISS_TEST_VERBOSITY=$(VERBOSITY_FISS) \
	$(PYTHON) -m unittest discover -v -s $(TEST_DIR) -p '$(TESTS)'

# Run tests in a clean container for a given Python version. The source tree
# is mounted read-only and copied, so nothing is written back to it. FISS and
# gcloud credentials are mounted read-only when present, for DOCKER_TESTS=test
DOCKER_TESTS=lintify test_smoke
DOCKER_MOUNTS=$(if $(wildcard $(HOME)/.fissconfig),-v $(HOME)/.fissconfig:/root/.fissconfig:ro) \
	$(if $(wildcard $(HOME)/.config/gcloud),-v $(HOME)/.config/gcloud:/root/.config/gcloud:ro)

test27:
	@$(MAKE) test_docker PY_IMAGE=python:2.7

test314:
	@$(MAKE) test_docker PY_IMAGE=python:3.14

test_docker:
	docker run --rm -v $(ROOT):/src:ro $(DOCKER_MOUNTS) \
		-e PIP_DISABLE_PIP_VERSION_CHECK=1 -e PIP_NO_PYTHON_VERSION_WARNING=1 \
		-e PIP_ROOT_USER_ACTION=ignore -e REUSE_SPACE \
		$(PY_IMAGE) bash -c 'set -e; mkdir /fiss; \
		tar -C /src --exclude=./venv --exclude=./.git --exclude=./.idea \
			--exclude=__pycache__ --exclude="*.pyc" -cf - . | tar -C /fiss -xf -; \
		cd /fiss; pip install -q ".[test]"; \
		make $(DOCKER_TESTS) PYTHON=python VERBOSITY_FISS=$(VERBOSITY_FISS)'

LINTIFY=lintify
install: $(LINTIFY)
	$(PIP) install --upgrade .

install_test:
	$(PIP) install --upgrade '.[test]'

reinstall:
	$(MAKE) uninstall
	$(MAKE) install

uninstall:
	$(PIP) uninstall -y firecloud

# Pre-flight checks, then push the version tag that triggers
# .github/workflows/release.yml (PyPI upload and Docker image push, then
# GitHub Release)
release:
	@test -n "$(VERSION)" || { echo "Error: no __version__ found" >&2; exit 1; }
	@test "$$(git rev-parse --abbrev-ref HEAD)" = master || \
		{ echo "Error: releases are tagged from master" >&2; exit 1; }
	@test -z "$$(git status --porcelain --untracked-files=no)" || \
		{ echo "Error: uncommitted changes" >&2; exit 1; }
	@# Fetch master only: fetching all tags fails on any harmless local/remote
	@# tag difference (e.g. annotated vs lightweight) unrelated to this release
	@git fetch -q origin master || \
		{ echo "Error: could not fetch master from origin" >&2; exit 1; }
	@test "$$(git rev-parse HEAD)" = "$$(git rev-parse FETCH_HEAD)" || \
		{ echo "Error: master differs from origin/master" >&2; exit 1; }
	@! git rev-parse -q --verify "refs/tags/v$(VERSION)" > /dev/null || \
		{ echo "Error: tag v$(VERSION) already exists locally" >&2; exit 1; }
	@! git ls-remote --exit-code --tags origin "refs/tags/v$(VERSION)" > /dev/null || \
		{ echo "Error: tag v$(VERSION) already exists on origin" >&2; exit 1; }
	@$(ROOT)/util/changelog_notes.py $(VERSION) > /dev/null
	git tag -a v$(VERSION) -m "FISS v$(VERSION)"
	git push origin v$(VERSION)
	@echo "Release workflow: https://github.com/broadinstitute/fiss/actions/workflows/release.yml"

publish: clean
	$(PYTHON) setup.py sdist && \
	twine upload dist/* && \
	rm -rf build dist *.egg-info

IMAGE=broadgdac/fiss
image:
	docker build --platform=linux/amd64 -t $(IMAGE):$(VERSION) -t $(IMAGE):latest .
	@test "$$(docker run --rm $(IMAGE):$(VERSION) fissfc --version)" = "$(VERSION)" || \
		{ echo "Error: fissfc --version in $(IMAGE):$(VERSION) is not $(VERSION)" >&2; exit 1; }

push_image:
	docker push $(IMAGE):$(VERSION)
	docker push $(IMAGE):latest

clean:
	rm -rf build dist .eggs *.egg-info *~ */*~ *.pyc */*.pyc

.PHONY: help test test3 test_cli test_highlevel test_lowlevel test_smoke \
	test_one test27 test314 test_docker invoke_tests install install_test \
	reinstall uninstall release publish image push_image clean lintify
