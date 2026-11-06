#! /usr/bin/env python
"""Offline smoke tests for FISS: no network access or credentials required.

These verify that the package imports, the configuration parser works, and
the CLI starts, on every supported Python version (2.7 and 3.x). The
high/low level tests exercise the live FireCloud/Terra API.
"""

from __future__ import print_function
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
import warnings

from firecloud import fccore
from firecloud.__about__ import __version__

PKG_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(PKG_DIR)


def run_cli(*args):
    """Run the FISS CLI in a subprocess, returning (exit code, output)."""
    proc = subprocess.Popen([sys.executable, "-m", "firecloud.fiss"] +
                            list(args),
                            cwd=ROOT_DIR,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT)
    out, _ = proc.communicate()
    return proc.returncode, out.decode("utf-8")


class TestFISSSmoke(unittest.TestCase):

    def test_version_string(self):
        self.assertTrue(re.match(r"^\d+\.\d+\.\d+$", __version__))

    def test_imports(self):
        # firecloud.{config,method,submission,workspace} are omitted: they
        # reference api.PROD_API_ROOT, which was removed long ago, and do not
        # import on any Python version
        import firecloud.api
        import firecloud.entity
        import firecloud.errors
        import firecloud.fiss
        import firecloud.supervisor

    def test_sources_compile_without_warnings(self):
        # Catches e.g. invalid escape sequences, a SyntaxWarning in 3.12+
        sources = [os.path.join(ROOT_DIR, "setup.py")]
        for d in (PKG_DIR, os.path.join(PKG_DIR, "tests")):
            sources += [os.path.join(d, f) for f in sorted(os.listdir(d))
                        if f.endswith(".py")]
        for path in sources:
            with open(path) as fh:
                source = fh.read()
            with warnings.catch_warnings():
                warnings.simplefilter("error")
                compile(source, path, "exec")

    def test_config_parse(self):
        cfg = fccore.config_get_all()
        saved = dict(cfg)
        tmpdir = tempfile.mkdtemp()
        try:
            path = os.path.join(tmpdir, "fissconfig")
            with open(path, "w") as fh:
                fh.write("[DEFAULT]\n"
                         "project = smoke-project\n"
                         "workspace = smoke-workspace\n"
                         "[smoke_section]\n"
                         "smoke_option = %(project)s-option\n")
            # Nonexistent profile name: ignore the user's ~/.fissconfig
            fccore.config_parse(files=path,
                                config_profile=".fissconfig-smoke-test")
            self.assertEqual(cfg.project, "smoke-project")
            self.assertEqual(cfg.workspace, "smoke-workspace")
            self.assertEqual(cfg.smoke_section.smoke_option,
                             "smoke-project-option")
            self.assertTrue(cfg.root_url.endswith("/"))
        finally:
            shutil.rmtree(tmpdir)
            cfg.clear()
            cfg.update(saved)

    def test_cli_version(self):
        code, out = run_cli("--version")
        self.assertEqual(code, 0)
        self.assertEqual(out.strip(), __version__)

    def test_cli_help(self):
        code, out = run_cli("-h")
        self.assertEqual(code, 0)
        self.assertIn("usage: fissfc", out)

    def test_cli_list_commands(self):
        code, out = run_cli("-l")
        self.assertEqual(code, 0)
        self.assertIn("space_clone", out.split())

    def test_create_submission_positional_order(self):
        # New parameters must be appended, so existing positional callers of
        # this public function keep working
        import inspect
        from firecloud import api
        try:
            params = inspect.getfullargspec(api.create_submission).args
        except AttributeError:  # Python 2
            params = inspect.getargspec(api.create_submission).args
        self.assertEqual(params, [
            "wnamespace", "workspace", "cnamespace", "config", "entity",
            "etype", "expression", "use_callcache",
            "delete_intermediate_output_files", "use_reference_disks",
            "memory_retry_multiplier", "workflow_failure_mode",
            "user_comment", "per_workflow_cost_cap"])

    def test_cli_config_start_help(self):
        code, out = run_cli("config_start", "-h")
        self.assertEqual(code, 0)
        self.assertIn("--per_workflow_cost_cap", out)

    def test_cli_space_clone_help(self):
        code, out = run_cli("space_clone", "-h")
        self.assertEqual(code, 0)
        self.assertIn("--bucketLocation", out)


if __name__ == '__main__':
    unittest.main()
