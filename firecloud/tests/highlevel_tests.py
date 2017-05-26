#! /usr/bin/env python

from __future__ import print_function
import unittest
import json
import logging
import os, re
from getpass import getuser
import nose
from firecloud.fiss import main as fiss_main
from firecloud.fccore import fc_config_get
from firecloud import api as fapi

# Context manager to capture stdout when calling another function
# Source: http://stackoverflow.com/questions/16571150/how-to-capture-stdout-output-from-a-python-function-call
# from cStringIO import StringIO
# replace cStringIO with StringIO for python3 compatibility
from io import StringIO
import sys

def call_fiss(*args):
    return fiss_main(["fissfc"] + list(args))

def workspace_extract(response):
    p = re.compile(r'.*workspaceId.*?"name": "(.*?)".*')
    if isinstance(response, list):
        if sys.version_info > (3,):
            m = [m.group(1) for resp in response for m in [p.search(resp)] if m]
        else:
            rsp = ''.join(response)
            m = [m.group(1) for m in [p.search(rsp)] if m]
        return m.pop() if m else ""
    else:
        m = p.search(response)
    return m.group(1) if m else ""

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

class TestFISS(unittest.TestCase):
    """Unit test the firecloud.api module.

    There should be at least one test per api call,
    with composite tests as feasible.
    """
    @classmethod
    def setUpClass(cls):
        '''Set up FireCloud etc to run tests'''

        print("\nStarting high-level CLI tests ...\n", file=sys.stderr)

        fiss_verbosity = os.environ.get("FISS_TEST_VERBOSITY", None)
        if fiss_verbosity == None:
            fiss_verbosity = 0
        fapi.set_verbosity(fiss_verbosity)

        cls.project = fc_config_get("project")
        if not cls.project:
            raise ValueError("Your configuration must define a FireCloud project")

        # Set up a temp workspace for duration of tests. And in case a previous
        # test failed, we attempt to unlock & delete before creating anew
        cls.workspace = getuser() + '_FISS_TEST'
        r = fapi.unlock_workspace(cls.project, cls.workspace)
        r = fapi.delete_workspace(cls.project, cls.workspace)
        r = fapi.create_workspace(cls.project, cls.workspace)
        fapi._check_response_code(r, 201)

    @classmethod
    def tearDownClass(cls):
        print("\nFinishing high-level CLI tests ...\n", file=sys.stderr)
        r = fapi.delete_workspace(cls.project, cls.workspace)

    def test_ping(self):
        self.assertEqual(0, call_fiss("ping"))

    def test_space_info(self):
        with Capturing() as fiss_output:
            ret = call_fiss("space_info","-p",self.project,"-w",self.workspace)
        logging.debug(fiss_output)
        space_info = workspace_extract(fiss_output)
        self.assertEqual(space_info, self.workspace)
        self.assertEqual(0, ret)

    def test_dash_l(self):
        with Capturing() as output:
            ret = call_fiss('-l')
        output = ''.join(output)
        logging.debug(output)
        self.assertIn("space_info", output)

        with Capturing() as output:
            ret = call_fiss("-l", "config")
        output = ''.join(output)
        logging.debug(output)
        self.assertIn("config_validate", output)
        self.assertNotIn("space_info", output)

    def test_dash_F(self):
        with Capturing() as output:
            call_fiss("-F", "space_info")
        output = ''.join(output)
        logging.debug(output)
        self.assertIn('def space_info(args)', output)

    def test_space_new_delete(self):
        space = self.workspace + "_snd"
        ret = call_fiss("space_new", "-p", self.project, "-w", space)
        self.assertEqual(0, ret)
        ret = call_fiss("-y", "space_delete", "-p", self.project,"-w",space)
        self.assertEqual(0, ret)

    def test_space_lock_unlock(self):
        args = ("-p", self.project, "-w", self.workspace)
        with Capturing() as output:
            ret = call_fiss('space_lock', *args)
        logging.debug(''.join(output))
        self.assertEqual(0, ret)

        # Verify LOCKED worked, by trying to delete
        r = fapi.delete_workspace(self.project, self.workspace)
        fapi._check_response_code(r, 403)

        with Capturing() as output:
            ret = call_fiss('space_unlock', *args)
        logging.debug(''.join(output))
        self.assertEqual(0, ret)

        # Verify UNLOCKED, again by trying to delete
        r = fapi.delete_workspace(self.project, self.workspace)
        fapi._check_response_code(r, 202)

        # Lastly, recreate space in case this test was run in series with others
        r = fapi.create_workspace(self.project, self.workspace)
        fapi._check_response_code(r, 201)

    def test_space_search(self):
        # First retrieve information about the space
        r = fapi.get_workspace(self.project, self.workspace)
        fapi._check_response_code(r, 200)
        metadata = r.json()["workspace"]
        # Now use part of that info (bucket id) to find the space (name)
        with Capturing() as output:
            ret = call_fiss("space_search", "-b", metadata['bucketName'])
        self.assertEqual(0, ret)
        self.assertIn(metadata['name'], ''.join(output))

    def test_space_list(self):
        with Capturing() as output:
            ret = call_fiss("space_list")
        self.assertIn(self.workspace,''.join(output))
        self.assertEquals(0, ret)

    def test_space_exists(self):
        ret = call_fiss("space_exists", "-p", self.project, "-w", self.workspace)
        self.assertEquals(True, ret)

    def test_entity_import_and_list(self):
        args = ("entity_import", "-p", self.project, "-w", self.workspace,
               "-f", os.path.join("firecloud", "tests", "participants.tsv"))
        ret = call_fiss(*args)
        self.assertEquals(0, ret)

        # Verify import by spot-checking length and content of
        args = ("participant_list", "-p", self.project, "-w", self.workspace)
        with Capturing() as output:
            ret = call_fiss(*args)
        self.assertEquals(0, ret)
        self.assertEquals(2000, len(output))
        self.assertEquals(True, 'P-0' in output)
        self.assertEquals(True, 'P-999' in output)
        self.assertEquals(True, 'P-1999' in output)

    def test_attr_workspace(self):
        call_fiss("-y", "attr_set", "-p", self.project, "-w", self.workspace,
                   "-a", "workspace_attr", "-v", "test_value")

        with Capturing() as output:
            ret = call_fiss("attr_get", "-p", self.project, "-w", self.workspace,
                             "-a", "workspace_attr")
        self.assertEquals(0, ret)
        output = ''.join(output)
        logging.debug(output)
        self.assertEquals(output, "workspace_attr\ttest_value")

    def test_attr_ops(self):
        # Upload the 4 test data files
        call_fiss("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "participants.tsv"))
        call_fiss("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "samples.tsv"))
        call_fiss("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "sset_membership.tsv"))
        call_fiss("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "sset.tsv"))

        # Now call attr_get
        with Capturing() as output:
            ret = call_fiss("attr_get", "-p", self.project, "-w", self.workspace,
                            "-t", "sample_set", "-a", "set_attr_1")

        self.assertEquals(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        self.assertEquals(output, "sample_set_id\tset_attr_1\nSS-NT\tValue-C\nSS-TP\tValue-A")

        # Now call attr_set on a sample_set, followed by attr_get
        call_fiss("-y", "attr_set", "-p", self.project, "-w", self.workspace,
                   "-t", "sample_set", "-e", "SS-TP", "-a", "set_attr_1", "-v", "Value-E")

        with Capturing() as output:
            ret = call_fiss("attr_get", "-p", self.project, "-w", self.workspace,
                             "-t", "sample_set", "-a", "set_attr_1")
        self.assertEquals(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        self.assertEquals(output, "sample_set_id\tset_attr_1\nSS-NT\tValue-C\nSS-TP\tValue-E")

def main():
    nose.main()

if __name__ == '__main__':
    main()
