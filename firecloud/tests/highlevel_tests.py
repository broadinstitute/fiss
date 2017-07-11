#! /usr/bin/env python

from __future__ import print_function
import unittest
import logging
import sys
import os
from getpass import getuser
import nose
import json
from firecloud.fiss import main as fiss_func
from firecloud.fiss import main_as_cli as fiss_cli
from firecloud.fccore import fc_config_get_all
from firecloud import api as fapi
from io import StringIO             # cStringIO avoided, for Python2/3 compat

def call_func(*args):
    # Call HL API as Python function, returning objects.  Most tests use this
    # approach, but some use call_cli to ensure that use case is also exercised
    return fiss_func(["fissfc"] + list(args))

def call_cli(*args):
    # Call HL level API as from UNIX CLI,  prints to stdout & returns int status
    return fiss_cli(["fissfc"] + list(args))

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self
    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

class TestFISSHighLevel(unittest.TestCase):
    """ Exercise the high level interface of FISS, the (F)ireCloud (S)ervice (S)elector
    There should be at least one test per api call, with composite tests as feasible
    """
    @classmethod
    def setUpClass(cls):
        '''Set up FireCloud etc to run tests'''

        print("\nStarting high-level CLI tests ...\n", file=sys.stderr)

        fiss_verbosity = os.environ.get("FISS_TEST_VERBOSITY", None)
        if fiss_verbosity == None:
            fiss_verbosity = 0

        fcconfig = fc_config_get_all()
        fcconfig.set_verbosity(fiss_verbosity)
        cls.project = fcconfig.project
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
        #r = fapi.delete_workspace(cls.project, cls.workspace)

    def test_ping(self):
        #  Should return a string resembling '2017-07-07T17:37:23.228+0000'
        self.assertGreater(len(call_func("ping")), 9)

    def test_space_info(self):
        result = call_func("space_info","-p",self.project,"-w",self.workspace)
        result = json.loads(result)
        self.assertEqual(result['workspace']['name'], self.workspace)

    def test_dash_l(self):
        result = call_func('-l')
        self.assertIn("space_info", result)
        result = call_func("-l", "config")
        self.assertIn("config_validate", result)
        self.assertNotIn("space_info", result)
        with Capturing() as result:
            status = call_cli("-l", "config")
        self.assertEqual(0, status)
        self.assertIn("config_get", result)
        self.assertIn("config_acl", result)
        self.assertNotIn("flow_start", result)

    def test_dash_F(self):
        result = call_func("-F", "space_info")
        self.assertIn('def space_info(args)', result)

    def test_space_new_delete(self):
        space = self.workspace + "_snd"
        ret = call_func("space_new", "-p", self.project, "-w", space)
        self.assertEqual(0, ret)
        ret = call_func("-y", "space_delete", "-p", self.project,"-w",space)
        self.assertEqual(0, ret)

    def test_space_lock_unlock(self):
        args = ("-p", self.project, "-w", self.workspace)
        self.assertEqual(0, call_func('space_lock', *args))

        # Verify LOCKED worked, by trying to delete
        r = fapi.delete_workspace(self.project, self.workspace)
        fapi._check_response_code(r, 403)

        self.assertEqual(0, call_func('space_unlock', *args))

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
        result = call_func("space_search", "-b", metadata['bucketName'])
        self.assertIn(metadata['name'], ''.join(result))
        # Now search for goofy thing that should never be found
        self.assertEqual([], call_func("space_search", "-b", '__NoTTHeRe__'))

    def test_space_list(self):
        result = call_func("space_list")
        self.assertIn(self.workspace, ''.join(result))

    def test_space_exists(self):
        ret = call_func("space_exists", "-p", self.project, "-w",self.workspace)
        self.assertEqual(True, ret)

    def test_entity_import_and_list(self):
        args = ("entity_import", "-p", self.project, "-w", self.workspace,
               "-f", os.path.join("firecloud", "tests", "participants.tsv"))
        self.assertEqual(0, call_func(*args))
        # Verify import by spot-checking size and content of entity
        args = ("participant_list", "-p", self.project, "-w", self.workspace)
        result = call_func(*args)
        self.assertEqual(2000, len(result))
        self.assertIn('P-0',   result)
        self.assertIn('P-999', result)
        self.assertIn('P-1999',result)

    def test_attr_workspace(self):
        name = "workspace_attr"
        value = "test_value"
        call_func("-y", "attr_set", "-p", self.project, "-w", self.workspace,
                   "-a", name, "-v", value)

        result = call_func("attr_get", "-p", self.project, "-w", self.workspace,
                    "-a", name)
        self.assertEqual(result[name], value)

    def load_entities(self):
        # To potentially save time, check if entities already exist
        r = fapi.get_entity(self.project, self.workspace, "sample_set", "SS-NT")
        if r.status_code == 200:
            return

        if r.status_code != 404:
            raise RuntimeError("while determining if SS-NT sample_set exists")

        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "participants.tsv"))
        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "samples.tsv"))
        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "sset_membership.tsv"))
        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "sset.tsv"))
        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "pairs.tsv"))
        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "pairset_membership.tsv"))
        call_func("entity_import", "-p", self.project, "-w", self.workspace,
                   "-f", os.path.join("firecloud", "tests", "pairset_attr.tsv"))

    def test_attr_sample_set(self):
        self.load_entities()
        with Capturing() as output:
            ret = call_cli("attr_get", "-p", self.project, "-w", self.workspace,
                            "-t", "sample_set", "-e", "SS-TP", "-a", "set_attr_1")

        self.assertEqual(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        self.assertEqual(output, "entity:sample_set_id\tset_attr_1\nSS-TP\tValue-A")

        # Now call attr_set on a sample_set, followed by attr_get
        ret = call_cli("-y", "attr_set", "-p", self.project, "-w", self.workspace,
                   "-t", "sample_set", "-e", "SS-TP", "-a", "set_attr_1", "-v", "Value-E")
        self.assertEqual(0, ret)

        with Capturing() as output:
            ret = call_cli("attr_get", "-p", self.project, "-w", self.workspace,
                             "-t", "sample_set", "-e", "SS-TP", "-a", "set_attr_1")
        self.assertEqual(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        self.assertEqual(output, "entity:sample_set_id\tset_attr_1\nSS-TP\tValue-E")

    def test_attr_del(self):
        self.load_entities()
        ret = call_cli("-y", "attr_delete", "-p", self.project, "-w", self.workspace,
                   "-t", "sample_set", "-e", "SS-NT", "-a", "set_attr_1")
        self.assertEqual(0, ret)

        with Capturing() as output:
            ret = call_cli("attr_get", "-p", self.project, "-w", self.workspace,
                             "-t", "sample_set", "-e", "SS-NT")

        # Should return only set_attr_2, non-existent set_attr_1 will be ignored
        self.assertEqual(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        self.assertEqual(output, "entity:sample_set_id\tset_attr_2\nSS-NT\tValue-D")

    def test_attr_pair(self):
        self.load_entities()
        with Capturing() as output:
            ret = call_cli("attr_get", "-p", self.project, "-w", self.workspace,
                            "-t", "pair", "-e", "PAIR-1")

        self.assertEqual(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        # FIXME: sort order of of attributes is different between Python 2 & 3,
        # which we should properly address w/in internals of FISS; for now, we
        # avoid verbatim string comparison here, in favor of sorted lists
        self.assertEqual(sorted(output.split()),
           ['P-1',
            'PAIR-1',
            'S-1-NT',
            'S-1-TP',
            'attr1_value1',
            'attr2_value1',
            'case_sample',
            'control_sample',
            'entity:pair_id',
            'pair_attr1',
            'pair_attr2',
            'participant'])

    def test_attr_pair_set(self):
        self.load_entities()
        with Capturing() as output:
            ret = call_cli("attr_get", "-p", self.project, "-w", self.workspace,
                            "-t", "pair_set", "-e", "PAIRSET-1")

        self.assertEqual(0, ret)
        output = '\n'.join(output)
        logging.debug(output)
        self.assertEqual(output,
            'entity:pair_set_id\tpset_attr1\tpset_attr2\n'
            'PAIRSET-1\tpset_attr1_value1\tpset_attr2_value1')

def main():
    nose.main()

if __name__ == '__main__':
    main()
