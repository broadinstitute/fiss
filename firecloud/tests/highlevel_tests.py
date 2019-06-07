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
from firecloud import fccore
from firecloud import api as fapi
from firecloud.errors import *

if sys.version_info > (3,):
    from io import StringIO
else:
    from StringIO import StringIO

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

        fcconfig = fccore.config_get_all()
        fcconfig.set_verbosity(fiss_verbosity)
        cls.project = fcconfig.project
        if not cls.project:
            raise ValueError("Your configuration must define a FireCloud project")

        # Set up a temp workspace for duration of tests. And in case a previous
        # test failed, we attempt to unlock & delete before creating anew
        cls.workspace = getuser() + '_FISS_TEST'

        ret = call_func("space_exists", "-p", cls.project, "-w", cls.workspace)
        if ret and os.environ.get("REUSE_SPACE", None):
            return

        print("\tCreating test workspace ...\n", file=sys.stderr)
        r = fapi.unlock_workspace(cls.project, cls.workspace)
        r = fapi.delete_workspace(cls.project, cls.workspace)
        r = fapi.create_workspace(cls.project, cls.workspace)
        fapi._check_response_code(r, 201)

    @classmethod
    def tearDownClass(cls):
        print("\nFinishing high-level CLI tests ...\n", file=sys.stderr)
        if not os.environ.get("REUSE_SPACE", None):
            fapi.delete_workspace(cls.project, cls.workspace)

    def test_health(self):
        #  Should return a bytes of 'OK'
        self.assertEqual(call_func("health"), b'OK')

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
        self.assertNotIn("meth_exists", result)

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
        with Capturing() as result:
            ret = call_cli("space_list")
        self.assertEqual(0, ret)
        self.assertIn(self.project + '\t' + self.workspace, result)

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
        # To potentially save time, sanity check if entities already exist
        r1 = fapi.get_entity(self.project, self.workspace, "sample_set", "SS-TP")
        r2 = fapi.get_entity(self.project, self.workspace, "participant_set", "PARTICIP_SET_2")
        if r1.status_code == 200 and r2.status_code == 200:
            return

        if r1.status_code not in [404,200] or r2.status_code not in [404,200]:
            raise RuntimeError("while checking for sample_set/participant_set")

        print("\n\tLoading data entities for tests ...", file=sys.stderr)
        args =("entity_import", "-p", self.project, "-w", self.workspace)
        datapath = os.path.join("firecloud", "tests")
        call_func(*(args + ("-f", os.path.join(datapath, "participants.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "particip_set_members.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "particip_set.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "samples.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "sset_membership.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "sset.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "pairs.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "pairset_membership.tsv"))))
        call_func(*(args + ("-f", os.path.join(datapath, "pairset_attr.tsv"))))
        print("\t... done loading data ...", file=sys.stderr)
    
    def test_entity_delete(self):
        self.load_entities()
        ret = call_cli("-y", "entity_delete", "-p", self.project,
                       "-w", self.workspace, "-t", "sample_set",
                       "-e", "SS-NT")
        self.assertEqual(0, ret)
        with Capturing() as output:
            ret = call_cli("sset_list", "-p", self.project,
                           "-w", self.workspace)            
        self.assertNotIn("SS-NT", output)
    
    def test_entity_tsv(self):
        self.load_entities()
        
        with Capturing() as output:
            ret = call_cli('entity_tsv', "-p", self.project, "-w", self.workspace,
                           '-t', 'participant')
        self.assertEqual(0, ret)
        self.assertEqual(2002, len(output))

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
        # See FIXME comment in attr_pair for why we compare sorted lists here
        self.assertEqual(sorted(output.split()),
            ['PAIRSET-1',
            'entity:pair_set_id',
            'pset_attr1',
            'pset_attr1_value1',
            'pset_attr2',
            'pset_attr2_value1'])

    def test_config_ops(self):
        name = 'echo'
        ns = 'broadgdac'
        code = os.path.join('firecloud', 'tests', 'echo.wdl')
        if not call_func('meth_exists', name):
            ret = call_cli('meth_new', '--method', name, '--wdl', code)
            self.assertEqual(0, ret)
        self.assertEqual(True, call_func('meth_exists', name))

        # Generate a config template, using the 'echo' method in the
        # broadgdac namespace (which should be publicly readable)
        config = call_func('config_template',
            '--method', name,
            '--namespace', ns, '--snapshot-id', "2",
            '--entity-type', 'sample_set',
            '--configname', name)

        self.assertIn("methodRepoMethod", config)
        self.assertIn("namespace", config)
        output_attribute = "echo.echo_task.echoed"
        self.assertIn(output_attribute, config)

        input_attribute = "echo.echo_task.message"
        self.assertIn(input_attribute, config)

        config = json.loads(config)
        self.assertEqual(config['name'], name)

        # Set the output attribute (making it a valid config), then install
        config['outputs'][output_attribute] = 'workspace.echoed_results'
        config['inputs'][input_attribute] = '"Hello, from the simple GDAC echo task"'
        result = call_func('config_put', '-p', self.project,
            '-w', self.workspace,
            '-c', json.dumps(config))
        self.assertEqual(True, result)

        # Do some spot-checking using CLI to ensure the json is not corrupted.
        with Capturing() as config:
            ret = call_cli('config_get', '-p', self.project,
                '-w', self.workspace,
                '-c', name)
        self.assertEqual(0, ret)
        config = '\n'.join(config)

        self.assertTrue(len(config) != 0)

        config = json.loads(config)
        self.assertEqual(config['outputs'][output_attribute], 'workspace.echoed_results')
        self.assertEqual(config['rootEntityType'], 'sample_set')
        self.assertEqual(config['methodConfigVersion'], 1)

    def test_sample_list(self):

        self.load_entities()
        args = ('sample_list', '-p', self.project, '-w', self.workspace)

        # Sample set, by way of using default value for -t
        result = call_func(*(args + ('-e', 'SS-TP')))
        self.assertEqual(2000, len(result))
        self.assertIn('S-0-TP',   result)
        self.assertIn('S-501-TP', result)
        self.assertIn('S-1999-TP',result)

        # Single sample (silly, but FISS aims to tolerate such)
        result = call_func(*(args + ('-e', 'S-1000-TP', '-t', 'sample')))
        self.assertEqual(1, len(result))
        self.assertIn('S-1000-TP', result)

        # Pair
        result = call_func(*(args + ('-e', 'PAIR-3', '-t', 'pair')))
        self.assertEqual(2, len(result))
        self.assertIn('S-3-TP',   result)
        self.assertIn('S-3-NT', result)

        # Participant
        result = call_func(*(args + ('-e', 'P-23', '-t', 'participant')))
        self.assertEqual(2, len(result))
        self.assertIn('S-23-NT', result)
        self.assertIn('S-23-TP', result)

        # Workspace, by way of using all defaults (no entity or type args)
        result = call_func(*args)
        self.assertEqual(4000, len(result))
        self.assertIn('S-0-TP',    result)
        self.assertIn('S-1000-TP', result)
        self.assertIn('S-1999-TP', result)
        self.assertIn('S-999-NT',  result)
        self.assertIn('S-1999-NT', result)

    def test_pair_list(self):

        self.load_entities()
        args = ('pair_list', '-p', self.project, '-w', self.workspace)

        # Workspace
        result = call_func(*args)
        self.assertEqual(10, len(result))
        self.assertIn('PAIR-1',  result)
        self.assertIn('PAIR-5',  result)
        self.assertIn('PAIR-10', result)

        # Participant
        result = call_func(*(args + ('-e', 'P-9', '-t', 'participant')))
        self.assertEqual(1, len(result))
        self.assertIn('PAIR-9', result)

        # Pair set, by way of using default value for -t
        result = call_func(*(args + ('-e', 'PAIRSET-1')))
        self.assertEqual(5, len(result))
        self.assertEqual(sorted(result),
           ['PAIR-1', 'PAIR-2', 'PAIR-3', 'PAIR-4', 'PAIR-5'])

        # Single pair (silly, but FISS aims to tolerate such)
        result = call_func(*(args + ('-e', 'PAIR-4', '-t', 'pair')))
        self.assertEqual(1, len(result))
        self.assertIn('PAIR-4', result)

    def test_participant_list(self):

        self.load_entities()
        args = ('participant_list', '-p', self.project, '-w', self.workspace)

        # Workspace
        result = call_func(*args)
        self.assertEqual(2000, len(result))
        self.assertIn('P-0',  result)
        self.assertIn('P-1000',  result)
        self.assertIn('P-1999', result)

        # Participant set, by way of using default value for -t
        result = call_func(*(args + ('-e', 'PARTICIP_SET_2')))
        self.assertEqual(10, len(result))
        self.assertIn('P-11',  result)
        self.assertIn('P-15',  result)
        self.assertIn('P-20',  result)

        # Single participant (silly, but FISS aims to tolerate such)
        result = call_func(*(args + ('-e', 'P-1350', '-t', 'participant')))
        self.assertEqual(1, len(result))
        self.assertIn('P-1350', result)

    def test_set_export(self):

        self.load_entities()
        args = ('-p', self.project, '-w', self.workspace)

        # Sample set
        result = call_func(*(('sset_export', '-e', 'SS-TP') + args))
        self.assertEqual(2001, len(result))
        self.assertEqual('membership:sample_set_id\tsample_id', result[0])
        self.assertEqual('SS-TP\tS-0-TP', result[1])
        self.assertEqual('SS-TP\tS-999-TP', result[1000])
        self.assertEqual('SS-TP\tS-1999-TP', result[2000])

        # Pair set
        result = call_func(*(('pset_export', '-e', 'PAIRSET-1') + args))
        self.assertEqual(6, len(result))
        self.assertEqual('membership:pair_set_id\tpair_id', result[0])
        self.assertEqual('PAIRSET-1\tPAIR-2', result[2])
        self.assertEqual('PAIRSET-1\tPAIR-5', result[5])
        self.assertEqual('PAIRSET-1\tPAIR-1', result[1])

        # Participant set
        result = call_func(*(('ptset_export', '-e', 'PARTICIP_SET_2') + args))
        self.assertEqual(11, len(result))
        self.assertEqual('membership:participant_set_id\tparticipant_id', result[0])
        self.assertEqual('PARTICIP_SET_2\tP-12', result[2])
        self.assertEqual('PARTICIP_SET_2\tP-15', result[5])
        self.assertEqual('PARTICIP_SET_2\tP-20', result[10])

        # Non-existent sample set
        try:
            result = call_func(*(('sset_export', '-e', '_No_SuCh_SeT_') + args))
        except FireCloudServerError as e:
            self.assertEqual(e.code, 404)

def main():
    nose.main()

if __name__ == '__main__':
    main()
