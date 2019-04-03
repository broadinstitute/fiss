#! /usr/bin/env python

from __future__ import print_function
import unittest
import time
import nose
import sys
import os
from getpass import getuser
from firecloud import fccore
from firecloud import api as fapi

class TestFISSLowLevel(unittest.TestCase):
    """Unit test the low-level interface of FireCloud-enabled FISS. There should
    be at least one test per low-level call, with composite tests as feasible.
    """

    @classmethod
    def setUpClass(cls, msg=""):
        '''Set up FireCloud etc to run tests'''

        print("\nStarting low-level api tests ...\n", file=sys.stderr)

        fiss_verbosity = os.environ.get("FISS_TEST_VERBOSITY", None)
        if fiss_verbosity == None:
            fiss_verbosity = 0

        fcconfig = fccore.config_parse()
        cls.project = fcconfig.project
        if not cls.project:
            raise ValueError("Your configuration must define a FireCloud project")
        fcconfig.set_verbosity(fiss_verbosity)

        # Set up a temp workspace for duration of tests; and in case a previous
        # test failed, attempt to unlock & delete before creating anew.  Note
        # that bc we execute space create/delete here, their tests are NO-OPs
        cls.workspace = getuser() + '_FISS_TEST'
        r = fapi.unlock_workspace(cls.project, cls.workspace)
        r = fapi.delete_workspace(cls.project, cls.workspace)
        r = fapi.create_workspace(cls.project, cls.workspace)
        fapi._check_response_code(r, 201)

    @classmethod
    def tearDownClass(cls):
        print("\nFinishing low-level CLI tests ...\n", file=sys.stderr)
        r = fapi.delete_workspace(cls.project, cls.workspace)

    # Test individual api calls, 1 test per api call,
    # listed in alphabetical order for convenience
    @unittest.skip("Not Implemented")
    def test_abort_submission(self):
        """Test abort_submission()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_add_user_to_group(self):
        """Test add_user_to_group()."""
        pass

    def test_clone_workspace(self):
        """Test clone_workspace()."""
        temp_space = getuser() + '_FISS_TEST_CLONE'
        r = fapi.unlock_workspace(self.project, temp_space)
        r = fapi.delete_workspace(self.project, temp_space)
        r =  fapi.clone_workspace(self.project, self.workspace,
                                  self.project, temp_space)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 201)

        # Compare new workspace and old workspace
        # Cleanup, Delete workspace
        r =  fapi.delete_workspace(self.project, temp_space)
        print(r.status_code, r.content)
        self.assertIn(r.status_code, [200, 202])

    @unittest.skip("Not Implemented")
    def test_copy_config_from_repo(self):
        """Test copy_config_from_repo()."""
        pass

    @unittest.skip("Not Implemented")
    def test_copy_config_to_repo(self):
        """Test copy_config_to_repo()."""
        pass

    @unittest.skip("Not Implemented")
    def test_copy_entities(self):
        """Test copy_entities()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_create_group(self):
        """Test create_group()."""
        pass

    @unittest.skip("Not Implemented")
    def test_create_submission(self):
        """Test create_submission()."""
        pass

    @unittest.skip("Not Implemented")
    def test_create_workspace_config(self):
        """Test create_config()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_entity(self):
        """Test delete_entity()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_delete_group(self):
        """Test delete_group()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_pair(self):
        """Test delete_pair()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_pair_set(self):
        """Test delete_pair_set()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_participant(self):
        """Test delete_participant()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_participant_set(self):
        """Test delete_participant_set()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_repository_method(self):
        """Test delete_repository_method()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_sample(self):
        """Test delete_sample()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_sample_set(self):
        """Test delete_sample_set()."""
        pass

    def test_create_workspace(self):
        # NO-OP, because this feature is tested in setUpClass & elsewhere
        pass

    def test_delete_workspace(self):
        # NO-OP, because this feature is tested in setUpClass & elsewhere
        pass

    @unittest.skip("Not Implemented")
    def test_delete_workspace_config(self):
        """Test delete_config()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_config_template(self):
        """Test get_config_template()."""
        pass

    def test_get_entities(self):
        """Test get_entities()."""
        r =  fapi.get_entities(self.project,
                               self.workspace,
                               "participant")
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    @unittest.skip("Not Implemented")
    def test_get_entities_tsv(self):
        """Test get_entities_tsv()."""
        r =  fapi.get_entities_tsv(self.project,
                                   self.workspace,
                                   "participant")
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_get_entities_with_type(self):
        """Test get_entities_with_type()."""
        r =  fapi.get_entities_with_type(self.project,
                                         self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    @unittest.skip("Not Implemented")
    def test_get_entity(self):
        """Test get_entity()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_group(self):
        """Test get_group()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_get_groups(self):
        """Test get_groups()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_inputs_outputs(self):
        """Test get_inputs_outputs()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_repository_config(self):
        """Test get_repository_config()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_repository_config_acl(self):
        """Test get_repository_config_acl()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_repository_method(self):
        """Test get_repository_method()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_repository_method_acl(self):
        """Test get_repository_method_acl()."""
        pass

    def test_get_api_methods_definitions(self):
        """Test get_api_methods_definitions()."""
        r = fapi.get_api_methods_definitions()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_get_status(self):
        """Test get_status()."""
        r =  fapi.get_status()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    @unittest.skip("Not Implemented")
    def test_get_submission(self):
        """Test get_submission()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_submission_queue(self):
        """Test get_submission_queue()."""
        pass

    @unittest.skip("Not Implemented")
    def test_get_workflow_outputs(self):
        """Test get_workflow_outputs()."""
        pass

    def test_get_workspace(self):
        """Test get_workspace()."""
        r = fapi.get_workspace(self.project, self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)
        space_dict = r.json()['workspace']
        self.assertEqual(space_dict['name'], self.workspace)
        self.assertEqual(space_dict['namespace'], self.project)

    def test_get_workspace_acl(self):
        """Test get_workspace_acl()."""
        r =  fapi.get_workspace_acl(self.project, self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    @unittest.skip("Not Implemented")
    def test_get_workspace_config(self):
        """Test get_workspace_config()."""
        pass

    def test_list_billing_projects(self):
        """Test list_billing_projects()."""
        r =  fapi.list_billing_projects()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_list_entity_types(self):
        """Test list_entity_types()."""
        r =  fapi.list_entity_types(self.project,
                                    self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_list_repository_configs(self):
        """Test list_repository_configs()."""
        r =  fapi.list_repository_configs()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_list_repository_methods(self):
        """Test list_repository_methods()."""
        r =  fapi.list_repository_methods()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_list_submissions(self):
        """Test list_submissions()."""
        r =  fapi.list_submissions(self.project,
                                   self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_list_workspace_configs(self):
        """Test list_workspace_configs()."""
        r =  fapi.list_workspace_configs(self.project,
                                         self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_list_workspaces(self):
        """Test list_workspaces()."""
        r = fapi.list_workspaces()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)
        workspace_names = [w['workspace']['name'] for w in r.json()]
        self.assertIn(self.workspace, workspace_names)

    def test_lock_workspace(self):
        """Test lock_workspace()"""
        r =  fapi.lock_workspace(self.project, self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 204)

        # Unlock, for other tests
        fapi.unlock_workspace(self.project, self.workspace)

    def test_health(self):
        """Test health()."""
        r =  fapi.health()
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    @unittest.skip("Not Implemented")
    def test_overwrite_workspace_config(self):
        """Test overwrite_workspace_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_remove_user_from_group(self):
        """Test remove_user_from_group()."""
        pass

    @unittest.skip("Not Implemented")
    def test_rename_workspace_config(self):
        """Test rename_workspace_config()."""
        pass
    
    @unittest.skip("Not Implemented")
    def test_request_access_to_group(self):
        """Test request_access_to_group()."""
        pass

    def test_unlock_workspace(self):
        """Test unlock_workspace()."""
        r =  fapi.unlock_workspace(self.project, self.workspace)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 204)

    @unittest.skip("Not Implemented")
    def test_update_repository_config_acl(self):
        """Test update_repository_config_acl()."""
        pass

    @unittest.skip("Not Implemented")
    def test_update_repository_method(self):
        """Test update_repository_method()."""
        pass

    @unittest.skip("Not Implemented")
    def test_update_repository_method_acl(self):
        """Test update_repository_method_acl()."""
        pass

    def test_update_workspace_acl(self):
        """Test update_workspace_acl()."""
        updates = [{ "email": "abaumann.firecloud@gmail.com",
                     "accessLevel": "READER"}]
        r =  fapi.update_workspace_acl(self.project, self.workspace,
                                       updates)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    def test_update_workspace_attributes(self):
        """Test update_workspace_attributes()."""
        updates = [fapi._attr_set("key1", "value1")]
        r =  fapi.update_workspace_attributes(self.project,
                                              self.workspace, updates)
        print(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)

    @unittest.skip("Not Implemented")
    def test_update_workspace_config(self):
        """Test update_workspace_config()."""
        pass

    @unittest.skip("Not Implemented")
    def test_upload_entities(self):
        """Test upload_entities()."""
        pass

    @unittest.skip("Not Implemented")
    def test_upload_entities_tsv(self):
        """Test upload_entities_tsv()."""
        pass

    @unittest.skip("Not Implemented")
    def test_validate_config(self):
        """Test validate_config()."""
        pass

def main():
    nose.main()

if __name__ == '__main__':
    main()
