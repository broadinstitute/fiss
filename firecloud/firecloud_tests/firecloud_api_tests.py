#! /usr/bin/env python

import unittest
import nose
from six import print_

import firecloud.api as fapi


class TestFirecloudAPI(unittest.TestCase):
    """Unit test the firecloud.api module.

    There should be at least one test per api call,
    with composite tests as feasible.
    """
    # Set up and tear down for API testing
    def setUp(self):
        """Set up test conditions."""
        self.test_workspace = "API_TEST_TEMP"
        self.test_namespace = "broad-firecloud-testing"

        self.static_workspace = "API_TEST_DONT_DELETE"

    def tearDown(self):
        """Tear down test conditions."""
        pass

    # Test individual api calls, 1 test per api call,
    # listed in alphabetical order for convenience
    @unittest.skip("Not Implemented")
    def test_abort_sumbission(self):
        """Test abort_sumbission()."""
        pass

    def test_clone_workspace(self):
        """Test clone_workspace()."""
        r =  fapi.clone_workspace(self.test_namespace, self.static_workspace,
                                    self.test_namespace,
                                    self.static_workspace + "_CLONE")
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 201)

        #TODO: Compare new workspace and old workspace
        ##Cleanup, Delete workspace
        r =  fapi.delete_workspace(self.test_namespace,
                                     self.static_workspace + "_CLONE")
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 202)

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
    def test_create_submission(self):
        """Test create_submission()."""
        pass

    def test_create_workspace(self):
        """Test create_workspace()."""
        r = fapi.create_workspace(self.test_namespace, self.test_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 201)

    @unittest.skip("Not Implemented")
    def test_create_workspace_config(self):
        """Test create_config()."""
        pass

    @unittest.skip("Not Implemented")
    def test_delete_entity(self):
        """Test delete_entity()."""
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

    def test_delete_workspace(self):
        """Test delete_workspace()."""
        # Try to create workspace, but ignore errors
        print_(fapi.create_workspace(self.test_namespace, self.test_workspace))

        r = fapi.delete_workspace(self.test_namespace, self.test_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 202)

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
        r =  fapi.get_entities(self.test_namespace,
                                 self.static_workspace,
                                 "participant")
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_get_entities_tsv(self):
        """Test get_entities_tsv()."""
        r =  fapi.get_entities_tsv(self.test_namespace,
                                     self.static_workspace,
                                     "participant")
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_get_entities_with_type(self):
        """Test get_entities_with_type()."""
        r =  fapi.get_entities_with_type(self.test_namespace,
                                           self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    @unittest.skip("Not Implemented")
    def test_get_entity(self):
        """Test get_entity()."""
        pass

    def test_get_entity_types(self):
        """Test get_entity_types()."""
        r =  fapi.get_entity_types(self.test_namespace,
                                     self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

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

    def test_get_status(self):
        """Test get_status()."""
        r =  fapi.get_status()
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

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
        r = fapi.get_workspace(self.test_namespace, self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)
        space_dict = r.json()['workspace']
        self.assertEqual(space_dict['name'], self.static_workspace)
        self.assertEqual(space_dict['namespace'], self.test_namespace)

    def test_get_workspace_acl(self):
        """Test get_workspace_acl()."""
        r =  fapi.get_workspace_acl(self.test_namespace, self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    @unittest.skip("Not Implemented")
    def test_get_workspace_config(self):
        """Test get_workspace_config()."""
        pass

    def test_list_billing_projects(self):
        """Test list_billing_projects()."""
        r =  fapi.list_billing_projects()
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_list_repository_configs(self):
        """Test list_repository_configs()."""
        r =  fapi.list_repository_configs()
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_list_repository_methods(self):
        """Test list_repository_methods()."""
        r =  fapi.list_repository_methods()
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_list_submissions(self):
        """Test list_submissions()."""
        r =  fapi.list_submissions(self.test_namespace,
                                    self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    @unittest.skip("Not Implemented")
    def test_list_workspace_configs(self):
        """Test get_configs()."""
        r =  fapi.get_configs(self.test_namespace,
                                self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_list_workspaces(self):
        """Test get_workspaces()."""
        r = fapi.get_workspaces()
        print_(r.status_code, r.content)
        self.assertEqual(r.status_code, 200)
        workspace_names = [w['workspace']['name'] for w in r.json()]
        self.assertIn(self.static_workspace, workspace_names)

    def test_lock_workspace(self):
        """Test lock_workspace()"""
        r =  fapi.lock_workspace(self.test_namespace, self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 204)

    def test_ping(self):
        """Test ping()."""
        r =  fapi.ping()
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    @unittest.skip("Not Implemented")
    def test_rename_workspace_config(self):
        """Test rename_workspace_config()."""
        pass

    def test_unlock_workspace(self):
        """Test unlock_workspace()."""
        r =  fapi.unlock_workspace(self.test_namespace, self.static_workspace)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 204)

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
        r =  fapi.update_workspace_acl(self.test_namespace, self.static_workspace,
                                         updates)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

    def test_update_workspace_attributes(self):
        """Test update_workspace_attributes()."""
        updates = [fapi._attr_up("key1", "value1")]
        r =  fapi.update_workspace_attributes(self.test_namespace,
                                                self.static_workspace, updates)
        print_(r.status_code, r.content)
        self.assertEqual(r.status, 200)

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
