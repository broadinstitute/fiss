# -*- coding: utf-8 -*-
"""Unit tests for functions used in mop"""

import unittest
from datetime import datetime

import pprint

from firecloud.fiss import get_files_to_keep


def make_file_metadata(file_path, md5, size, time_created, is_in_data_table):
    file_name = file_path.split("/")[-1]
    return {
        "file_name": file_name,
        "file_path": file_path,
        "size": size,
        "md5": md5,
        "time_created": time_created,
        "time_updated": time_created,
        "unique_key": f"{file_name}.{md5}.{size}",
        "is_in_data_table": is_in_data_table
    }


class TestMop(unittest.TestCase):

    def test_get_files_to_keep(self):
        test_bucket = "gs://bucket/"

        # file A that is duplicated, this one is older, is in data table
        file_path_1 = f"{test_bucket}folder1/A"
        md5_1 = "abc"
        size_1 = "10"
        time_created_1 = datetime(2020, 10, 10)
        file_1_metadata = make_file_metadata(file_path_1, md5_1, size_1, time_created_1, True)

        # file A that is duplicated, newer, but not in data table
        file_path_2 = f"{test_bucket}folder2/A"
        md5_2 = "abc"
        size_2 = "10"
        time_created_2 = datetime(2020, 10, 15)
        file_2_metadata = make_file_metadata(file_path_2, md5_2, size_2, time_created_2, False)

        # make sure those are duplicates
        self.assertEqual(file_1_metadata['unique_key'], file_2_metadata['unique_key'])

        # file B that's not duplicated, is not in data table
        file_path_3 = f"{test_bucket}folder1/B"
        md5_3 = "xyz"
        size_3 = "10"
        time_created_3 = datetime(2020, 10, 10)
        file_3_metadata = make_file_metadata(file_path_3, md5_3, size_3, time_created_3, False)

        # file C that is duplicated, this one is older, not in data table
        file_path_4 = f"{test_bucket}folder1/C"
        md5_4 = "tuv"
        size_4 = "10"
        time_created_4 = datetime(2020, 10, 10)
        file_4_metadata = make_file_metadata(file_path_4, md5_4, size_4, time_created_4, False)

        # file C that is duplicated, newer, not in data table
        file_path_5 = f"{test_bucket}folder2/C"
        md5_5 = "tuv"
        size_5 = "10"
        time_created_5 = datetime(2020, 10, 15)
        file_5_metadata = make_file_metadata(file_path_5, md5_5, size_5, time_created_5, False)

        # make sure those are duplicates
        self.assertEqual(file_4_metadata['unique_key'], file_5_metadata['unique_key'])

        test_bucket_dict = {
            file_path_1: file_1_metadata,
            file_path_2: file_2_metadata,
            file_path_3: file_3_metadata,
            file_path_4: file_4_metadata,
            file_path_5: file_5_metadata
        }

        files_to_keep = get_files_to_keep(test_bucket_dict)

        print('unique keys')
        print(file_1_metadata['unique_key'])
        print(file_2_metadata['unique_key'])
        print(file_3_metadata['unique_key'])
        print(file_4_metadata['unique_key'])
        print(file_5_metadata['unique_key'])

        print('files to keep')
        pprint.pprint(files_to_keep)

        # there should be x unique keys in the files_to_keep dict
        self.assertEqual(len(files_to_keep), 3)

        # file_path_1, file_path_3, and file_path_5 should be the keepers
        self.assertTrue(file_path_1 in files_to_keep)
        self.assertTrue(file_path_3 in files_to_keep)
        self.assertTrue(file_path_5 in files_to_keep)

        # file_path_2 and file_path_2 should not be keepers
        self.assertFalse(file_path_2 in files_to_keep)
        self.assertFalse(file_path_4 in files_to_keep)


if __name__ == '__main__':
    unittest.main()

