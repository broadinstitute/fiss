# -*- coding: utf-8 -*-
"""Unit tests for functions used in mop"""

import unittest
from datetime import datetime

from firecloud.fiss import get_files_to_keep


def make_file_metadata(file_path, md5, size, time_created):
    file_name = file_path.split("/")[-1]
    return {
        "file_name": file_name,
        "file_path": file_path,
        "size": size,
        "md5": md5,
        "time_created": time_created,
        "unique_key": f"{file_name}.{md5}.{size}"
    }


class TestMop(unittest.TestCase):

    def test_get_files_to_keep_one_in_data_model(self):
        test_bucket = "gs://bucket/"

        # make one file
        file_path_1 = f"{test_bucket}file/file1"
        md5_1 = "abcde"
        size_1 = "10"
        time_created_1 = datetime(2020, 10, 10)
        file_1_metadata = make_file_metadata(file_path_1, md5_1, size_1, time_created_1)

        # make a duplicate in another directory
        file_path_2 = f"{test_bucket}file/another/path/file1"
        md5_2 = "abcde"
        size_2 = "10"
        time_created_2 = datetime(2020, 10, 10)
        file_2_metadata = make_file_metadata(file_path_2, md5_2, size_2, time_created_2)

        # make sure we did things right
        self.assertEqual(file_1_metadata['unique_key'], file_2_metadata['unique_key'])

        # only the first file is referenced in the data table
        referenced_files = [file_path_1]

        test_bucket_dict = {
            file_path_1: file_1_metadata,
            file_path_2: file_2_metadata
        }

        files_to_keep, updated_bucket_dict = get_files_to_keep(test_bucket_dict, referenced_files)

        # files_to_keep for the unique_key should be file_path_1
        unique_key_1 = file_1_metadata['unique_key']
        self.assertEqual(files_to_keep[unique_key_1], [file_1_metadata])

        # first file should be marked as being in the data table, second is not
        self.assertTrue(updated_bucket_dict[file_path_1]['is_in_data_table'])
        self.assertFalse(updated_bucket_dict[file_path_2]['is_in_data_table'])


if __name__ == '__main__':
    unittest.main()

