import io
import unittest
from unittest import mock

from dig_open_data.catalog import (
    FileEntry,
    ListObjectsResult,
    _parse_list_objects,
    get_documentation,
    list_dataset_files,
    list_files_with_metadata,
)


class TestParseListObjects(unittest.TestCase):
    def test_parse_list_objects(self):
        xml = b"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
  <IsTruncated>false</IsTruncated>
  <Contents><Key>dataset1/file1.tsv.gz</Key></Contents>
  <Contents><Key>dataset1/README.md</Key></Contents>
  <CommonPrefixes><Prefix>dataset1/</Prefix></CommonPrefixes>
  <CommonPrefixes><Prefix>dataset2/</Prefix></CommonPrefixes>
</ListBucketResult>
"""
        result = _parse_list_objects(xml)
        self.assertFalse(result.is_truncated)
        self.assertEqual(result.next_token, None)
        self.assertEqual(
            result.keys,
            ["dataset1/file1.tsv.gz", "dataset1/README.md"],
        )
        self.assertEqual(result.common_prefixes, ["dataset1/", "dataset2/"])


class TestGetDocumentation(unittest.TestCase):
    def test_get_documentation(self):
        fake_keys = [
            "dataset1/README.md",
            "dataset1/data.tsv.gz",
            "dataset1/notes.txt",
        ]
        fake_result = ListObjectsResult(
            keys=fake_keys,
            common_prefixes=[],
            is_truncated=False,
            next_token=None,
        )
        with mock.patch("dig_open_data.catalog.list_all_objects", return_value=fake_result):
            with mock.patch("dig_open_data.catalog.open_text", return_value=io.StringIO("doc")):
                docs = get_documentation("dataset1/")

        self.assertEqual(docs, {"dataset1/README.md": "doc"})


class TestListDatasetFiles(unittest.TestCase):
    def test_list_dataset_files(self):
        fake_result = ListObjectsResult(
            keys=["bottom-line/Mixed/a.tsv.gz", "bottom-line/Mixed/b.tsv.gz"],
            common_prefixes=[],
            is_truncated=False,
            next_token=None,
        )
        with mock.patch("dig_open_data.catalog.list_all_objects", return_value=fake_result):
            files = list_dataset_files(prefix="bottom-line/")
        self.assertEqual(
            files,
            ["bottom-line/Mixed/a.tsv.gz", "bottom-line/Mixed/b.tsv.gz"],
        )

    def test_list_dataset_files_contains(self):
        fake_result = ListObjectsResult(
            keys=[
                "bottom-line/EU/AlbInT2D.sumstats.tsv.gz",
                "bottom-line/EU/CAD.sumstats.tsv.gz",
            ],
            common_prefixes=[],
            is_truncated=False,
            next_token=None,
        )
        with mock.patch("dig_open_data.catalog.list_all_objects", return_value=fake_result):
            files = list_dataset_files(prefix="bottom-line/EU/", contains="T2D")
        self.assertEqual(files, ["bottom-line/EU/AlbInT2D.sumstats.tsv.gz"])


class TestListFilesWithMetadata(unittest.TestCase):
    def test_list_files_with_metadata(self):
        fake_result = ListObjectsResult(
            keys=[
                "bottom-line/Mixed/a.tsv.gz",
                "bottom-line/EA/b.tsv.gz",
            ],
            common_prefixes=[],
            is_truncated=False,
            next_token=None,
        )
        with mock.patch("dig_open_data.catalog.list_all_objects", return_value=fake_result):
            entries = list_files_with_metadata()
        self.assertEqual(
            entries,
            [
                FileEntry(ancestry="EA", key="bottom-line/EA/b.tsv.gz"),
                FileEntry(ancestry="Mixed", key="bottom-line/Mixed/a.tsv.gz"),
            ],
        )

    def test_list_files_with_metadata_filter(self):
        fake_result = ListObjectsResult(
            keys=["bottom-line/Mixed/a.tsv.gz"],
            common_prefixes=[],
            is_truncated=False,
            next_token=None,
        )
        with mock.patch("dig_open_data.catalog.list_all_objects", return_value=fake_result):
            entries = list_files_with_metadata(ancestry="Mixed")
        self.assertEqual(
            entries,
            [FileEntry(ancestry="Mixed", key="bottom-line/Mixed/a.tsv.gz")],
        )

    def test_list_files_with_metadata_contains(self):
        fake_result = ListObjectsResult(
            keys=[
                "bottom-line/EU/AlbInT2D.sumstats.tsv.gz",
                "bottom-line/EU/CAD.sumstats.tsv.gz",
            ],
            common_prefixes=[],
            is_truncated=False,
            next_token=None,
        )
        with mock.patch("dig_open_data.catalog.list_all_objects", return_value=fake_result):
            entries = list_files_with_metadata(prefix="bottom-line/EU/", contains="T2D")
        self.assertEqual(
            entries,
            [FileEntry(ancestry="EU", key="bottom-line/EU/AlbInT2D.sumstats.tsv.gz")],
        )


if __name__ == "__main__":
    unittest.main()
