import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from dig_open_data import cli


class TestCliList(unittest.TestCase):
    def test_list_plain(self):
        with mock.patch(
            "dig_open_data.cli.list_dataset_files",
            return_value=["bottom-line/Mixed/a.tsv.gz", "bottom-line/EA/b.tsv.gz"],
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["list"])
        self.assertEqual(code, 0)
        self.assertEqual(
            out.getvalue().strip().splitlines(),
            ["bottom-line/Mixed/a.tsv.gz", "bottom-line/EA/b.tsv.gz"],
        )

    def test_list_json(self):
        with mock.patch(
            "dig_open_data.cli.list_dataset_files",
            return_value=["bottom-line/Mixed/a.tsv.gz"],
        ):
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["list", "--json"])
        self.assertEqual(code, 0)
        self.assertIn("\"bottom-line/Mixed/a.tsv.gz\"", out.getvalue())

    def test_list_limit(self):
        with mock.patch(
            "dig_open_data.cli.list_dataset_files",
            return_value=["bottom-line/Mixed/a.tsv.gz"],
        ) as mocked:
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["list", "--limit", "2"])
        self.assertEqual(code, 0)
        self.assertEqual(
            out.getvalue().strip().splitlines(),
            ["bottom-line/Mixed/a.tsv.gz"],
        )
        mocked.assert_called_with(
            bucket=cli.DEFAULT_BUCKET,
            prefix=cli.DEFAULT_PREFIX,
            ancestry=None,
            limit=2,
            max_keys=1000,
            contains=None,
        )

    def test_list_with_ancestry(self):
        entries = [
            mock.Mock(ancestry="Mixed", key="bottom-line/Mixed/a.tsv.gz"),
            mock.Mock(ancestry="EA", key="bottom-line/EA/b.tsv.gz"),
        ]
        with mock.patch("dig_open_data.cli.list_files_with_metadata", return_value=entries) as mocked:
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["list", "--with-ancestry"])
        self.assertEqual(code, 0)
        self.assertEqual(
            out.getvalue().strip().splitlines(),
            ["Mixed\tbottom-line/Mixed/a.tsv.gz", "EA\tbottom-line/EA/b.tsv.gz"],
        )
        mocked.assert_called_with(
            bucket=cli.DEFAULT_BUCKET,
            prefix=cli.DEFAULT_PREFIX,
            limit=None,
            ancestry=None,
            max_keys=1000,
            contains=None,
        )

    def test_list_contains(self):
        with mock.patch(
            "dig_open_data.cli.list_dataset_files",
            return_value=["bottom-line/EU/AlbInT2D.sumstats.tsv.gz"],
        ) as mocked:
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["list", "--contains", "T2D"])
        self.assertEqual(code, 0)
        self.assertEqual(
            out.getvalue().strip().splitlines(),
            ["bottom-line/EU/AlbInT2D.sumstats.tsv.gz"],
        )
        mocked.assert_called_with(
            bucket=cli.DEFAULT_BUCKET,
            prefix=cli.DEFAULT_PREFIX,
            ancestry=None,
            limit=None,
            max_keys=1000,
            contains="T2D",
        )


class TestCliAncestries(unittest.TestCase):
    def test_ancestries_plain(self):
        with mock.patch("dig_open_data.cli.list_ancestries", return_value=["AFR", "EUR"]) as mocked:
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["ancestries"])
        self.assertEqual(code, 0)
        self.assertEqual(out.getvalue().strip().splitlines(), ["AFR", "EUR"])
        mocked.assert_called_with(
            bucket=cli.DEFAULT_BUCKET, prefix=cli.DEFAULT_PREFIX, max_keys=1000
        )

    def test_ancestries_json(self):
        with mock.patch("dig_open_data.cli.list_ancestries", return_value=["AFR"]):
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["ancestries", "--json"])
        self.assertEqual(code, 0)
        self.assertIn("\"AFR\"", out.getvalue())


class TestCliDocs(unittest.TestCase):
    def test_docs_plain(self):
        docs = {"dataset1/README.md": "hello"}
        with mock.patch("dig_open_data.cli.get_documentation", return_value=docs):
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["docs", "dataset1/"])
        self.assertEqual(code, 0)
        self.assertIn("dataset1/README.md", out.getvalue())
        self.assertIn("hello", out.getvalue())

    def test_docs_none(self):
        with mock.patch("dig_open_data.cli.get_documentation", return_value={}):
            out = io.StringIO()
            err = io.StringIO()
            with redirect_stdout(out), redirect_stderr(err):
                code = cli.main(["docs", "dataset1/"])
        self.assertEqual(code, 1)
        self.assertIn("No documentation files found", err.getvalue())

    def test_docs_json(self):
        docs = {"dataset1/README.md": "hello"}
        with mock.patch("dig_open_data.cli.get_documentation", return_value=docs):
            out = io.StringIO()
            with redirect_stdout(out):
                code = cli.main(["docs", "dataset1/", "--json"])
        self.assertEqual(code, 0)
        self.assertIn("\"dataset1/README.md\"", out.getvalue())


if __name__ == "__main__":
    unittest.main()
