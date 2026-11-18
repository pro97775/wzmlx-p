import importlib.util
import unittest
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "bot"
    / "helper"
    / "ext_utils"
    / "file_types.py"
)
SPEC = importlib.util.spec_from_file_location("file_types", MODULE_PATH)
file_types = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(file_types)


class FileTypeTests(unittest.TestCase):
    def test_file_type_categories(self):
        self.assertEqual(file_types.get_file_type("Show/S01E01.MKV"), "video")
        self.assertEqual(file_types.get_file_type("track.flac"), "audio")
        self.assertEqual(file_types.get_file_type("book.pdf"), "document")
        self.assertEqual(file_types.get_file_type("release.nfo"), "other")

    def test_extension_filter_is_case_insensitive(self):
        self.assertFalse(
            file_types.is_extension_allowed("movie.mkv.!qB", None, ["aria2", "!qB"])
        )
        self.assertTrue(
            file_types.is_extension_allowed("movie.mkv", None, ["aria2", "!qB"])
        )

    def test_normalize_types_keeps_known_keys(self):
        self.assertEqual(
            file_types.normalize_types("video, AUDIO unknown"), ["video", "audio"]
        )

    def test_file_type_filter(self):
        self.assertTrue(file_types.is_file_type_allowed("movie.mkv", ["video"]))
        self.assertFalse(file_types.is_file_type_allowed("notes.txt", ["video"]))
        self.assertTrue(file_types.is_file_type_allowed("notes.txt", []))


class PlanBatchTests(unittest.TestCase):
    files = [{"size": 10}, {"size": 20}, {"size": 30}]

    def test_count_limit(self):
        self.assertEqual(file_types.plan_batch(self.files, 2, 0), self.files[:2])

    def test_size_limit(self):
        self.assertEqual(file_types.plan_batch(self.files, 0, 30), self.files[:2])

    def test_first_file_always_included(self):
        self.assertEqual(file_types.plan_batch(self.files, 0, 5), self.files[:1])

    def test_no_limits_returns_all(self):
        self.assertEqual(file_types.plan_batch(self.files), self.files)


class UploadedFileLedgerTests(unittest.TestCase):
    files = [{"index": 0, "size": 10}, {"index": 1, "size": 20}]

    def key(self, file_info):
        return file_types.get_batch_file_key(
            42, "qBittorrent", "abcdef", file_info["index"]
        )

    def test_key_is_per_user_file_and_engine(self):
        self.assertEqual(
            file_types.get_batch_file_key(1, "qBittorrent", "abc", 2),
            "1:qBittorrent:abc:2",
        )
        self.assertNotEqual(
            file_types.get_batch_file_key(1, "qBittorrent", "abc", 2),
            file_types.get_batch_file_key(2, "qBittorrent", "abc", 2),
        )
        self.assertNotEqual(
            file_types.get_batch_file_key(1, "Aria2", "abc", 2),
            file_types.get_batch_file_key(1, "qBittorrent", "abc", 2),
        )

    def test_filter_uploaded_files(self):
        uploaded = {self.key(self.files[0])}
        remaining = file_types.filter_uploaded_files(
            self.files, uploaded, self.key
        )
        self.assertEqual(remaining, [self.files[1]])

    def test_no_uploaded_files_keeps_all(self):
        self.assertEqual(
            file_types.filter_uploaded_files(self.files, set(), self.key),
            self.files,
        )


if __name__ == "__main__":
    unittest.main()
