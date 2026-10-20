import tempfile
import unittest
from pathlib import Path

from scripts.find_duckdb_library import engine_version, find_library


class FindDuckDBLibraryTest(unittest.TestCase):
    def test_decodes_locked_engine_version(self):
        self.assertEqual(engine_version("1.10506.0"), "1.5.6")

    def test_selects_exact_locked_version_when_cache_has_older_library(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            lockfile = root / "Cargo.lock"
            lockfile.write_text(
                'version = 3\n\n[[package]]\nname = "duckdb"\nversion = "1.10506.0"\n',
                encoding="utf-8",
            )
            older = root / "libs" / "aarch64-apple-darwin" / "1.5.5" / "libduckdb.dylib"
            expected = root / "libs" / "aarch64-apple-darwin" / "1.5.6" / "libduckdb.dylib"
            older.parent.mkdir(parents=True)
            older.touch()
            expected.parent.mkdir(parents=True)
            expected.touch()

            selected = find_library(lockfile, root / "libs", "aarch64-apple-darwin")

            self.assertEqual(selected, expected)


if __name__ == "__main__":
    unittest.main()
