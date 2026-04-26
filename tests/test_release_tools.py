from __future__ import annotations

import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path


SPEC = importlib.util.spec_from_file_location(
    "render_homebrew_formula",
    Path(__file__).resolve().parents[1] / "scripts" / "render_homebrew_formula.py",
)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ReleaseToolTests(unittest.TestCase):
    def test_render_formula_uses_sdist_version_and_sha(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sdist = Path(temp_dir) / "bdsc_cli-1.2.3.tar.gz"
            sdist.write_bytes(b"bdsc-cli-test")
            formula = MODULE.render_formula(sdist)

        self.assertIn('url "https://github.com/gumadeiras/bdsc-cli/releases/download/v1.2.3/bdsc_cli-1.2.3.tar.gz"', formula)
        self.assertIn(hashlib.sha256(b"bdsc-cli-test").hexdigest(), formula)

    def test_version_from_sdist_rejects_unexpected_name(self) -> None:
        with self.assertRaises(ValueError):
            MODULE.version_from_sdist(Path("not-bdsc.tar.gz"))


if __name__ == "__main__":
    unittest.main()
