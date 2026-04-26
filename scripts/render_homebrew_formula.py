from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

FORMULA_TEMPLATE = """class BdscCli < Formula
  include Language::Python::Virtualenv

  desc "Sync and query BDSC datasets locally"
  homepage "https://github.com/gumadeiras/bdsc-cli"
  url "https://github.com/gumadeiras/bdsc-cli/releases/download/v{version}/bdsc_cli-{version}.tar.gz"
  sha256 "{sha256}"
  license "MIT"

  depends_on "python@3.13"

  def install
    virtualenv_install_with_resources
  end

  test do
    assert_match version.to_s, shell_output("#{{bin}}/bdsc --version")
  end
end
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render a Homebrew formula from a built bdsc-cli sdist."
    )
    parser.add_argument("sdist", help="path to bdsc_cli-<version>.tar.gz")
    parser.add_argument(
        "--output",
        default="-",
        help="formula output path; default stdout",
    )
    return parser.parse_args()


def version_from_sdist(path: Path) -> str:
    suffix = ".tar.gz"
    if not path.name.startswith("bdsc_cli-") or not path.name.endswith(suffix):
        raise ValueError(f"unexpected sdist name: {path.name}")
    return path.name.removeprefix("bdsc_cli-").removesuffix(suffix)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def render_formula(sdist: Path) -> str:
    version = version_from_sdist(sdist)
    return FORMULA_TEMPLATE.format(version=version, sha256=sha256_file(sdist))


def main() -> int:
    args = parse_args()
    sdist = Path(args.sdist).expanduser().resolve()
    formula = render_formula(sdist)
    if args.output == "-":
        print(formula, end="")
        return 0

    output_path = Path(args.output).expanduser()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(formula, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
