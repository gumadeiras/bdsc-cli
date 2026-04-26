# RELEASE.md

`bdsc-cli` release flow.

## Preconditions

- `src/bdsc_cli/__init__.py` has the target `__version__`
- PyPI trusted publishing configured for this repo
- GitHub token can create releases
- Homebrew tap repo ready for formula update

## Local Preflight

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .[release]
python -m unittest discover -s tests
python -m build
python -m twine check dist/*
python scripts/render_homebrew_formula.py dist/bdsc_cli-$(python - <<'PY'
from bdsc_cli import __version__
print(__version__)
PY
).tar.gz
```

## Publish

1. Commit release-ready changes.
2. Push `main`.
3. Create and push a tag like `vX.Y.Z`.

```bash
git tag vX.Y.Z
git push origin vX.Y.Z
```

## What Happens

- GitHub Actions builds sdist + wheel
- `twine check` validates package metadata
- GitHub release is created with built artifacts attached
- PyPI publish runs from the same artifacts

## Homebrew

After the GitHub release exists:

```bash
curl -L -o /tmp/bdsc_cli-X.Y.Z.tar.gz \
  https://github.com/gumadeiras/bdsc-cli/releases/download/vX.Y.Z/bdsc_cli-X.Y.Z.tar.gz
python scripts/render_homebrew_formula.py /tmp/bdsc_cli-X.Y.Z.tar.gz --output /tmp/bdsc-cli.rb
```

Then commit the rendered formula into the tap repo with the matching release
version and sha256.

## PyPI Trusted Publisher

If the PyPI publish job fails with `invalid-publisher`, add a trusted
publisher on PyPI with these claims:

- owner/repo: `gumadeiras/bdsc-cli`
- workflow: `.github/workflows/release.yml`
- environment: `pypi`
