# RELEASE.md

`bdsc` release flow.

## Preconditions

- `src/bdsc_cli/__init__.py` has the target `__version__`
- PyPI trusted publishing configured for this repo
- GitHub token can create releases
- `HOMEBREW_TAP_TOKEN` repository secret can write to
  `gumadeiras/homebrew-tap`

## Local Preflight

```bash
./scripts/release check X.Y.Z
```

## Publish

Use the wrapper after explicit release approval:

```bash
./scripts/release run X.Y.Z
```

The wrapper updates version files, runs the local preflight, commits, tags,
pushes `main`, pushes `vX.Y.Z`, and waits for release CI.

## What Happens

- GitHub Actions builds sdist + wheel
- `twine check` validates package metadata
- GitHub release is created with built artifacts attached
- PyPI publish runs from the same artifacts
- `gumadeiras/homebrew-tap` is updated from the release sdist artifact

## Changelog Rules

- Every release must update `CHANGELOG.md` before the release tag is created.
- `CHANGELOG.md` must always keep an `Unreleased` section at the top for future entries.
- New user-facing changes should be added to `Unreleased` as they land.
- Use user-facing language whenever possible. Describe what changed for people using bdsc, not repository maintenance.
- Use these sections when they apply: `Features`, `Fixes`, and `Changes`.
- Omit empty sections.
- Do not include release chores unless the change affects how users install or use bdsc.

## Homebrew

The `homebrew-tap` release job updates `Formula/bdsc.rb` automatically
from the same sdist artifact used for the GitHub release.

If the tap update fails or needs manual repair, use the published release asset,
not a freshly built local sdist:

```bash
curl -L -o /tmp/bdsc-X.Y.Z.tar.gz \
  https://github.com/gumadeiras/bdsc-cli/releases/download/vX.Y.Z/bdsc-X.Y.Z.tar.gz
sha256="$(shasum -a 256 /tmp/bdsc-X.Y.Z.tar.gz | awk '{print $1}')"
python ../homebrew-tap/scripts/update_formula.py \
  --formula ../homebrew-tap/Formula/bdsc.rb \
  --url https://github.com/gumadeiras/bdsc-cli/releases/download/vX.Y.Z/bdsc-X.Y.Z.tar.gz \
  --sha256 "$sha256"
```

Then commit the formula in `~/git/homebrew-tap` and push `main`.

## PyPI Trusted Publisher

If the PyPI publish job fails with `invalid-publisher`, add a trusted
publisher on PyPI with these claims:

- owner/repo: `gumadeiras/bdsc-cli`
- workflow: `.github/workflows/release.yml`
- environment: `pypi`
