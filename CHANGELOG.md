# Changelog

## Unreleased

### Changes

- Added the final `bdsc-cli` release notice. Install `bdsc` now.
- Updated release automation to the current GitHub Actions checkout, Python setup, and release actions.
- Added a local release wrapper for version sync, package validation, tagging, and release workflow verification.

## 0.2.2 - 2026-05-13

### Fixes

- Fixed query limits and CLI argument help.

### Changes

- Documented PyPI installation.

## 0.2.1 - 2026-05-05

### Fixes

- Fixed construct fragment search.

### Changes

- Documented release install paths and PyPI trusted publisher setup.

## 0.2.0 - 2026-04-25

Initial release.

### Features

- Added local sync for public BDSC CSV datasets and a local SQLite index.
- Added agent-friendly stock, gene, component, property, relationship, and status queries.
- Added auto-detection and batch lookup flows.
- Added exact component, identifier, property, and driver-family queries.
- Added compound dataset filters, fuzzy local search ranking, and typo-tolerant direct lookups.
- Added normalized export commands and filtered export queries.
- Added property and relationship term discovery.
- Added canned query reports.
- Added release automation and packaging helpers.

### Changes

- Centered query UX on `find` and aligned public help and usage guidance.
