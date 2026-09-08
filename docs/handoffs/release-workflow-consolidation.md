# Handoff: release publishing merged into `release.yml`

The former `publish-to-pypi.yaml` and `publish-to-crates-io.yml` are merged into
[`.github/workflows/release.yml`](../../.github/workflows/release.yml) with an
ordered `needs` chain so a tag can no longer split a release across registries.

## Ordering (user-approved)

1. `validate-branch` (tag is on main)
2. `build-wheels` + `build-sdist` (parallel, verbatim maturin-action jobs)
3. `publish-crates-io` — failure-prone leg runs FIRST: new-crate rate limits /
   lockstep-check failures leave NOTHING published, and a re-tag is cheap.
4. `publish-to-pypi` — needs the crates leg; a crates failure skips the PyPI push.
5. `github-release` — needs both pushes; the "release completed cleanly" signal.

`test-*` tags publish only to Test PyPI (crates.io policy-forbidden), unchanged.
A same-tag failure after a successful leg is accepted and recoverable: crates.io
is fixed and re-tagged before PyPI; PyPI retries are safe and tag-driven.

## REQUIRED operator actions (one-time)

- **PyPI trusted publishing is keyed to the workflow FILENAME.** The publish jobs
  moved from `publish-to-pypi.yaml` to `release.yml`, so the existing trusted-
  publisher entries are dead. Update BOTH entries (pypi production AND testpypi)
  to workflow filename `release.yml` — repo, environment names (`pypi`,
  `testpypi`) and job names are unchanged. Do this BEFORE the next release tag.
- crates.io auth uses the repo secret `CRATES_IO_TOKEN` (unchanged by the move).

## Verification

- `actionlint .github/workflows/release.yml` passes.
- All distinctive job bodies (maturin builds with sccache, tag/version lockstep
  check, verified dry-run, retry-after publish loop, OIDC publishes, sigstore
  signing, `gh release create`) are present verbatim.
- `pyproject.toml` comments updated to the new filename; historical release docs
  are intentionally left referring to the old filename.