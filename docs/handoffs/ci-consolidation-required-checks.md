# Handoff: CI consolidation — required-check re-registration

Epic RGNN2D (CI renovation) merged and renamed the checks in
`.github/workflows/ci.yml`. GitHub branch protection keys required checks by
job NAME, so the required-check list must be re-registered before PR gating
uses the new names.

## Old -> New mapping

| Removed check | Where its substance lives now |
|---|---|
| Commit Lint | Unchanged (`commitlint` job) |
| Markdown Lint | Unchanged (`markdownlint` job) |
| Rust Lint | Step of the consolidated **Rust (lint, tests, F2 golden replay, build, publish dry-run)** job |
| Rust Tests | Same `rust` job |
| Solver Golden Replay (F2 gate) | Same `rust` job — make THIS job the merge-blocking check |
| Rust Build | Same `rust` job |
| Publish dry-run (crates.io gate) | Same `rust` job (path gating moved to the `Detect Changes` job) |
| Python Lint | Step of the `Python 3.12` matrix leg |
| Python Tests (3.12) | `Python 3.12` |
| Python Tests (3.13) | Dropped — matrix slimmed to 3.12 + 3.14 (min/max) |
| Python Tests (3.14) | `Python 3.14` |
| Python Tests (standalone anvil) | `Python 3.12` (Foundry installed on the floor leg) |
| — (new) | **Detect Changes** (path filter; also gates all of the above) |
| Tier-3 On-Chain Accuracy Oracle | Unchanged name, now path-gated |

## Required-check set to register

1. `Rust (lint, tests, F2 golden replay, build, publish dry-run)` — the F2 golden gate comment in ci.yml points here.
2. `Python 3.12`
3. `Python 3.14`
4. (Optional) `Tier-3 On-Chain Accuracy Oracle` if it was previously required; its name is unchanged but its skip behavior is now path-dependent.

> Related: release publishing was later consolidated into `release.yml` —
> see [release-workflow-consolidation.md](release-workflow-consolidation.md).
>
> Note: a path-gated job that is skipped reports success to branch protection,
> so requiring it does NOT block docs-only PRs.

## Verification

- `./actionlint -color .github/workflows/ci.yml` passes (binary fetchable via
  `curl -sSf https://raw.githubusercontent.com/rhysd/actionlint/main/scripts/download-actionlint.bash | bash`).
- All command lines formerly run by the merged jobs are preserved verbatim,
  cheapest-first, in one runner-local sequence (fmt -> clippy -> test ->
  golden -> build -> dry-run). Gate semantics: identical.