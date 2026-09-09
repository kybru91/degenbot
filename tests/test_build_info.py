"""Build-identity tests for the Rust extension (stale-.so detector).

Background (AGENTS.md "Rebuilding the Rust `.so` after edits"): maturin/uv have
repeatedly served a stale cached artifact for `degenbot._ffi` after Rust edits
while reporting a successful rebuild. Every compile of `degenbot_rs` runs
`rust/crates/degenbot-python/build.rs`, which embeds a `<count, fingerprint>`
build identity (the counter advancing only when the crate's source content
changes). Any installed extension whose fingerprint differs from the repo
receipt predates the latest build.
"""

import pytest

from degenbot.build_info import installed_build_number, read_receipt, verify_build_fresh


def test_build_number_is_positive() -> None:
    # build.rs runs on EVERY compile of degenbot_rs (including this test
    # build), so the installed extension must always carry a number >= 1.
    # 0 is the no-build.rs fallback and means the tagging broke.
    assert installed_build_number() >= 1


def test_installed_extension_is_fresh() -> None:
    # The primary gate: fails whenever the installed .so was built from
    # different sources than the latest recorded build (the cached-artifact
    # failure mode). Skips where there is no repo receipt to compare against
    # (non-editable/published install, fresh clone before a first build).
    if read_receipt() is None:
        pytest.skip("no .build-number receipt (non-editable install)")
    verify_build_fresh()
