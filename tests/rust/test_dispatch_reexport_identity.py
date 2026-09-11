"""Companion-layer re-exports of Rust dispatch/sim/signer FFI symbols.

The three-layer architecture (ADR-005) requires that driver code import
dispatch / simulation / signer symbols from the companion package
``degenbot.dispatch`` — not from the PyO3 wrapper ``degenbot_rs``. The
``Py*`` prefix and ``*_py`` suffix on the FFI names are the seam naming
itself; those should never appear in driver code.

These re-exports must be **direct aliases** (``from degenbot._ffi import
PyX as X``), not Python wrappers/subclasses: the Rust engine constructs and
consumes these pyclasses / pyfunctions directly, and driver code passes the
instances to Rust. A wrapper class would break type identity at the Rust
FFI boundary (the Rust side expects the exact ``PyX`` pyclass). The
companion's only job here is to give the symbols stable, seam-agnostic
names so driver code does not import ``degenbot_rs``.
"""

from __future__ import annotations

import degenbot.dispatch as d
from degenbot._ffi.simulation import (
    DispatchCandidate,
    DispatchOutcome,
    PayloadOutcome,
    PayloadVerdict,
    SimulateContext,
    dispatch_profitable_py,
    merge_payload_results_py,
)
from degenbot._ffi.submission import (
    Dispatcher,
    TxSigner,
    dispatch_and_submit_py,
    fetch_fee_history_py,
)


def test_dispatch_candidate_is_identity_alias() -> None:
    """``degenbot.dispatch.DispatchCandidate`` is the Rust pyclass itself."""
    assert d.DispatchCandidate is DispatchCandidate


def test_dispatch_outcome_is_identity_alias() -> None:
    """``degenbot.dispatch.DispatchOutcome`` is the Rust pyclass itself."""
    assert d.DispatchOutcome is DispatchOutcome


def test_dispatcher_is_identity_alias() -> None:
    """``degenbot.dispatch.Dispatcher`` is the Rust pyclass itself."""
    assert d.Dispatcher is Dispatcher


def test_simulate_context_is_identity_alias() -> None:
    """``degenbot.dispatch.SimulateContext`` is the Rust pyclass itself."""
    assert d.SimulateContext is SimulateContext


def test_tx_signer_is_identity_alias() -> None:
    """``degenbot.dispatch.TxSigner`` is the Rust pyclass itself."""
    assert d.TxSigner is TxSigner


def test_dispatch_and_submit_is_identity_alias() -> None:
    """``degenbot.dispatch.dispatch_and_submit`` is the Rust pyfunction."""
    assert d.dispatch_and_submit is dispatch_and_submit_py


def test_dispatch_profitable_is_identity_alias() -> None:
    """``degenbot.dispatch.dispatch_profitable`` is the Rust pyfunction."""
    assert d.dispatch_profitable is dispatch_profitable_py


def test_fetch_fee_history_is_identity_alias() -> None:
    """``degenbot.dispatch.fetch_fee_history`` is the Rust pyfunction."""
    assert d.fetch_fee_history is fetch_fee_history_py


def test_merge_payload_results_is_identity_alias() -> None:
    """``degenbot.dispatch.merge_payload_results`` is the Rust pyfunction
    (NUUJFA: the inline-sim payload arm routes through the same seam the
    FFI batch join uses)."""
    assert d.merge_payload_results is merge_payload_results_py


def test_payload_outcome_is_identity_alias() -> None:
    """``degenbot.dispatch.PayloadOutcome`` is the Rust pyclass itself."""
    assert d.PayloadOutcome is PayloadOutcome


def test_payload_verdict_is_identity_alias() -> None:
    """``degenbot.dispatch.PayloadVerdict`` is the Rust pyclass itself."""
    assert d.PayloadVerdict is PayloadVerdict


def test_all_symbols_reachable_from_package() -> None:
    """Every stable name is exported from ``degenbot.dispatch``.

    ``SubmitCandidate`` joined the surface with the inline-sim seam
    (SIMPIPE2 T3): the runner builds submit records from payload batches,
    so it is a public name alongside the FFI leaf wrappers. NUUJFA added
    the payload seam (``merge_payload_results`` + the two pyclasses) when
    the payload arm started routing through the same Rust sim join.
    """
    expected = {
        "DispatchCandidate",
        "DispatchOutcome",
        "Dispatcher",
        "PayloadOutcome",
        "PayloadVerdict",
        "SimulateContext",
        "SubmitCandidate",
        "TxSigner",
        "dispatch_and_submit",
        "dispatch_profitable",
        "fetch_fee_history",
        "merge_payload_results",
    }
    assert expected.issubset(set(dir(d)))
    assert expected == set(d.__all__)
