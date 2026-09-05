"""SIMPIPE2 T3 acceptance — payload entries degrade to render+submit-only.

The engine's inline-sim payloads (``DEGENBOT_SOLVE_INLINE_SIM`` stance) ride
``batch['payloads']`` into the driver. The contract:

- per-entry presence decides: payload entries NEVER enter the FFI sim batch;
- a payload success yields a ``SubmitCandidate`` built straight from the
  primitive payload (net/gas/calldata/access-list parity with the FFI join);
- a payload ``failure`` surfaces as a ``[sim-fail]`` record through the
  merged outcome's ``failures`` (revert detail carried through);
- a mixed batch keeps the legacy FFI path for the payload-less entries.

The Rust ``SubmitCandidate`` pyclass is real (import-time); the engine seam
(``payload_path_info``) is faked at the session boundary — no live RPC.
"""

from __future__ import annotations

import types
from typing import TYPE_CHECKING

from degenbot.runner._dispatch import (
    _inline_failure_record,
    _merge_payload_outcome,
)

if TYPE_CHECKING:
    import pytest


class _FakeEngine:
    def payload_path_info(self, pid: int):
        """Render-shape path_info for the payload render parity."""
        if pid == 7:  # unknown path -> None (renderers degrade to '?')
            return None
        return {
            "path_type": "V2-V4",
            "hops": [
                {"family": "V2", "pool_address": "0xV2ADDR"},
                {"family": "V4", "pool_id_hex": "0xV4ID"},
            ],
        }


class _FakeSession:
    def __init__(self) -> None:
        self.dispatcher = type("D", (), {"current_block": 42})()
        self.sim_ctx = types.SimpleNamespace(
            executor_address="0x690B9A9E9aa1C9dB991C7721a92d351Db4FaC990"
        )
        self.engine_registry = type("R", (), {"engine": _FakeEngine()})()


def _payload(pid: int, *, net: int = 500_000_000_000, failure: dict | None = None) -> dict:
    return {
        "path_id": pid,
        "gross_profit": 600_000_000_000,
        "net_profit": net,
        "gas_used": 300_000,
        "priority_fee": 2,
        "base_fee_next": 30,
        "execute_calldata": b"\\xab\\x58\\x98\\xe8\\x01",
        "access_list": None,
        "captured_swaps": [],
        "hop_count": 2,
        "failure": failure,
    }


def test_build_candidates_skips_payload_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    """Payload pids never enter the FFI sim batch (per-entry presence)."""
    session = types.SimpleNamespace(
        engine_registry=types.SimpleNamespace(engine=object()),
    )
    results = [(7, 1, 2, (3,), (4,), 42, (0,)), (9, 1, 2, (3,), (4,), 42, (0,))]
    payloads = {7: _payload(7)}

    class _FakeDispatchCandidate:
        def __init__(self, **kwargs) -> None:
            self.__dict__.update(kwargs)

    import degenbot.runner._dispatch as dispatch_mod

    monkeypatch.setattr(dispatch_mod, "DispatchCandidate", _FakeDispatchCandidate)
    built = dispatch_mod._build_dispatch_candidates(session, results, payloads=payloads)
    assert [c.path_id for c in built] == [9], "the payload pid must not enter the FFI batch"


def test_merge_payload_outcome_builds_submit_candidate() -> None:
    session = _FakeSession()
    payloads = {5: _payload(5)}
    merged = _merge_payload_outcome(session, None, payloads)
    assert merged is not None
    assert bool(merged)
    assert merged.candidate_count == 1
    assert merged.fail_count == 0
    assert merged.gas_unprofitable_count == 0
    cand = merged.gas_profitable[0]
    assert cand.path_id == 5
    assert int(cand.net_profit) == 500_000_000_000
    # (path_pools/access_list are parsed by the pyclass ctor — the submit
    # seam re-reads them Rust-side; only the scalar getters are Python-visible.)
    # path_infos attribute parity (the ``[profit]`` render source).
    assert merged.path_infos[5]["path_type"] == "V2-V4"


def test_merge_payload_failure_surfaces_sim_fail_record() -> None:
    session = _FakeSession()
    payloads = {
        5: _payload(
            5,
            failure={
                "fail_index": 3,
                "revert_data": bytes([0xDE, 0xAD]),
                "bucket": "inline-revert",
            },
        )
    }
    merged = _merge_payload_outcome(session, None, payloads)
    assert merged is not None
    assert merged.gas_profitable == []
    assert merged.fail_count == 1
    assert merged.fail_buckets == {"inline-revert": 1}
    rec = merged.failures[0]
    assert rec["path_id"] == 5
    assert rec["bucket"] == "inline-revert"
    assert rec["fail_index"] == 3
    assert rec["revert_data"].startswith("dead")


def test_merge_mixed_batch_stitches_base_outcome() -> None:
    session = _FakeSession()
    base = types.SimpleNamespace(
        gas_profitable=["legacy-cand"],
        gas_unprofitable_count=1,
        exception_count=0,
        fail_count=2,
        candidate_count=3,
        suppressed_count=0,
        thin_dropped=0,
        divergent_dropped=0,
        fot_dropped=0,
        fail_buckets={"rpc-failed": 2},
        failures=[{"path_id": 1, "bucket": "rpc-failed"}],
        path_infos={1: {"path_type": "V2", "hops": []}},
    )
    # MIN_PROFIT_NET == 1 (wei floor) — net=0 is the below-threshold arm.
    payloads = {5: _payload(5), 6: _payload(6, net=0)}
    merged = _merge_payload_outcome(session, base, payloads)
    assert merged is not None
    assert len(merged.gas_profitable) == 2
    assert merged.gas_profitable[0] == "legacy-cand"
    assert merged.gas_profitable[1].path_id == 5
    assert merged.gas_unprofitable_count == 2  # 1 base + 1 below MIN_PROFIT_NET
    assert merged.candidate_count == 5  # 3 base + 1 payload cand + 1 unprof
    assert merged.fail_buckets["rpc-failed"] == 2
    assert set(merged.path_infos) == {1, 5, 6}


def test_below_threshold_payload_counts_unprofitable() -> None:
    session = _FakeSession()
    payloads = {5: _payload(5, net=0)}
    merged = _merge_payload_outcome(session, None, payloads)
    assert merged is not None
    assert merged.gas_profitable == []
    assert merged.gas_unprofitable_count == 1


def test_inline_failure_record_shape_matches_ffi_rows() -> None:
    rec = _inline_failure_record(
        5, _payload(5, failure={"fail_index": 1, "revert_data": b"", "bucket": None})
    )
    assert rec["fail_index"] == 1
    assert rec["bucket"] == "inline-fail"
    assert rec["reverting_frame"] is None
    assert rec["captured_swaps"] == []
