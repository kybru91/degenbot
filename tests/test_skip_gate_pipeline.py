"""Pipeline-side wiring of SkipGate: counter replay for memoized V4 rejects (2CBDPR).

The memo short-circuit in `PathRegistrationPipeline._consume` must keep the
summary counters byte-compatible with the first-attempt branches, so the
`[build_paths] Progress` breakdown looks identical whether a rejection is
seen live or replayed from the memo.

The CXKACI concurrent-duplicate-build coordination that used to live in a
Python claim driver (`_build_pool_claimed` over the `PoolBuildClaims` FFI
peer) was DISSOLVED by the PRG-1 registry cutover (epic IRUMXD): the Rust
build path itself single-flights duplicate builds and answers
already-registered addresses from the registry of record (`BotState`). Its
guarantees are enforced in Rust (`bot::build_flights` unit tests + the
`build_v2_pool_ans*` registry-of-record FFI tests), not here.
"""

import asyncio
from types import SimpleNamespace

import pytest

from degenbot.exceptions import PoolAlreadyRegisteredError
from degenbot.runner.build_paths import PathRegistrationPipeline


def make_pipeline() -> PathRegistrationPipeline:
    ctx = SimpleNamespace(
        bot=None,
        chain_id=1,
        db=None,
        uniswap_v3_tracker=None,
        sushiswap_v3_tracker=None,
        pancakeswap_v3_tracker=None,
        weth=None,
    )
    return PathRegistrationPipeline(context=ctx, engine_registry=None)


def test_memo_replay_hook_rejection_counts_admission() -> None:
    p = make_pipeline()
    assert p._reject_v4_from_memo("v4-hook-rejected") is True
    assert p.v4_hook_rejected == 1
    assert p.v4_dynamic_fee_rejected == 0
    assert p._skip_reasons["v4-hook-rejected"] == 1


def test_memo_replay_dynamic_fee_counts_admission() -> None:
    p = make_pipeline()
    assert p._reject_v4_from_memo("v4-dynamic-fee-rejected") is True
    assert p.v4_dynamic_fee_rejected == 1
    assert p.v4_hook_rejected == 0
    assert p._skip_reasons["v4-dynamic-fee-rejected"] == 1


def test_memo_replay_plain_failure_is_not_admission() -> None:
    p = make_pipeline()
    assert p._reject_v4_from_memo("build-v4:HighFeePoolRejectedError") is False
    assert p.v4_hook_rejected == 0
    assert p.v4_dynamic_fee_rejected == 0
    assert p._skip_reasons["build-v4:HighFeePoolRejectedError"] == 1


# ── CXKACI: concurrent duplicate-build race (registration variance) ──────────
#
# With REG_WORKERS concurrent consumers, two candidates containing the same
# not-yet-built pool race between the Bot registry pre-check and the Rust
# registration; the loser raises PoolAlreadyRegisteredError although the pool
# becomes available milliseconds later. The build path must retry the whole
# build (the retry hits the registry pre-check and returns the existing pool)
# instead of losing the path AND fatally memoizing the pool.


def test_exhausted_race_skip_does_not_memoize_fatal() -> None:
    """The exhausted-race skip must not treat the pool as an immutable fact."""
    p = make_pipeline()
    tag = "build-v4:PoolAlreadyRegisteredError"
    # The wiring in _consume notes the tag with fatal=False: a raced pool is
    # usable on later blocks (the winner's registry entry exists), so the
    # fatal memo must never short-circuit it.
    assert p.skip_gate.fatal_tag("v4", "som-mock-pool-id") is None
    p.skip_gate.note("v4", "some-mock-pool-id", tag, fatal=False)
    assert p.skip_gate.fatal_tag("v4", "some-mock-pool-id") is None
