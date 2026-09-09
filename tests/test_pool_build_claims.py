"""FFI-level tests for the Rust PoolBuildClaims coordinator (CXKACI).

The claim lives in the Rust core; Python stays a driver (see
``degenbot/runner/build_paths.py::_build_pool_claimed``). These exercises pin
the FFI surface: leader election, waiter resolution, failure propagation,
and the closed-claim-window contract.
"""

import asyncio

import pytest

from degenbot._ffi import PoolBuildClaims


def test_leader_publishes_and_waiters_share_the_pool() -> None:
    async def scenario() -> object:
        claims = PoolBuildClaims()
        assert claims.try_claim("v4", "hash-1") is True
        assert claims.try_claim("v4", "hash-1") is False, "second consumer waits"

        # Direct await (the FFI returns a Rust-resolved awaitable, not a
        # task-able coroutine — same contract as the subscription iterator).
        waiter = claims.wait("v4", "hash-1")
        await asyncio.sleep(0.02)  # the waiter's Rust future parks
        claims.complete("v4", "hash-1", "shared-pool-object")
        return await waiter

    assert asyncio.run(scenario()) == "shared-pool-object"


def test_failure_reaches_waiters_and_releases_the_claim() -> None:
    async def scenario() -> None:
        claims = PoolBuildClaims()
        assert claims.try_claim("v2", "pool-9")
        waiter = claims.wait("v2", "pool-9")
        await asyncio.sleep(0.02)
        claims.fail("v2", "pool-9", ValueError("rpc down"))
        with pytest.raises(ValueError, match="rpc down"):
            await waiter
        # the claim is released: a fresh build can start
        assert claims.try_claim("v2", "pool-9") is True

    asyncio.run(scenario())


def test_no_claim_in_flight_returns_none() -> None:
    async def scenario() -> object:
        claims = PoolBuildClaims()
        return await claims.wait("v3", "never-claimed")

    assert asyncio.run(scenario()) is None


def test_distinct_keys_claim_independently() -> None:
    claims = PoolBuildClaims()
    assert claims.try_claim("v3", "a") is True
    assert claims.try_claim("v3", "b") is True
    assert claims.try_claim("v3", "a") is False