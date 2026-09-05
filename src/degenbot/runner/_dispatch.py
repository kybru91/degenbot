"""Dispatch + sim-render helpers for the settlement-arbitrage ``BotRunner``.

Extracted from ``examples/eth_backrun_v2_v3_v4_rust.py`` (epic 5TSYKN, task
DZTFSJ). Owns the encode→simulate→submit leaf
(:func:`_dispatch_profitable` — the ``dispatch_profitable`` /
``dispatch_and_submit`` Rust seam) and the ``[sim]``/``[profit]``/``[sim-fail]``
renderers that contextualize ``DispatchOutcome``.

The renderers are display-only (``stays-python``); all sim/submit arithmetic
runs in the Rust core. Only candidate-list shaping + log rendering happen here.
"""

from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING

from degenbot.dispatch import (
    DispatchCandidate,
    TxSigner,
    dispatch_and_submit,
    dispatch_profitable,
)
from degenbot.logging import logger as bot_logger
from degenbot.runner._render import (
    _render_fot_tokens,
    _render_profit_logs,
    _render_sim_failures,
    _render_sim_summary,
)
from degenbot.runner.config import ArbitrageConfig

if TYPE_CHECKING:
    from degenbot.runner.bot_runner import _SessionState

#: One raw engine-result row (path_id, optimal_input, profit, hop_outputs,
#: consumed_inputs, solve_block, state_nonces) - the tuple shape the result
#: batch stream delivers.
_RawResult = tuple[int, int, int, tuple[int, ...], tuple[int, ...], int, tuple[int, ...]]

from degenbot.runner._driver_constants import (  # ruff: ignore[module-import-not-at-top-of-file] - after the type alias block
    ERC6909_PROFIT,
    INJECT_EXECUTOR_CODE,
    MIN_PROFIT_MARGIN_BPS,
    MIN_PROFIT_NET,
)

# The executor runtime bytecode file (one canonical filename in any
# contracts directory).
_EXECUTOR_RUNTIME_FILE = "cmd_executor_runtime_bytecode.txt"


def _resolve_executor_runtime_path(cfg: ArbitrageConfig) -> pathlib.Path:
    """Resolve the executor-runtime bytecode path — explicit, NO filesystem walk.

    Resolution order (first hit wins):
    1. ``cfg.executor_runtime`` — the operator's explicit path.
    2. ``$DEGENBOT_CONTRACTS_DIR/<file>`` — one explicit contracts dir.
    3. Exactly one computed candidate for the source layout: the repo root
       reached by a fixed-depth hop from this module
       (``<root>/src/degenbot/runner/dispatch.py`` -> ``<root>``), then
       ``contracts/<file>``. A wheel install has no such candidate — the
       operator must pass ``executor_runtime`` explicitly.
    """
    if cfg.executor_runtime is not None:
        return pathlib.Path(cfg.executor_runtime)
    env_dir = os.environ.get("DEGENBOT_CONTRACTS_DIR")
    if env_dir:
        return pathlib.Path(env_dir) / _EXECUTOR_RUNTIME_FILE
    root = pathlib.Path(__file__).resolve().parents[3]
    return root / "contracts" / _EXECUTOR_RUNTIME_FILE


def _load_executor_runtime_bytecode(cfg: ArbitrageConfig) -> str:
    """Load the patched runtime bytecode (0x-prefixed hex text).

    The bytecode has all 5 immutable slots baked in: OWNER_ADDR, WETH_ADDR,
    POOL_MANAGER_ADDR, and 2 precomputed delta slots (WETH, NATIVE).
    See contracts/recompile.py for the full layout.
    """
    bytecode_path = _resolve_executor_runtime_path(cfg)
    if not bytecode_path.exists():
        msg = (
            f"executor runtime bytecode not found at {bytecode_path}. "
            "Set ArbitrageConfig.executor_runtime to the file path, or set "
            "DEGENBOT_CONTRACTS_DIR to the directory containing "
            f"{_EXECUTOR_RUNTIME_FILE} (wheel installs: pass executor_runtime explicitly)."
        )
        raise RuntimeError(msg)
    code = bytecode_path.read_text(encoding="utf-8").strip()
    if not code.startswith("0x"):
        msg = f"Runtime bytecode file must start with 0x, got: {code[:20]}..."
        raise ValueError(msg)
    bot_logger.info(
        f"[inject] Loaded executor runtime bytecode: "
        f"{len(code) // 2 - 1} bytes from {bytecode_path}",
    )
    return code


async def _dispatch_profitable(
    session: _SessionState,
    results: list[_RawResult],
    *,
    block_timestamp: int,
    base_fee_next: int,
    operator_nonce: int,
) -> None:
    """Encode - simulate - submit one batch of profitable results serially.

    The A5 cutover LEAF, kept as the serial composition for the pipeline A/B
    arm. Production drives :mod:`degenbot.runner._sim_submit_pipeline` (SIMPIPE
    option A: K-way concurrent sims over the same seam contracts, ordered
    submit fan-in). All session coordination state is read from the single
    ``session`` owner (CONTEXT.md: *session state*), never re-passed.
    """
    candidates = _build_dispatch_candidates(session, results)
    if not candidates:
        return
    current_block = session.dispatcher.current_block
    outcome = await _simulate_batch(
        session,
        candidates,
        block_timestamp=block_timestamp,
        base_fee_next=base_fee_next,
        current_block=current_block,
    )
    _render_outcome(session, outcome, current_block)
    await _submit_batch_records(
        session,
        outcome,
        operator_nonce=operator_nonce,
    )


def _build_dispatch_candidates(
    session: _SessionState,
    results: list[_RawResult],
) -> list[DispatchCandidate]:
    """Shape a batch of raw engine results into Rust-seam candidates.

    Shared by the serial leaf (:func:`_dispatch_profitable`) and the concurrent
    pipeline (``_sim_submit_pipeline``): the GIL-held candidate construction +
    the empty-hop skip (``[sim-none]``). Returns an EMPTY list when nothing is
    dispatchable (the caller skips sim + submit).
    """
    engine_registry = session.engine_registry
    candidates: list[DispatchCandidate] = []
    for pid, inp, prof, ho, ci, sb, sn in results:
        if not ho:
            bot_logger.debug(f"[sim-none] path={pid}: empty hop_outputs")
            continue
        candidates.append(
            DispatchCandidate(
                engine=engine_registry.engine,
                path_id=pid,
                optimal_input=inp,
                engine_profit=prof,
                hop_outputs=list(ho),
                consumed_inputs=list(ci),
                solve_block=sb,
                state_nonces=list(sn),
                # SMOZG3: the operator's ERC6909 vault-capture toggle - the
                # Rust seam defaults it to False (custody capture, the
                # long-standing production behavior); env-gated opt-in.
                erc6909_profit=ERC6909_PROFIT,
            ),
        )
    return candidates


async def _simulate_batch(
    session: _SessionState,
    candidates: list[DispatchCandidate],
    *,
    block_timestamp: int,
    base_fee_next: int,
    current_block: int,
) -> object:
    """Run the Rust simulate fan-out for a candidate batch (one DispatchOutcome)."""
    if session.sim_ctx is None:
        msg = "SimulateContext is required to dispatch (non-Alloy provider or sim context unbuilt)"
        raise RuntimeError(msg)
    return await dispatch_profitable(
        candidates=candidates,
        context=session.sim_ctx,
        dispatcher=session.dispatcher,
        base_fee_next=base_fee_next,
        current_block=current_block,
        block_timestamp=block_timestamp,
        min_profit_net=MIN_PROFIT_NET,
        min_profit_margin_bps=MIN_PROFIT_MARGIN_BPS,
        engine=session.engine_registry.engine,
    )


def _render_outcome(
    session: _SessionState,
    outcome: object,
    current_block: int,
) -> None:
    """The display-only renderers over a sim outcome (D4 stays-python)."""
    _render_sim_summary(outcome)
    _render_sim_failures(outcome, current_block=current_block)
    _render_fot_tokens(session.dispatcher, current_block)
    _render_profit_logs(outcome)


async def _submit_batch_records(
    session: _SessionState,
    outcome: object,
    *,
    operator_nonce: int,
) -> None:
    """Submit gas-profitable candidates via the Rust submit leaf + render records.

    Shared by the serial leaf and the pipeline's ordered submitter. Expects
    the operator nonce fetched AT submit time (serialized consumers only).
    """
    async_alloy = session.async_w3.as_async_alloy()
    if async_alloy is None:
        bot_logger.error("[dispatch] async_w3 is not an Alloy-backed provider; cannot submit")
        return
    signer = TxSigner(key=session.cfg.operator_private_key, chain_id=1)
    records = await dispatch_and_submit(
        candidates=outcome.gas_profitable,
        dispatcher=session.dispatcher,
        provider=async_alloy,
        signer=signer,
        operator_nonce=operator_nonce,
        current_block=session.dispatcher.current_block,
        dry_run=session.cfg.dry_run,
        inject_code=INJECT_EXECUTOR_CODE,
    )
    for record in records:
        if record["kind"] == "submitted":
            bot_logger.info(
                f"Submitted path {record['path_id']} "
                f"hash={record['tx_hash']} nonce={record['nonce']}",
            )
        elif record["reason"] == "pools_claimed":
            bot_logger.debug(f"[dispatch] skip path={record['path_id']}: pools claimed after sim")
        elif record["reason"] == "dry_run":
            pass  # dry_run skip already logged above
        elif record["reason"] == "inject_code":
            bot_logger.warning(
                f"[dispatch] path={record.get('path_id')}: skipping submission - "
                "INJECT_EXECUTOR_CODE is active",
            )
        elif record["reason"] == "broadcast_failed":
            bot_logger.debug(f"Send failed: {record.get('detail', '')}")
