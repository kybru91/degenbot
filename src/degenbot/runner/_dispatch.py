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
    SubmitCandidate,
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
    payloads: dict[int, dict] | None = None,
) -> None:
    """Encode - simulate - submit one batch of profitable results serially.

    The A5 cutover LEAF, kept as the serial composition for the pipeline A/B
    arm. Production drives :mod:`degenbot.runner._sim_submit_pipeline` (SIMPIPE
    option A: K-way concurrent sims over the same seam contracts, ordered
    submit fan-in). All session coordination state is read from the single
    ``session`` owner (CONTEXT.md: *session state*), never re-passed.

    SIMPIPE2 T3: ``payloads`` are the engine's inline-sim results — those
    entries skip the FFI sim and their submit records are stitched straight
    into the outcome (per-entry presence decides).
    """
    candidates = _build_dispatch_candidates(session, results, payloads=payloads)
    outcome: object | None = None
    if candidates:
        current_block = session.dispatcher.current_block
        outcome = await _simulate_batch(
            session,
            candidates,
            block_timestamp=block_timestamp,
            base_fee_next=base_fee_next,
            current_block=current_block,
        )
    merged = _merge_payload_outcome(session, outcome, payloads)
    if not merged:
        return
    _render_outcome(session, merged, session.dispatcher.current_block)
    await _submit_batch_records(
        session,
        merged,
        operator_nonce=operator_nonce,
    )


def _build_dispatch_candidates(
    session: _SessionState,
    results: list[_RawResult],
    *,
    payloads: dict[int, dict] | None = None,
) -> list[DispatchCandidate]:
    """Shape a batch of raw engine results into Rust-seam candidates.

    Shared by the serial leaf (:func:`_dispatch_profitable`) and the concurrent
    pipeline (``_sim_submit_pipeline``): the GIL-held candidate construction +
    the empty-hop skip (``[sim-none]``). Returns an EMPTY list when nothing is
    dispatchable (the caller skips sim + submit).

    SIMPIPE2 T3: path ids present in ``payloads`` were ALREADY simulated
    inline in the Rust engine — they never enter the FFI sim batch (the
    payload derives their submit records directly; per-entry presence
    decides, so a mixed batch only degrades the payload-less entries).
    """
    engine_registry = session.engine_registry
    candidates: list[DispatchCandidate] = []
    for pid, inp, prof, ho, ci, sb, sn in results:
        if not ho:
            bot_logger.debug(f"[sim-none] path={pid}: empty hop_outputs")
            continue
        if payloads and pid in payloads:
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


class MergedOutcome:
    """A ``DispatchOutcome``-protocol view: the FFI outcome + payload records.

    SIMPIPE2 T3: entries the engine simulated inline never enter the FFI
    batch, so the batch outcome alone under-reports. This adapter stitches
    the payload-derived submit candidates/failure records into the base
    outcome's tallies so the renderers and the submit leaf see one
    attribute-uniform object (the ``[sim]``/``[profit]``/``[sim-fail]``
    attribute parity the T3 render contract demands). When every entry was
    payload-served (no FFI batch ran), ``base`` is ``None`` and only the
    payload records surface.
    """

    def __init__(
        self,
        base: object | None,
        candidates: list[SubmitCandidate],
        failures: list[dict],
        path_infos: dict[int, dict],
        unprofitable_count: int,
    ) -> None:
        self._base = base
        self._candidates = candidates
        self._failures = failures
        self._path_infos = path_infos
        self._unprofitable_count = unprofitable_count

    def __bool__(self) -> bool:
        # A merged outcome with no base and no payload records is empty
        # ( callers skip render+submit on falsy outcomes).
        if self._base is not None:
            return True
        return bool(self._candidates or self._failures or self._unprofitable_count)

    @property
    def gas_profitable(self) -> list[SubmitCandidate]:
        base = list(self._base.gas_profitable) if self._base is not None else []
        return base + self._candidates

    @property
    def gas_unprofitable_count(self) -> int:
        base = self._base.gas_unprofitable_count if self._base is not None else 0
        return base + self._unprofitable_count

    @property
    def exception_count(self) -> int:
        return self._base.exception_count if self._base is not None else 0

    @property
    def fail_count(self) -> int:
        base = self._base.fail_count if self._base is not None else 0
        return base + len(self._failures)

    @property
    def candidate_count(self) -> int:
        base = self._base.candidate_count if self._base is not None else 0
        return base + len(self._candidates) + self._unprofitable_count + len(self._failures)

    @property
    def suppressed_count(self) -> int:
        return self._base.suppressed_count if self._base is not None else 0

    @property
    def thin_dropped(self) -> int:
        return self._base.thin_dropped if self._base is not None else 0

    @property
    def divergent_dropped(self) -> int:
        return self._base.divergent_dropped if self._base is not None else 0

    @property
    def fot_dropped(self) -> int:
        return self._base.fot_dropped if self._base is not None else 0

    @property
    def fail_buckets(self) -> dict[str, int]:
        base = dict(self._base.fail_buckets) if self._base is not None else {}
        for rec in self._failures:
            bucket = rec["bucket"]
            base[bucket] = base.get(bucket, 0) + 1
        return base

    @property
    def failures(self) -> list[dict]:
        base = list(self._base.failures) if self._base is not None else []
        return base + self._failures

    @property
    def path_infos(self) -> dict[int, dict]:
        base = dict(self._base.path_infos) if self._base is not None else {}
        base.update(self._path_infos)
        return base


def _inline_failure_record(pid: int, payload: dict) -> dict:
    """Shape a payload ``failure`` sub-dict into the FFI ``failures`` row shape.

    The renderer reads ``path_id``/``bucket``/``fail_index``/``revert_data``
    positionally and everything else via ``.get`` — the payload carries the
    scalar trio plus the revert bytes; the EVM-diagnostic extras default empty
    (the inline sim surfaces revert detail through ``revert_data``).
    """
    failure = payload["failure"]
    revert_data = failure.get("revert_data") or b""
    revert_hex = revert_data if isinstance(revert_data, str) else bytes(revert_data).hex()
    return {
        "path_id": pid,
        "bucket": failure.get("bucket") or "inline-fail",
        "fail_index": failure.get("fail_index"),
        "revert_data": revert_hex,
        "reverting_frame": None,
        "captured_swaps": payload.get("captured_swaps") or [],
        "reverted_swaps": [],
        "call_trace": [],
        "log_full_count": 0,
    }


def _merge_payload_outcome(
    session: _SessionState,
    base_outcome: object | None,
    payloads: dict[int, dict] | None,
) -> MergedOutcome | None:
    """Stitch inline-sim payload records into (or over) the FFI batch outcome.

    Per-entry presence decides: each payload either yields a
    :class:`SubmitCandidate` (gross/net/gas/calldata/access-list from the
    engine's inline sim — the same field-set the FFI join stamps) or a
    ``[sim-fail]`` record through the payload's ``failure`` field. Entries
    whose net profit falls below :data:`MIN_PROFIT_NET` count as
    gas-unprofitable (valid sim, below threshold — same categorization the
    FFI fan-out applies).

    ``base_outcome`` is the FFI outcome for the REMAINING (payload-less)
    entries — ``None`` only when there was nothing to send through the FFI
    batch at all.
    """
    if not payloads:
        return base_outcome or None
    candidates: list[SubmitCandidate] = []
    failures: list[dict] = []
    path_infos: dict[int, dict] = {}
    unprofitable = 0
    sim_ctx = session.sim_ctx
    if sim_ctx is None:
        msg = "SimulateContext is required to merge payload records"
        raise RuntimeError(msg)
    executor_address = sim_ctx.executor_address
    engine = session.engine_registry.engine

    for raw_pid, payload in payloads.items():
        pid = int(raw_pid)
        path_info = engine.payload_path_info(pid)
        hops: list[dict] = path_info.get("hops", []) if path_info else []
        if path_info is not None:
            path_infos[pid] = path_info
        # The mutual-exclusion set, derived from the hops exactly as the FFI
        # join does (V4 -> pool_id_hex; V2/V3 -> checksummed pool_address).
        path_pools = {h["pool_id_hex"] if h["family"] == "V4" else h["pool_address"] for h in hops}
        if payload.get("failure") is not None:
            failures.append(_inline_failure_record(pid, payload))
            continue
        if int(payload["net_profit"]) < MIN_PROFIT_NET:
            unprofitable += 1
            continue
        candidates.append(
            SubmitCandidate(
                pid,
                int(payload["gross_profit"]),
                int(payload["net_profit"]),
                int(payload["gas_used"]),
                int(payload["priority_fee"]),
                int(payload["base_fee_next"]),
                bytes(payload["execute_calldata"]),
                executor_address,
                access_list=payload.get("access_list"),
                path_pools=path_pools,
            )
        )

    if base_outcome is None and not (candidates or failures or unprofitable):
        return None
    return MergedOutcome(base_outcome, candidates, failures, path_infos, unprofitable)


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
