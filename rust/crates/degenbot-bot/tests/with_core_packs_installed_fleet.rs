//! LW-T9 hard cutover (ergo CQLMM2): the fleet stance is GONE — the worker
//! fleet is the only behavior. Standalone integration-test binary: the
//! process-global holder is safe here (nobody else installs first).
//!
//! Contracts pinned at the real-boot layer:
//! 1. A surviving `DEGENBOT_FLEET` setting fails the BOOT load loudly
//!    (the deprecation-style hard error, mirror of the P6YXA6
//!    `DEGENBOT_SOLVE_EXECUTOR` treatment).
//! 2. The python-driven pump's boot order (loader → holder install →
//!    `ArbitrageEngine::with_core`) still constructs the engine on the
//!    installed config (the P6YXA6 production-boot fix regression guard;
//!    there is no stance to probe — fleet structurally is the executor).

#![expect(clippy::expect_used)]

use std::sync::Arc;

/// The process env is process-global and the loader reads it — the env-touch
/// tests in this binary take this lock so parallel tests never observe an
/// env mid-toggle.
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn retired_fleet_stance_env_fails_the_boot_load_loudly() {
    let _env = ENV_LOCK.lock().expect("env lock");
    // Mirror the true boot order's env layer: a real (process) env var.
    std::env::set_var("DEGENBOT_FLEET", "fleet");
    let err = degenbot_config::BotConfigLoader::new()
        .load()
        .expect_err("the retired DEGENBOT_FLEET setting must fail the load");
    std::env::remove_var("DEGENBOT_FLEET");
    let msg = format!("{err}");
    assert!(
        msg.contains("DEGENBOT_FLEET"),
        "the error names the retired variable: {msg}"
    );
    assert!(
        msg.contains("fleet.stance"),
        "the error names the retired typed key: {msg}"
    );
}

#[test]
fn with_core_boots_from_the_installed_config_without_a_stance() {
    let _env = ENV_LOCK.lock().expect("env lock");
    let loaded = degenbot_config::BotConfigLoader::new()
        .load()
        .expect("a config with no retired keys loads");
    assert!(
        degenbot_bot::bot_core::stance::install(Arc::new(loaded.config)),
        "first install into the fresh holder"
    );

    let core = Arc::new(degenbot_bot::bot_core::state_lock::StateLock::new(
        degenbot_bot::bot_core::BotState::new(),
    ));
    // Construction packs its stances from the INSTALLED loader config and
    // unconditionally installs the fleet boots (no stance gate survives).
    let _engine = degenbot_bot::arb_engine::ArbitrageEngine::with_core(core);
}
