//! P6YXA6 production-boot fix (red test): the python-driven pump builds the
//! engine via `ArbitrageEngine::with_core` — it must pack its construction
//! stances from the INSTALLED loader config (the _ffi module-init install),
//! not a fresh `BotConfig::default()`. Standalone integration-test binary:
//! the process-global holder is safe here (nobody else installs first).

#![expect(clippy::expect_used)]

use std::sync::Arc;

#[test]
fn with_core_packs_installed_fleet_stance() {
    // Mirror the true boot order: loader (env layer) -> holder install ->
    // engine construction via the no-cfg convenience ctor.
    std::env::set_var("DEGENBOT_FLEET", "fleet");
    let loaded = degenbot_config::BotConfigLoader::new()
        .load()
        .expect("config loads");
    assert!(
        matches!(
            loaded.config.fleet.stance,
            degenbot_config::FleetStance::Fleet
        ),
        "loader must parse DEGENBOT_FLEET=fleet"
    );
    assert!(
        degenbot_bot::bot_core::stance::install(Arc::new(loaded.config)),
        "first install into the fresh holder"
    );

    let core = Arc::new(degenbot_bot::bot_core::state_lock::StateLock::new(
        degenbot_bot::bot_core::BotState::new(),
    ));
    let engine = degenbot_bot::arb_engine::ArbitrageEngine::with_core(core);

    assert_eq!(
        engine.fleet_stance_probe(),
        "fleet",
        "with_core packed schema defaults instead of the installed config — \
         the production pump never observes fleet.stance (ADR-042 cutover bug)"
    );
}
