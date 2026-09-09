//! Acceptance criterion: precedence is unit-tested per layer on
//! representative keys of each type (bool, duration-ms, usize, enum).

use std::path::PathBuf;

use degenbot_config::{BotConfigLoader, MapEnv, SolveExecutor, Source};

// Representative keys, one per type:
// - bool   : `allocator.mimalloc_auto_purge` / `DEGENBOT_MIMALLOC_AUTO_PURGE`
// - ms     : `state_lock.warn_ms`            / `DEGENBOT_LOCK_WARN_MS`
// - usize  : `solve.envelope_max_tangent_lines` / `DEGENBOT_ENVELOPE_MAX_TANGENT_LINES`
// - enum   : `solve.executor` / `DEGENBOT_SOLVE_EXECUTOR`

// Test env provider.
fn map_env(pairs: &[(&str, &str)]) -> Box<dyn degenbot_config::EnvVars> {
    Box::new(MapEnv::new(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    ))
}

fn temp_toml(name: &str, body: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "degenbot-config-precedence-{}-{name}.toml",
        std::process::id()
    ));
    if let Err(e) = std::fs::write(&path, body) {
        unreachable!("temp toml write failed: {e}");
    }
    path
}

fn cleanup(path: &PathBuf) {
    // Temp-file cleanup is best-effort in a sandboxed test tree.
    if std::fs::remove_file(path).is_err() {}
}

/// Load that MUST succeed; panics with the config error otherwise.
fn must_ok(loader: &BotConfigLoader) -> degenbot_config::LoadedConfig {
    match loader.load() {
        Ok(catalog) => catalog,
        Err(e) => unreachable!("load constructed to succeed: {e}"),
    }
}

/// Load that MUST fail; panics when it succeeds.
fn must_err(loader: &BotConfigLoader) -> degenbot_config::ConfigError {
    let Err(err) = loader.load() else {
        unreachable!("load constructed to fail");
    };
    err
}

#[test]
fn defaults_feed_every_representative_type() {
    let cfg = must_ok(&BotConfigLoader::new().without_env());
    assert!(cfg.config.allocator.mimalloc_auto_purge, "bool default");
    assert_eq!(cfg.config.state_lock.warn_ms, 500, "duration-ms default");
    assert_eq!(
        cfg.config.solve.envelope_max_tangent_lines, 32,
        "usize default"
    );
    assert_eq!(
        cfg.config.solve.executor,
        SolveExecutor::Tokio,
        "enum default"
    );
    assert_eq!(
        cfg.source_of("DEGENBOT_MIMALLOC_AUTO_PURGE"),
        Some(Source::Default)
    );
}

#[test]
fn file_layer_overrides_defaults() {
    let path = temp_toml(
        "file",
        "[state_lock]\nwarn_ms = 1500\n\n[solve]\nenvelope_max_tangent_lines = 64\n\n[allocator]\nmimalloc_auto_purge = false\n",
    );
    let loaded = must_ok(&BotConfigLoader::new().without_env().with_config_path(&path));
    assert_eq!(loaded.config.state_lock.warn_ms, 1500);
    assert_eq!(loaded.config.solve.envelope_max_tangent_lines, 64);
    assert!(
        !loaded.config.allocator.mimalloc_auto_purge,
        "bool from file"
    );
    assert_eq!(
        loaded.source_of("DEGENBOT_LOCK_WARN_MS"),
        Some(Source::File)
    );
    cleanup(&path);
}

#[test]
fn env_layer_overrides_file_layer() {
    let path = temp_toml(
        "env",
        "[state_lock]\nwarn_ms = 1500\n\n[solve]\nexecutor = \"rayon\"\n",
    );
    let loaded = must_ok(
        &BotConfigLoader::new()
            .with_env(map_env(&[("DEGENBOT_LOCK_WARN_MS", "2500")]))
            .with_config_path(&path),
    );
    assert_eq!(loaded.config.state_lock.warn_ms, 2500, "env beats file");
    assert_eq!(loaded.source_of("DEGENBOT_LOCK_WARN_MS"), Some(Source::Env));
    // Env NOT set -> file value still applies.
    assert_eq!(loaded.config.solve.executor, SolveExecutor::Rayon);
    assert_eq!(
        loaded.source_of("DEGENBOT_SOLVE_EXECUTOR"),
        Some(Source::File)
    );
    cleanup(&path);
}

#[test]
fn cli_layer_overrides_env_and_file() {
    let path = temp_toml(
        "cli",
        "[state_lock]\nwarn_ms = 1500\n\n[solve]\nenvelope_max_tangent_lines = 64\n\n[allocator]\nmimalloc_auto_purge = false\n",
    );
    let loaded = must_ok(
        &BotConfigLoader::new()
            .with_env(map_env(&[
                ("DEGENBOT_LOCK_WARN_MS", "2500"),
                ("DEGENBOT_ENVELOPE_MAX_TANGENT_LINES", "96"),
                ("DEGENBOT_MIMALLOC_AUTO_PURGE", "0"),
                ("DEGENBOT_SOLVE_EXECUTOR", "rayon"),
            ]))
            .with_config_path(&path)
            // A CLI override key accepts the env name OR the TOML dotted path.
            .with_cli("DEGENBOT_LOCK_WARN_MS", "3500")
            .with_cli("solve.envelope_max_tangent_lines", "128")
            .with_cli("allocator.mimalloc_auto_purge", "true"),
    );
    assert_eq!(
        loaded.config.state_lock.warn_ms, 3500,
        "cli beats env (duration-ms)"
    );
    assert_eq!(
        loaded.config.solve.envelope_max_tangent_lines, 128,
        "cli (by TOML path) beats env (usize)"
    );
    assert!(
        loaded.config.allocator.mimalloc_auto_purge,
        "cli beats env (bool)"
    );
    assert_eq!(
        loaded.config.solve.executor,
        SolveExecutor::Rayon,
        "no cli override -> env wins (enum)"
    );
    assert_eq!(loaded.source_of("DEGENBOT_LOCK_WARN_MS"), Some(Source::Cli));
    assert_eq!(
        loaded.source_of("DEGENBOT_SOLVE_EXECUTOR"),
        Some(Source::Env)
    );
    cleanup(&path);
}

#[test]
fn every_layer_for_every_type_in_sequence() {
    // One full precedence chain per representative type.
    let path = temp_toml(
        "chain",
        "[solve]\nenvelope_max_tangent_lines = 64\nexecutor = \"rayon\"\n\n[state_lock]\nwarn_ms = 1500\n\n[allocator]\nmimalloc_auto_purge = false\n",
    );
    let loaded = must_ok(
        &BotConfigLoader::new()
            .with_env(map_env(&[
                ("DEGENBOT_ENVELOPE_MAX_TANGENT_LINES", "96"),
                ("DEGENBOT_SOLVE_EXECUTOR", "tokio"),
                ("DEGENBOT_LOCK_WARN_MS", "2500"),
                ("DEGENBOT_MIMALLOC_AUTO_PURGE", "0"),
            ]))
            .with_config_path(&path)
            .with_cli_overrides([
                (
                    "DEGENBOT_ENVELOPE_MAX_TANGENT_LINES".to_string(),
                    "7".to_string(),
                ),
                ("DEGENBOT_SOLVE_EXECUTOR".to_string(), "rayon".to_string()),
                ("DEGENBOT_LOCK_WARN_MS".to_string(), "42".to_string()),
                (
                    "DEGENBOT_MIMALLOC_AUTO_PURGE".to_string(),
                    "true".to_string(),
                ),
            ]),
    );
    assert_eq!(
        loaded.config.solve.envelope_max_tangent_lines, 7,
        "cli top (usize)"
    );
    assert_eq!(
        loaded.config.state_lock.warn_ms, 42,
        "cli top (duration-ms)"
    );
    assert!(
        loaded.config.allocator.mimalloc_auto_purge,
        "cli top (bool)"
    );
    assert_eq!(
        loaded.config.solve.executor,
        SolveExecutor::Rayon,
        "cli top (enum)"
    );
    cleanup(&path);
}

#[test]
fn loader_fails_closed_on_bad_values_and_unknown_keys() {
    // Bad enum value -> aggregated error naming the key type; no silent fallback.
    let err = must_err(
        &BotConfigLoader::new()
            .without_env()
            .with_cli("DEGENBOT_SOLVE_EXECUTOR", "ninja"),
    );
    assert!(
        format!("{err}").contains("SolveExecutor"),
        "error names the key"
    );

    // Unknown TOML key -> error.
    let path = temp_toml("unknown", "[nope]\nflag = true\n");
    let err = must_err(&BotConfigLoader::new().without_env().with_config_path(&path));
    assert!(format!("{err}").contains("unknown section"));
    cleanup(&path);

    // Unknown CLI key -> error.
    let err = must_err(
        &BotConfigLoader::new()
            .without_env()
            .with_cli("NOT_A_SCHEMA_KEY_1", "1"),
    );
    assert!(
        format!("{err}").contains("does not name a schema key"),
        "unknown cli keys are rejected"
    );
}

#[test]
fn missing_config_file_is_reported() {
    let err = must_err(
        &BotConfigLoader::new()
            .without_env()
            .with_config_path("/nonexistent/degenbot-config-should-not-exist.toml"),
    );
    assert!(format!("{err}").contains("unreadable"));
}

// ---- SMTH6M: ambient-runtime sizing key (`runtime.io_workers`) ----

#[test]
fn runtime_io_workers_unset_by_default_and_derived_marker() {
    let loaded = must_ok(&BotConfigLoader::new().without_env());
    assert_eq!(
        loaded.config.runtime.io_workers, None,
        "ambient workers default to the CPU-budget derivation, not a fixed count"
    );
}

#[test]
fn runtime_io_workers_env_key_loads_with_provenance() {
    let loaded =
        must_ok(&BotConfigLoader::new().with_env(map_env(&[("DEGENBOT_IO_WORKERS", "4")])));
    assert_eq!(loaded.config.runtime.io_workers, Some(4));
    assert_eq!(
        loaded.source_of("DEGENBOT_IO_WORKERS"),
        Some(Source::Env),
        "the declared DEGENBOT_* env name must map onto the typed field"
    );
}

#[test]
fn runtime_io_workers_file_and_env_precedence() {
    let path = temp_toml("runtime", "[runtime]\nio_workers = 6\n");
    let loaded = must_ok(
        &BotConfigLoader::new()
            .with_env(map_env(&[("DEGENBOT_IO_WORKERS", "4")]))
            .with_config_path(&path),
    );
    assert_eq!(loaded.config.runtime.io_workers, Some(4), "env beats file");
    assert_eq!(loaded.source_of("DEGENBOT_IO_WORKERS"), Some(Source::Env));
    let from_file = must_ok(&BotConfigLoader::new().without_env().with_config_path(&path));
    assert_eq!(from_file.config.runtime.io_workers, Some(6), "file wins");
    assert_eq!(
        from_file.source_of("DEGENBOT_IO_WORKERS"),
        Some(Source::File)
    );
    cleanup(&path);
}

#[test]
fn runtime_io_workers_invalid_value_fails_closed() {
    let err =
        must_err(&BotConfigLoader::new().with_env(map_env(&[("DEGENBOT_IO_WORKERS", "lots")])));
    assert!(
        format!("{err}").contains("runtime.io_workers"),
        "error names the typed key: {err}"
    );
}

#[test]
fn legacy_tokio_worker_threads_env_fails_the_load() {
    // SMTH6M: the raw `TOKIO_WORKER_THREADS` read that sized the ambient
    // runtime is retired. Its name is NOT a schema key; a surviving setting
    // must fail the load loudly (config-loader fail-closed convention) and
    // point at the replacement, never silently size the runtime.
    let ok = BotConfigLoader::new().with_env(map_env(&[])).load();
    assert!(
        ok.is_ok(),
        "absence of the legacy name must not fail the load"
    );
    let err = must_err(&BotConfigLoader::new().with_env(map_env(&[("TOKIO_WORKER_THREADS", "2")])));
    let msg = format!("{err}");
    assert!(
        msg.contains("TOKIO_WORKER_THREADS"),
        "the legacy name is named in the error: {msg}"
    );
    assert!(
        msg.contains("DEGENBOT_IO_WORKERS"),
        "the error points at the replacement key: {msg}"
    );
}

#[test]
fn unset_option_keys_stay_unset_and_default_provenance_holds() {
    let loaded = must_ok(&BotConfigLoader::new().without_env());
    assert_eq!(loaded.config.allocator.mimalloc_purge_delay_ms, None);
    assert_eq!(
        loaded.provenance.len(),
        degenbot_config::SCHEMA.len(),
        "one provenance entry per schema key"
    );
    for key in degenbot_config::SCHEMA {
        assert_eq!(loaded.source_of(key.env), Some(Source::Default));
    }
}
