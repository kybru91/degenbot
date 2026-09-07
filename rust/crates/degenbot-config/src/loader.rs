//! The 12-factor loader: precedence CLI > env > file > defaults.
//!
//! Layer order is applied to the typed default value; every assignment is
//! recorded in [`LoadedConfig::provenance`] so operators/tests can see WHICH
//! layer supplied each key.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::error::ConfigError;
use crate::schema::{BotConfig, SCHEMA};

/// Which layer supplied a key's value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Source {
    /// Built-in default from the schema declaration.
    Default,
    /// `--config` TOML file.
    File,
    /// `DEGENBOT_*` environment variable.
    Env,
    /// CLI / explicit argument override.
    Cli,
}

impl std::fmt::Display for Source {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Default => "default",
            Self::File => "file",
            Self::Env => "env",
            Self::Cli => "cli",
        })
    }
}

/// Environment variable provider (test seam so no test ever mutates the
/// process environment).
pub trait EnvVars {
    /// Env lookup; `None` when unset.
    fn get(&self, name: &str) -> Option<String>;
}

/// The real process environment.
pub struct ProcessEnv;

impl EnvVars for ProcessEnv {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Overlay map environment (tests, embedding hosts).
#[derive(Debug, Clone, Default)]
pub struct MapEnv(BTreeMap<String, String>);

impl MapEnv {
    /// Build from an ordered map.
    #[must_use]
    pub fn new(map: BTreeMap<String, String>) -> Self {
        Self(map)
    }
}

impl EnvVars for MapEnv {
    fn get(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

/// Loaded result: the typed config plus per-key provenance.
#[derive(Debug, Clone)]
pub struct LoadedConfig {
    /// The typed configuration.
    pub config: BotConfig,
    /// Winning source per env key name (one entry per schema key).
    pub provenance: BTreeMap<&'static str, Source>,
}

impl LoadedConfig {
    /// Which layer supplied `env_key` (e.g. `DEGENBOT_OTEL`).
    #[must_use]
    pub fn source_of(&self, env: &str) -> Option<Source> {
        self.provenance.get(env).copied()
    }
}

/// Builder for the layered load.
#[derive(Default)]
pub struct BotConfigLoader {
    file: Option<PathBuf>,
    cli: Vec<(String, String)>,
    env: Option<Box<dyn EnvVars>>,
}

impl std::fmt::Debug for BotConfigLoader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BotConfigLoader")
            .field("file", &self.file)
            .field("cli", &self.cli)
            .field(
                "env",
                &if self.env.is_some() {
                    "<custom>"
                } else {
                    "<process>"
                },
            )
            .finish()
    }
}

impl BotConfigLoader {
    /// Empty loader: defaults only (no env, no file, no CLI) until a layer
    /// is attached with the `with_*` builders.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Select the config file (the `--config <path>` surface). Replaces any
    /// previously selected path.
    #[must_use]
    pub fn with_config_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.file = Some(path.into());
        self
    }

    /// Add one CLI / explicit override. The key may be the `DEGENBOT_*` env
    /// name OR the dotted TOML path (e.g. `solve.executor`).
    #[must_use]
    pub fn with_cli(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.cli.push((key.into(), value.into()));
        self
    }

    /// Add CLI overrides in bulk (key = env name or TOML path).
    #[must_use]
    pub fn with_cli_overrides(
        mut self,
        overrides: impl IntoIterator<Item = (String, String)>,
    ) -> Self {
        self.cli.extend(overrides);
        self
    }

    /// Replace the environment source (tests pass a [`MapEnv`]; production
    /// keeps the default [`ProcessEnv`]).
    #[must_use]
    pub fn with_env(mut self, env: Box<dyn EnvVars>) -> Self {
        self.env = Some(env);
        self
    }

    /// Drop the environment layer entirely (file-vs-default tests).
    #[must_use]
    pub fn without_env(mut self) -> Self {
        self.env = None;
        self
    }

    /// Run the layered load. Fails closed with ALL problems aggregated.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when the file is unreadable/unparsable, a TOML key is
    /// unknown, a value does not parse into the declared kind, or a CLI
    /// override names an undeclared key.
    pub fn load(&self) -> Result<LoadedConfig, ConfigError> {
        let mut problems: Vec<String> = Vec::new();
        let mut config = BotConfig::default();

        // Defaults first: every schema key starts at its declared default.
        let mut provenance: BTreeMap<&'static str, Source> =
            SCHEMA.iter().map(|k| (k.env, Source::Default)).collect();

        // Layer 2 (lowest override): --config TOML file.
        if let Some(path) = &self.file {
            Self::apply_file(path, &mut config, &mut provenance, &mut problems);
        }

        // Layer 3: environment. Iterate the SCHEMA (not the process env) so
        // foreign DEGENBOT_*-prefixed vars never leak into the typed tree.
        if let Some(env) = &self.env {
            for key in SCHEMA {
                if let Some(raw) = env.get(key.env) {
                    match config.assign(key.section, key.field, &raw) {
                        Ok(()) => {
                            provenance.insert(key.env, Source::Env);
                        }
                        Err(problem) => problems.push(problem),
                    }
                }
            }
        }

        // Layer 4 (highest): CLI / explicit argument overrides.
        for (name, value) in &self.cli {
            match resolve_key(name) {
                Some(key) => match config.assign(key.section, key.field, value) {
                    Ok(()) => {
                        provenance.insert(key.env, Source::Cli);
                    }
                    Err(problem) => problems.push(problem),
                },
                None => problems.push(format!(
                    "cli override {name:?} does not name a schema key (env name or TOML path required)"
                )),
            }
        }

        if problems.is_empty() {
            Ok(LoadedConfig { config, provenance })
        } else {
            Err(ConfigError::of(problems))
        }
    }

    fn apply_file(
        path: &Path,
        config: &mut BotConfig,
        provenance: &mut BTreeMap<&'static str, Source>,
        problems: &mut Vec<String>,
    ) {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) => {
                problems.push(format!("--config {}: unreadable: {e}", path.display()));
                return;
            }
        };
        let value: toml::Table = match text.parse() {
            Ok(value) => value,
            Err(e) => {
                problems.push(format!("--config {}: parse error: {e}", path.display()));
                return;
            }
        };
        // A parsed `toml::Table` IS the top-level table.
        let table = &value;
        for (section, section_value) in table {
            let members: Vec<_> = SCHEMA
                .iter()
                .filter(|k| k.section == section.as_str())
                .collect();
            if members.is_empty() {
                problems.push(format!(
                    "--config {}: unknown section [{section}]",
                    path.display()
                ));
                continue;
            }
            let Some(section_table) = section_value.as_table() else {
                problems.push(format!(
                    "--config {}: section [{section}] must be a table",
                    path.display()
                ));
                continue;
            };
            for (field, field_value) in section_table {
                let Some(key) = members.iter().copied().find(|k| k.field == field.as_str()) else {
                    problems.push(format!(
                        "--config {}: unknown key {field} in section [{section}]",
                        path.display()
                    ));
                    continue;
                };
                let Some(raw) = toml_value_to_raw(field_value, key.toml_path, path, problems)
                else {
                    continue;
                };
                match config.assign(key.section, key.field, &raw) {
                    Ok(()) => {
                        provenance.insert(key.env, Source::File);
                    }
                    Err(problem) => problems.push(problem),
                }
            }
        }
    }
}

/// Resolve a CLI override key: exact env name first, then TOML path.
fn resolve_key(name: &str) -> Option<&'static crate::schema::KeyDecl> {
    SCHEMA.iter().find(|k| k.env == name || k.toml_path == name)
}

/// Convert a TOML scalar into the normalized raw text the typed parser
/// consumes (`bool`/integer/float render to their textual form; the `u128`
/// wei kind is declared as a quoted string in TOML).
fn toml_value_to_raw(
    value: &toml::Value,
    label: &str,
    path: &Path,
    problems: &mut Vec<String>,
) -> Option<String> {
    let rendered = match value {
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(float) => float.to_string(),
        toml::Value::String(s) => s.clone(),
        other => {
            problems.push(format!(
                "--config {}: {label}: unsupported TOML value {other:?} (expected bool/integer/float/string)",
                path.display()
            ));
            return None;
        }
    };
    Some(rendered)
}
