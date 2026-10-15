//! Configuration of the OAR base system.
//!
//! The OAR3 configuration file (`/etc/oar/oar.conf`, see `etc/oar.conf`) is a
//! bash-sourced list of `KEY="value"` assignments. We load it with [`dotenvy`],
//! keep the OAR3 keys we understand, overlay `OAR_*` (and raw OAR3-named)
//! environment variables, and let the [`config`] crate type-check the result
//! into [`Config`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::types::{DEFAULT_WALLTIME_SECS, NodeInfo};

/// Environment variable giving the path of the OAR3 configuration file.
pub const CONFIG_ENV: &str = "OAR_CONFIG";
/// Default location of the OAR3 configuration file.
pub const DEFAULT_CONFIG_PATH: &str = "/etc/oar/oar.conf";

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("configuration file {path} could not be read: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error(transparent)]
    Config(#[from] config::ConfigError),
}

/// Effective configuration of every component; each daemon only reads the
/// fields it needs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    // ---- server ------------------------------------------------------------
    pub server_hostname: String,
    pub server_port: u16,
    /// `id=address` pairs (comma/semicolon separated) of the static node list.
    /// A value starting with `@` is read from the given file.
    pub nodes: String,
    /// Default `-l` resource request used when the user gives none.
    pub default_resources: String,
    pub default_walltime_secs: u32,
    pub automaton_wait_secs: u64,
    pub scheduler_timeout_secs: u64,
    pub scheduler_job_security_time_secs: u64,
    pub finaud_frequency_secs: u64,

    // ---- client ------------------------------------------------------------
    pub client_socket: PathBuf,

    // ---- compute node ------------------------------------------------------
    pub node_id: String,
    pub node_address: String,
    pub node_heartbeat_secs: u64,
    pub simulated_job_duration_secs: u64,

    // ---- placeholder database ---------------------------------------------
    pub db_hostname: String,
    pub db_port: u16,
    pub db_base_name: String,
    pub db_login: String,
    pub db_password: String,

    // ---- logging -----------------------------------------------------------
    pub log_level: u8,
    pub log_categories: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server_hostname: "localhost".into(),
            server_port: 6666,
            nodes: String::new(),
            default_resources: "/resource_id=1".into(),
            default_walltime_secs: DEFAULT_WALLTIME_SECS,
            automaton_wait_secs: 1,
            scheduler_timeout_secs: 30,
            scheduler_job_security_time_secs: 60,
            finaud_frequency_secs: 300,
            client_socket: crate::ipc::default_socket_path(),
            node_id: String::new(),
            node_address: "127.0.0.1:7001".into(),
            node_heartbeat_secs: 10,
            simulated_job_duration_secs: 2,
            db_hostname: "127.0.0.1".into(),
            db_port: 5432,
            db_base_name: "oar".into(),
            db_login: "oar".into(),
            db_password: String::new(),
            log_level: 2,
            log_categories: "all".into(),
        }
    }
}

impl Config {
    /// Loads the configuration from `path`, or from [`CONFIG_ENV`] /
    /// [`DEFAULT_CONFIG_PATH`] when `path` is `None`, then applies environment
    /// overrides. A missing file is not an error.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut entries: Vec<(String, String)> = Vec::new();

        let file = path.map(Path::to_path_buf).or_else(|| {
            std::env::var_os(CONFIG_ENV).map(PathBuf::from).or_else(|| {
                Path::new(DEFAULT_CONFIG_PATH)
                    .is_file()
                    .then(|| PathBuf::from(DEFAULT_CONFIG_PATH))
            })
        });

        if let Some(file) = file
            && file.is_file()
        {
            entries.extend(read_conf_file(&file)?);
        }
        entries.extend(environment_entries());

        Self::from_entries(entries)
    }

    /// Loads the configuration without any file, from the environment only.
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_entries(environment_entries())
    }

    /// Applies `(key, value)` pairs (already OAR3-named) on top of the defaults.
    pub fn from_entries(entries: Vec<(String, String)>) -> Result<Self, ConfigError> {
        let mut map: HashMap<String, String> = HashMap::new();
        for (raw_key, value) in entries {
            if let Some(key) = canonical_key(&raw_key) {
                map.insert(key.to_string(), value);
            }
        }
        let source = MapSource(map);
        let built = config::Config::builder().add_source(source).build()?;
        built.try_deserialize::<Config>().map_err(ConfigError::from)
    }

    /// `host:port` of the server's gRPC endpoint.
    pub fn server_address(&self) -> String {
        format!("{}:{}", self.server_hostname, self.server_port)
    }

    /// Path of the client daemon's unix domain socket.
    pub fn socket_path(&self) -> PathBuf {
        std::env::var_os(crate::ipc::SOCKET_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| self.client_socket.clone())
    }

    /// Static node list, parsed from `nodes` (or from the file it points at).
    pub fn node_list(&self) -> Result<Vec<NodeInfo>, ConfigError> {
        let raw = self.nodes.trim();
        if raw.is_empty() {
            return Ok(Vec::new());
        }
        let content = if let Some(path) = raw.strip_prefix('@') {
            std::fs::read_to_string(path.trim()).map_err(|source| ConfigError::Io {
                path: PathBuf::from(path.trim()),
                source,
            })?
        } else {
            raw.to_string()
        };
        Ok(parse_node_list(&content))
    }

    /// Node id used by an `oar-node` daemon: configured, or derived from the
    /// address / hostname.
    pub fn resolved_node_id(&self) -> String {
        if !self.node_id.trim().is_empty() {
            return self.node_id.trim().to_string();
        }
        if let Some(host) = self.node_address.split(':').next()
            && !host.is_empty()
        {
            return host.to_string();
        }
        "node".to_string()
    }

    /// `RUST_LOG`-compatible filter derived from the OAR3 log level.
    pub fn log_filter(&self) -> String {
        match self.log_level {
            0 | 1 => "error".into(),
            2 => "warn,oar=info".into(),
            _ => "info,oar=debug".into(),
        }
    }

    /// Initialises tracing with this configuration (idempotent).
    pub fn init_tracing(&self) {
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(self.log_filter()));
        let _ = tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .try_init();
    }
}

/// Maps an OAR3 key (any case) to the field name of [`Config`]; unknown keys
/// are ignored. Kept explicit so the set of honoured settings is auditable.
fn canonical_key(raw: &str) -> Option<&'static str> {
    let key = raw.trim().to_ascii_lowercase();
    Some(match key.as_str() {
        "server_hostname" => "server_hostname",
        "server_port" => "server_port",
        "nodes" | "node_list" => "nodes",
        "oarsub_default_resources" | "default_resources" => "default_resources",
        "default_walltime" | "default_walltime_secs" => "default_walltime_secs",
        "automaton_wait" | "automaton_wait_secs" => "automaton_wait_secs",
        "scheduler_timeout" | "scheduler_timeout_secs" => "scheduler_timeout_secs",
        "scheduler_job_security_time" | "scheduler_job_security_time_secs" => {
            "scheduler_job_security_time_secs"
        }
        "finaud_frequency" | "finaud_frequency_secs" => "finaud_frequency_secs",
        "client_socket" => "client_socket",
        "node_id" => "node_id",
        "node_address" => "node_address",
        "node_heartbeat" | "node_heartbeat_secs" => "node_heartbeat_secs",
        "simulated_job_duration" | "simulated_job_duration_secs" => "simulated_job_duration_secs",
        "db_hostname" => "db_hostname",
        "db_port" => "db_port",
        "db_base_name" => "db_base_name",
        "db_base_login" | "db_login" => "db_login",
        "db_base_passwd" | "db_base_password" | "db_password" => "db_password",
        "log_level" => "log_level",
        "log_categories" => "log_categories",
        _ => return None,
    })
}

/// Parses an OAR3 configuration file; unreadable/invalid lines are skipped with
/// a warning so one bad line cannot take a daemon down.
fn read_conf_file(path: &Path) -> Result<Vec<(String, String)>, ConfigError> {
    let iter = dotenvy::from_path_iter(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source: std::io::Error::other(source),
    })?;

    let mut entries = Vec::new();
    for item in iter {
        match item {
            Ok(pair) => entries.push(pair),
            Err(error) => {
                tracing::warn!(file = %path.display(), %error, "skipping invalid config line")
            }
        }
    }
    Ok(entries)
}

/// Environment overrides: `OAR_<FIELD>` always wins; a variable literally named
/// like an honoured OAR3 key (`SERVER_PORT`, `SCHEDULER_TIMEOUT`, ...) is
/// accepted too, matching how OAR3 sources its own configuration.
fn environment_entries() -> Vec<(String, String)> {
    let mut entries = Vec::new();
    for (key, value) in std::env::vars() {
        if let Some(rest) = key.strip_prefix("OAR_") {
            entries.push((rest.to_string(), value));
        } else if canonical_key(&key).is_some() {
            entries.push((key, value));
        }
    }
    entries
}

/// Parses `id=address` pairs separated by commas or semicolons, one per line in
/// a file. A bare `address` is used as its own id.
pub fn parse_node_list(input: &str) -> Vec<NodeInfo> {
    input
        .split([',', ';', '\n'])
        .map(str::trim)
        .filter(|item| !item.is_empty() && !item.starts_with('#'))
        .map(|item| match item.split_once('=') {
            Some((id, address)) => NodeInfo::new(id.trim(), address.trim()),
            None => NodeInfo::new(item, item),
        })
        .collect()
}

/// [`config::Source`] adapter over a flat `key -> string` map.
#[derive(Debug, Clone)]
struct MapSource(HashMap<String, String>);

impl config::Source for MapSource {
    fn collect(&self) -> Result<config::Map<String, config::Value>, config::ConfigError> {
        Ok(self
            .0
            .iter()
            .map(|(key, value)| (key.clone(), config::Value::new(None, value.clone())))
            .collect())
    }

    fn clone_into_box(&self) -> Box<dyn config::Source + Send + Sync> {
        Box::new(self.clone())
    }
}

/// Convenience: resolve a node id from an explicit value or [`Config`].
pub fn node_id_or(config: &Config, fallback: impl Into<String>) -> String {
    let resolved = config.resolved_node_id();
    if resolved.is_empty() {
        fallback.into()
    } else {
        resolved
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_conf(contents: &str) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[test]
    fn defaults_are_sane() {
        let config = Config::from_entries(Vec::new()).unwrap();
        assert_eq!(config, Config::default());
        assert_eq!(config.server_address(), "localhost:6666");
        assert_eq!(config.default_walltime_secs, 7200);
    }

    #[test]
    fn oar3_file_is_loaded_and_typed() {
        let file = write_conf(
            r#"
# a comment
SERVER_HOSTNAME="cluster.example.org"
SERVER_PORT="7777"
SCHEDULER_TIMEOUT="42"
OARSUB_DEFAULT_RESOURCES="/resource_id=3"
LOG_LEVEL="3"
DB_BASE_NAME="oartest"
UNKNOWN_KEY="ignored"
"#,
        );
        let config = Config::load(Some(file.path())).unwrap();
        assert_eq!(config.server_hostname, "cluster.example.org");
        assert_eq!(config.server_port, 7777);
        assert_eq!(config.scheduler_timeout_secs, 42);
        assert_eq!(config.default_resources, "/resource_id=3");
        assert_eq!(config.log_level, 3);
        assert_eq!(config.db_base_name, "oartest");
        assert_eq!(config.server_address(), "cluster.example.org:7777");
    }

    #[test]
    fn example_oar_conf_parses() {
        // The shipped example must stay loadable, whatever the OAR3 version.
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../etc/oar.conf");
        if !path.is_file() {
            return;
        }
        let config = Config::load(Some(&path)).unwrap();
        assert_eq!(config.server_port, 6666);
        assert_eq!(config.db_base_name, "oar");
    }

    #[test]
    fn entries_override_defaults_and_ignore_unknown() {
        let config = Config::from_entries(vec![
            ("SERVER_PORT".into(), "9000".into()),
            ("NOT_A_SETTING".into(), "x".into()),
        ])
        .unwrap();
        assert_eq!(config.server_port, 9000);
    }

    #[test]
    fn invalid_values_are_reported() {
        let err = Config::from_entries(vec![("SERVER_PORT".into(), "not-a-port".into())]);
        assert!(matches!(err, Err(ConfigError::Config(_))));
    }

    #[test]
    fn missing_file_is_not_an_error() {
        let config = Config::load(Some(Path::new("/definitely/not/here/oar.conf"))).unwrap();
        assert_eq!(config, Config::default());
    }

    #[test]
    fn node_list_parsing() {
        let nodes = parse_node_list("n1=127.0.0.1:7001, n2=127.0.0.1:7002;n3=127.0.0.1:7003");
        assert_eq!(nodes.len(), 3);
        assert_eq!(nodes[0].id, "n1");
        assert_eq!(nodes[0].address, "127.0.0.1:7001");
        assert_eq!(nodes[2].id, "n3");

        let bare = parse_node_list("node-a");
        assert_eq!(bare[0].id, "node-a");
        assert_eq!(bare[0].address, "node-a");

        assert!(parse_node_list("  \n # comment \n").is_empty());
    }

    #[test]
    fn configured_node_list_is_used() {
        let config = Config::from_entries(vec![(
            "NODES".into(),
            "n1=127.0.0.1:7001,n2=127.0.0.1:7002".into(),
        )])
        .unwrap();
        let nodes = config.node_list().unwrap();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[1].id, "n2");
    }

    #[test]
    fn node_list_from_file() {
        let file = write_conf("n1=127.0.0.1:7001\nn2=127.0.0.1:7002\n");
        let config = Config::from_entries(vec![(
            "NODES".into(),
            format!("@{}", file.path().display()),
        )])
        .unwrap();
        assert_eq!(config.node_list().unwrap().len(), 2);
    }

    #[test]
    fn node_id_resolution() {
        let config = Config {
            node_id: "worker-1".into(),
            ..Config::default()
        };
        assert_eq!(config.resolved_node_id(), "worker-1");

        let config = Config {
            node_address: "10.0.0.5:7001".into(),
            ..Config::default()
        };
        assert_eq!(config.resolved_node_id(), "10.0.0.5");

        let config = Config {
            node_address: String::new(),
            ..Config::default()
        };
        assert_eq!(config.resolved_node_id(), "node");
    }

    #[test]
    fn log_filter_follows_oar_log_level() {
        let mut config = Config {
            log_level: 1,
            ..Config::default()
        };
        assert_eq!(config.log_filter(), "error");
        config.log_level = 2;
        assert!(config.log_filter().contains("warn"));
        config.log_level = 4;
        assert!(config.log_filter().contains("debug"));
    }

    #[test]
    fn canonical_keys_are_case_insensitive() {
        assert_eq!(canonical_key("server_PORT"), Some("server_port"));
        assert_eq!(
            canonical_key("OARSUB_DEFAULT_RESOURCES"),
            Some("default_resources")
        );
        assert_eq!(canonical_key("nope"), None);
    }

    #[test]
    fn oar3_and_own_key_spellings_both_resolve() {
        // Our own `*_secs` names and the OAR3 uppercase ones are equivalent.
        for (oar3, own) in [
            ("AUTOMATON_WAIT", "automaton_wait_secs"),
            ("DEFAULT_WALLTIME", "default_walltime_secs"),
            ("NODE_HEARTBEAT", "node_heartbeat_secs"),
            ("SIMULATED_JOB_DURATION", "simulated_job_duration_secs"),
            ("SCHEDULER_TIMEOUT", "scheduler_timeout_secs"),
            ("FINAUD_FREQUENCY", "finaud_frequency_secs"),
            ("DB_BASE_LOGIN", "db_login"),
            ("DB_BASE_PASSWD", "db_password"),
        ] {
            assert_eq!(canonical_key(oar3), Some(own), "{oar3}");
            assert_eq!(canonical_key(own), Some(own), "{own}");
        }
    }
}
