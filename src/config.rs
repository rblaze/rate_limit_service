use std::collections::HashSet;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;
use serde::de::{self, Deserializer};
use thiserror::Error;

pub const DEFAULT_LISTEN_ADDR: &str = "127.0.0.1:18081";
pub const DEFAULT_DOMAIN: &str = "model_router";
pub const DEFAULT_REDIS_PORT: u16 = 6379;
pub const DEFAULT_REDIS_TIMEOUT_MS: u64 = 1000;
pub const MAX_SECOND_MULTIPLIER: u32 = 3600;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse YAML config: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    #[error("invalid configuration: {0}")]
    Validation(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuotaType {
    Requests,
    Tokens,
}

impl QuotaType {
    pub fn key_tag(self) -> &'static str {
        match self {
            Self::Requests => "req",
            Self::Tokens => "tok",
        }
    }

    pub fn from_descriptor_value(value: &str) -> Option<Self> {
        match value {
            "requests" => Some(Self::Requests),
            "tokens" => Some(Self::Tokens),
            _ => None,
        }
    }
}

impl fmt::Display for QuotaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Requests => f.write_str("requests"),
            Self::Tokens => f.write_str("tokens"),
        }
    }
}

impl<'de> Deserialize<'de> for QuotaType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        match raw.trim().to_ascii_uppercase().as_str() {
            "REQUESTS" => Ok(Self::Requests),
            "TOKENS" => Ok(Self::Tokens),
            _ => Err(de::Error::custom(format!(
                "unknown quota_type '{raw}', expected REQUESTS or TOKENS"
            ))),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Second,
    Minute,
    Hour,
    Day,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowParams {
    pub window_seconds: u64,
    pub bucket_seconds: u64,
    pub num_buckets: usize,
    pub unit_tag: &'static str,
}

impl TimeUnit {
    pub fn window_params(self, multiplier: u32) -> Result<WindowParams, String> {
        if multiplier == 0 {
            return Err("unit_multiplier must be greater than 0".to_string());
        }
        let mult = u64::from(multiplier);
        match self {
            Self::Second => {
                if multiplier > MAX_SECOND_MULTIPLIER {
                    return Err(format!(
                        "unit_multiplier {multiplier} for SECOND exceeds maximum {MAX_SECOND_MULTIPLIER}; use MINUTE or HOUR instead"
                    ));
                }
                Ok(WindowParams {
                    window_seconds: mult,
                    bucket_seconds: 1,
                    num_buckets: multiplier as usize,
                    unit_tag: "s",
                })
            }
            Self::Minute => Ok(WindowParams {
                window_seconds: 60 * mult,
                bucket_seconds: mult,
                num_buckets: 60,
                unit_tag: "m",
            }),
            Self::Hour => Ok(WindowParams {
                window_seconds: 3600 * mult,
                bucket_seconds: 60 * mult,
                num_buckets: 60,
                unit_tag: "h",
            }),
            Self::Day => Ok(WindowParams {
                window_seconds: 86400 * mult,
                bucket_seconds: 900 * mult,
                num_buckets: 96,
                unit_tag: "d",
            }),
        }
    }
}

impl fmt::Display for TimeUnit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Second => f.write_str("SECOND"),
            Self::Minute => f.write_str("MINUTE"),
            Self::Hour => f.write_str("HOUR"),
            Self::Day => f.write_str("DAY"),
        }
    }
}

impl<'de> Deserialize<'de> for TimeUnit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        match raw.trim().to_ascii_uppercase().as_str() {
            "SECOND" => Ok(Self::Second),
            "MINUTE" => Ok(Self::Minute),
            "HOUR" => Ok(Self::Hour),
            "DAY" => Ok(Self::Day),
            _ => Err(de::Error::custom(format!(
                "unsupported time_unit '{raw}', expected SECOND, MINUTE, HOUR, or DAY"
            ))),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    #[serde(default = "default_listen_addr")]
    pub listen: SocketAddr,

    #[serde(default = "default_domain")]
    pub domain: String,

    pub redis: RedisConfig,

    #[serde(default)]
    pub models: Vec<ModelConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    pub host: String,

    #[serde(default = "default_redis_port")]
    pub port: u16,

    #[serde(
        default = "default_redis_timeout",
        deserialize_with = "deserialize_duration"
    )]
    pub timeout: Duration,
}

impl RedisConfig {
    pub fn effective_port(&self) -> u16 {
        if self.port == 0 {
            DEFAULT_REDIS_PORT
        } else {
            self.port
        }
    }

    pub fn connection_url(&self) -> String {
        let port = self.effective_port();
        let host = self.host.trim();
        if host.contains(':') && !host.starts_with('[') {
            format!("redis://[{host}]:{port}/")
        } else {
            format!("redis://{host}:{port}/")
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    /// Optional alias matching Envoy's `Model.name` for convenient config reuse.
    #[serde(default)]
    pub name: Option<String>,

    pub provider: String,
    pub model: String,

    #[serde(default)]
    pub quotas: Vec<QuotaConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaConfig {
    pub quota_type: QuotaType,
    pub time_unit: TimeUnit,
    pub limit: u64,
    #[serde(default)]
    pub unit_multiplier: Option<u32>,
}

impl QuotaConfig {
    pub fn effective_multiplier(&self) -> u32 {
        self.unit_multiplier.unwrap_or(1)
    }
}

fn default_listen_addr() -> SocketAddr {
    DEFAULT_LISTEN_ADDR
        .parse()
        .expect("DEFAULT_LISTEN_ADDR is valid")
}

fn default_domain() -> String {
    DEFAULT_DOMAIN.to_string()
}

fn default_redis_port() -> u16 {
    DEFAULT_REDIS_PORT
}

fn default_redis_timeout() -> Duration {
    Duration::from_millis(DEFAULT_REDIS_TIMEOUT_MS)
}

fn parse_duration_str(raw: &str) -> Result<Duration, String> {
    let s = raw.trim();
    if s.is_empty() {
        return Err("duration string cannot be empty".to_string());
    }

    let (num_str, factor_secs) = if let Some(v) = s.strip_suffix("ms") {
        (v.trim(), 0.001_f64)
    } else if let Some(v) = s.strip_suffix('s') {
        (v.trim(), 1.0_f64)
    } else if let Some(v) = s.strip_suffix('m') {
        (v.trim(), 60.0_f64)
    } else if let Some(v) = s.strip_suffix('h') {
        (v.trim(), 3600.0_f64)
    } else {
        return Err(format!(
            "invalid duration '{s}', expected unit suffix such as 'ms', 's', 'm', or 'h'"
        ));
    };

    let value: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid numeric value in duration '{s}'"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("duration '{s}' must be non-negative"));
    }

    Duration::try_from_secs_f64(value * factor_secs)
        .map_err(|_| format!("duration '{s}' is out of range"))
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let raw = String::deserialize(deserializer)?;
    parse_duration_str(&raw).map_err(de::Error::custom)
}

impl ServiceConfig {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path_ref = path.as_ref();
        let contents = fs::read_to_string(path_ref).map_err(|source| ConfigError::Io {
            path: path_ref.display().to_string(),
            source,
        })?;
        Self::from_yaml_str(&contents)
    }

    pub fn from_yaml_str(yaml: &str) -> Result<Self, ConfigError> {
        let config: Self = serde_yaml_ng::from_str(yaml)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.domain.trim().is_empty() {
            return Err(ConfigError::Validation(
                "domain must not be empty".to_string(),
            ));
        }
        if self.redis.host.trim().is_empty() {
            return Err(ConfigError::Validation(
                "redis.host must not be empty".to_string(),
            ));
        }
        if self.redis.timeout.is_zero() {
            return Err(ConfigError::Validation(
                "redis.timeout must be greater than 0s".to_string(),
            ));
        }

        let mut seen_models = HashSet::new();
        let mut seen_names = HashSet::new();
        for entry in &self.models {
            if let Some(name) = &entry.name {
                if name.trim().is_empty() {
                    return Err(ConfigError::Validation(
                        "model name, when provided, must not be empty".to_string(),
                    ));
                }
                if !seen_names.insert(name.clone()) {
                    return Err(ConfigError::Validation(format!(
                        "duplicate model name '{name}'"
                    )));
                }
            }
            if entry.provider.trim().is_empty() {
                return Err(ConfigError::Validation(
                    "model provider must not be empty".to_string(),
                ));
            }
            if entry.model.trim().is_empty() {
                return Err(ConfigError::Validation(format!(
                    "model identifier for provider '{}' must not be empty",
                    entry.provider
                )));
            }
            let key = (entry.provider.clone(), entry.model.clone());
            if !seen_models.insert(key) {
                return Err(ConfigError::Validation(format!(
                    "duplicate quota configuration for provider='{}', model='{}'",
                    entry.provider, entry.model
                )));
            }

            for quota in &entry.quotas {
                if let Some(0) = quota.unit_multiplier {
                    return Err(ConfigError::Validation(format!(
                        "unit_multiplier for {}:{} must be greater than 0",
                        entry.provider, entry.model
                    )));
                }
                quota
                    .time_unit
                    .window_params(quota.effective_multiplier())
                    .map_err(ConfigError::Validation)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_valid_config_and_applies_defaults() {
        let yaml = r#"
redis:
  host: 127.0.0.1
models:
  - name: gpt4o
    provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
      - quota_type: tokens
        time_unit: hour
        limit: 50000
        unit_multiplier: 6
  - provider: anthropic
    model: claude-haiku-4-5
    quotas:
      - quota_type: requests
        time_unit: second
        limit: 5
        unit_multiplier: 10
"#;
        let cfg = ServiceConfig::from_yaml_str(yaml).expect("valid config");
        assert_eq!(cfg.domain, "model_router");
        assert_eq!(cfg.listen, "127.0.0.1:18081".parse().unwrap());
        assert_eq!(cfg.redis.effective_port(), 6379);
        assert_eq!(cfg.redis.timeout, Duration::from_secs(1));
        assert_eq!(cfg.models.len(), 2);
        assert_eq!(cfg.models[0].quotas[0].effective_multiplier(), 1);
        assert_eq!(cfg.models[0].quotas[1].effective_multiplier(), 6);
        assert_eq!(cfg.models[1].quotas[0].time_unit, TimeUnit::Second);
    }

    #[test]
    fn formats_ipv4_and_ipv6_redis_urls() {
        let v4 = RedisConfig {
            host: "127.0.0.1".to_string(),
            port: 0,
            timeout: Duration::from_secs(1),
        };
        assert_eq!(v4.connection_url(), "redis://127.0.0.1:6379/");

        let v6 = RedisConfig {
            host: "::1".to_string(),
            port: 6380,
            timeout: Duration::from_secs(1),
        };
        assert_eq!(v6.connection_url(), "redis://[::1]:6380/");
    }

    #[test]
    fn rejects_invalid_configs() {
        let cases = [
            (
                r#"
redis:
  host: ""
"#,
                "redis.host",
            ),
            (
                r#"
redis:
  host: 127.0.0.1
  timeout: 0s
"#,
                "redis.timeout",
            ),
            (
                r#"
domain: ""
redis:
  host: 127.0.0.1
"#,
                "domain",
            ),
            (
                r#"
redis:
  host: 127.0.0.1
models:
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 10
        unit_multiplier: 0
"#,
                "unit_multiplier",
            ),
            (
                r#"
redis:
  host: 127.0.0.1
models:
  - provider: openai
    model: gpt-4o
  - provider: openai
    model: gpt-4o
"#,
                "duplicate quota configuration",
            ),
            (
                r#"
redis:
  host: 127.0.0.1
models:
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MONTH
        limit: 10
"#,
                "unsupported time_unit",
            ),
        ];

        for (yaml, expected_substr) in cases {
            let err = ServiceConfig::from_yaml_str(yaml).expect_err("should fail");
            assert!(
                err.to_string().contains(expected_substr),
                "expected '{expected_substr}' in '{err}'"
            );
        }
    }
}
