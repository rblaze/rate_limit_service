use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use thiserror::Error;

use crate::config::{ConfigError, QuotaType, ServiceConfig, TimeUnit};
use crate::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use crate::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::RateLimitOverride;
use crate::envoy::service::ratelimit::v3::rate_limit_response::{
    RateLimit as ResponseRateLimit, rate_limit::Unit as ResponseUnit,
};
use crate::envoy::r#type::v3::RateLimitUnit;

pub trait Clock: Send + Sync + 'static {
    fn now_unix_secs(&self) -> i64;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_secs(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }
}

#[derive(Debug)]
pub struct MockClock {
    unix_secs: AtomicI64,
}

impl MockClock {
    pub fn new(initial_unix_secs: i64) -> Self {
        Self {
            unix_secs: AtomicI64::new(initial_unix_secs),
        }
    }

    pub fn set(&self, unix_secs: i64) {
        self.unix_secs.store(unix_secs, Ordering::SeqCst);
    }

    pub fn advance_secs(&self, delta_secs: i64) {
        self.unix_secs.fetch_add(delta_secs, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_unix_secs(&self) -> i64 {
        self.unix_secs.load(Ordering::SeqCst)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DescriptorError {
    #[error("request domain '{actual}' does not match configured domain '{expected}'")]
    DomainMismatch { expected: String, actual: String },
    #[error("RateLimitRequest must contain at least one descriptor")]
    EmptyDescriptors,
    #[error("descriptor {index} is missing required entry '{key}'")]
    MissingEntry { index: usize, key: &'static str },
    #[error("descriptor {index} has empty value for entry '{key}'")]
    EmptyEntryValue { index: usize, key: &'static str },
    #[error("descriptor {index} has duplicate entry '{key}'")]
    DuplicateEntry { index: usize, key: String },
    #[error("descriptor {index} has unsupported entry key '{key}'")]
    UnknownEntryKey { index: usize, key: String },
    #[error("descriptor {index} has invalid quota_type '{value}', expected 'requests' or 'tokens'")]
    InvalidQuotaType { index: usize, value: String },
    #[error("descriptor {index} has invalid RateLimitOverride: {reason}")]
    InvalidOverride { index: usize, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelKey {
    pub provider: String,
    pub model: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuotaSpec {
    pub quota_type: QuotaType,
    pub limit: u64,
    pub time_unit: TimeUnit,
    pub unit_multiplier: u32,
    pub window_seconds: u64,
    pub bucket_seconds: u64,
    pub num_buckets: usize,
    pub key_prefix: String,
}

impl QuotaSpec {
    pub fn new(
        domain: &str,
        provider: &str,
        model: &str,
        quota_type: QuotaType,
        time_unit: TimeUnit,
        unit_multiplier: u32,
        limit: u64,
    ) -> Result<Self, String> {
        let params = time_unit.window_params(unit_multiplier)?;
        let key_prefix = format!(
            "{domain}:{provider}:{model}:{}:{}{}:",
            quota_type.key_tag(),
            unit_multiplier,
            params.unit_tag
        );
        Ok(Self {
            quota_type,
            limit,
            time_unit,
            unit_multiplier,
            window_seconds: params.window_seconds,
            bucket_seconds: params.bucket_seconds,
            num_buckets: params.num_buckets,
            key_prefix,
        })
    }

    pub fn current_bucket(&self, now_seconds: i64) -> i64 {
        now_seconds.div_euclid(self.bucket_seconds as i64)
    }

    pub fn current_bucket_key(&self, now_seconds: i64) -> String {
        format!("{}{}", self.key_prefix, self.current_bucket(now_seconds))
    }

    pub fn window_keys(&self, now_seconds: i64) -> Vec<String> {
        let current = self.current_bucket(now_seconds);
        (0..self.num_buckets)
            .map(|offset| format!("{}{}", self.key_prefix, current - offset as i64))
            .collect()
    }

    pub fn seconds_until_bucket_rollover(&self, now_seconds: i64) -> i64 {
        let bucket = self.bucket_seconds as i64;
        bucket - now_seconds.rem_euclid(bucket)
    }

    pub fn to_proto_rate_limit(&self) -> ResponseRateLimit {
        let unit = match self.time_unit {
            TimeUnit::Second => ResponseUnit::Second,
            TimeUnit::Minute => ResponseUnit::Minute,
            TimeUnit::Hour => ResponseUnit::Hour,
            TimeUnit::Day => ResponseUnit::Day,
        };
        ResponseRateLimit {
            requests_per_unit: u32::try_from(self.limit).unwrap_or(u32::MAX),
            unit: unit as i32,
            name: self.key_prefix.trim_end_matches(':').to_string(),
            unit_multiplier: Some(self.unit_multiplier),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelQuotas {
    pub disabled: bool,
    pub request_quotas: Vec<QuotaSpec>,
    pub token_quotas: Vec<QuotaSpec>,
}

#[derive(Debug, Clone)]
pub struct QuotaCatalog {
    domain: String,
    models: HashMap<ModelKey, ModelQuotas>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TargetQuotas {
    /// Model or override is unrestricted (no Redis calls needed).
    Unrestricted,
    /// Model or override has limit == 0 (always OVER_LIMIT on check, no-op on consume).
    Disabled {
        current_limit: Option<ResponseRateLimit>,
    },
    /// Active rolling-window quotas to evaluate against Redis.
    Active(Vec<QuotaSpec>),
}

#[derive(Debug, Clone)]
pub struct ResolvedDescriptor {
    pub provider: String,
    pub model: String,
    pub quota_type: QuotaType,
    pub hits_addend: u64,
    pub is_negative_hits: bool,
    pub target: TargetQuotas,
}

impl ResolvedDescriptor {
    pub fn signed_delta(&self) -> i64 {
        let mag = i64::try_from(self.hits_addend).unwrap_or(i64::MAX);
        if self.is_negative_hits { -mag } else { mag }
    }
}

impl QuotaCatalog {
    pub fn from_config(config: &ServiceConfig) -> Result<Self, ConfigError> {
        let mut models = HashMap::new();

        for entry in &config.models {
            if entry.quotas.is_empty() {
                continue;
            }

            let mut model_quotas = ModelQuotas::default();
            for quota in &entry.quotas {
                if quota.limit == 0 {
                    model_quotas.disabled = true;
                }
                let spec = QuotaSpec::new(
                    &config.domain,
                    &entry.provider,
                    &entry.model,
                    quota.quota_type,
                    quota.time_unit,
                    quota.effective_multiplier(),
                    quota.limit,
                )
                .map_err(ConfigError::Validation)?;

                match quota.quota_type {
                    QuotaType::Requests => model_quotas.request_quotas.push(spec),
                    QuotaType::Tokens => model_quotas.token_quotas.push(spec),
                }
            }

            models.insert(
                ModelKey {
                    provider: entry.provider.clone(),
                    model: entry.model.clone(),
                },
                model_quotas,
            );
        }

        Ok(Self {
            domain: config.domain.clone(),
            models,
        })
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn validate_domain(&self, request_domain: &str) -> Result<(), DescriptorError> {
        if request_domain != self.domain {
            return Err(DescriptorError::DomainMismatch {
                expected: self.domain.clone(),
                actual: request_domain.to_string(),
            });
        }
        Ok(())
    }

    pub fn resolve_descriptors(
        &self,
        descriptors: &[RateLimitDescriptor],
        request_hits_addend: u32,
    ) -> Result<Vec<ResolvedDescriptor>, DescriptorError> {
        if descriptors.is_empty() {
            return Err(DescriptorError::EmptyDescriptors);
        }

        descriptors
            .iter()
            .enumerate()
            .map(|(idx, desc)| self.resolve_one(idx, desc, request_hits_addend))
            .collect()
    }

    fn resolve_one(
        &self,
        index: usize,
        descriptor: &RateLimitDescriptor,
        request_hits_addend: u32,
    ) -> Result<ResolvedDescriptor, DescriptorError> {
        let mut provider: Option<&str> = None;
        let mut model: Option<&str> = None;
        let mut quota_type_raw: Option<&str> = None;

        for entry in &descriptor.entries {
            match entry.key.as_str() {
                "provider" => {
                    if provider.is_some() {
                        return Err(DescriptorError::DuplicateEntry {
                            index,
                            key: entry.key.clone(),
                        });
                    }
                    if entry.value.is_empty() {
                        return Err(DescriptorError::EmptyEntryValue {
                            index,
                            key: "provider",
                        });
                    }
                    provider = Some(entry.value.as_str());
                }
                "model" => {
                    if model.is_some() {
                        return Err(DescriptorError::DuplicateEntry {
                            index,
                            key: entry.key.clone(),
                        });
                    }
                    if entry.value.is_empty() {
                        return Err(DescriptorError::EmptyEntryValue {
                            index,
                            key: "model",
                        });
                    }
                    model = Some(entry.value.as_str());
                }
                "quota_type" => {
                    if quota_type_raw.is_some() {
                        return Err(DescriptorError::DuplicateEntry {
                            index,
                            key: entry.key.clone(),
                        });
                    }
                    if entry.value.is_empty() {
                        return Err(DescriptorError::EmptyEntryValue {
                            index,
                            key: "quota_type",
                        });
                    }
                    quota_type_raw = Some(entry.value.as_str());
                }
                other => {
                    return Err(DescriptorError::UnknownEntryKey {
                        index,
                        key: other.to_string(),
                    });
                }
            }
        }

        let provider = provider.ok_or(DescriptorError::MissingEntry {
            index,
            key: "provider",
        })?;
        let model = model.ok_or(DescriptorError::MissingEntry {
            index,
            key: "model",
        })?;
        let quota_type_str = quota_type_raw.ok_or(DescriptorError::MissingEntry {
            index,
            key: "quota_type",
        })?;
        let quota_type = QuotaType::from_descriptor_value(quota_type_str).ok_or_else(|| {
            DescriptorError::InvalidQuotaType {
                index,
                value: quota_type_str.to_string(),
            }
        })?;

        let hits_addend = descriptor.hits_addend.unwrap_or_else(|| {
            if request_hits_addend > 0 {
                u64::from(request_hits_addend)
            } else {
                1
            }
        });

        let target = if let Some(override_limit) = &descriptor.limit {
            self.resolve_override(index, provider, model, quota_type, override_limit)?
        } else {
            self.resolve_from_catalog(provider, model, quota_type)
        };

        Ok(ResolvedDescriptor {
            provider: provider.to_string(),
            model: model.to_string(),
            quota_type,
            hits_addend,
            is_negative_hits: descriptor.is_negative_hits,
            target,
        })
    }

    fn resolve_override(
        &self,
        index: usize,
        provider: &str,
        model: &str,
        quota_type: QuotaType,
        override_limit: &RateLimitOverride,
    ) -> Result<TargetQuotas, DescriptorError> {
        let unit = RateLimitUnit::try_from(override_limit.unit).unwrap_or(RateLimitUnit::Unknown);
        let time_unit = match unit {
            RateLimitUnit::Second => TimeUnit::Second,
            RateLimitUnit::Minute => TimeUnit::Minute,
            RateLimitUnit::Hour => TimeUnit::Hour,
            RateLimitUnit::Day => TimeUnit::Day,
            other => {
                return Err(DescriptorError::InvalidOverride {
                    index,
                    reason: format!(
                        "unsupported RateLimitUnit '{:?}', expected SECOND, MINUTE, HOUR, or DAY",
                        other
                    ),
                });
            }
        };

        let multiplier = match override_limit.unit_multiplier {
            Some(0) => {
                return Err(DescriptorError::InvalidOverride {
                    index,
                    reason: "unit_multiplier must be greater than 0".to_string(),
                });
            }
            Some(m) => m,
            None => 1,
        };

        let spec = QuotaSpec::new(
            &self.domain,
            provider,
            model,
            quota_type,
            time_unit,
            multiplier,
            u64::from(override_limit.requests_per_unit),
        )
        .map_err(|reason| DescriptorError::InvalidOverride { index, reason })?;

        if override_limit.requests_per_unit == 0 {
            return Ok(TargetQuotas::Disabled {
                current_limit: Some(spec.to_proto_rate_limit()),
            });
        }

        Ok(TargetQuotas::Active(vec![spec]))
    }

    fn resolve_from_catalog(
        &self,
        provider: &str,
        model: &str,
        quota_type: QuotaType,
    ) -> TargetQuotas {
        let key = ModelKey {
            provider: provider.to_string(),
            model: model.to_string(),
        };
        let Some(model_quotas) = self.models.get(&key) else {
            return TargetQuotas::Unrestricted;
        };

        if model_quotas.disabled {
            let zero_limit = model_quotas
                .request_quotas
                .iter()
                .chain(model_quotas.token_quotas.iter())
                .find(|s| s.limit == 0)
                .map(QuotaSpec::to_proto_rate_limit);
            return TargetQuotas::Disabled {
                current_limit: zero_limit,
            };
        }

        let specs = match quota_type {
            QuotaType::Requests => &model_quotas.request_quotas,
            QuotaType::Tokens => &model_quotas.token_quotas,
        };

        if specs.is_empty() {
            TargetQuotas::Unrestricted
        } else {
            TargetQuotas::Active(specs.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::Entry;

    fn make_descriptor(
        provider: &str,
        model: &str,
        quota_type: &str,
        hits_addend: Option<u64>,
        override_limit: Option<RateLimitOverride>,
    ) -> RateLimitDescriptor {
        RateLimitDescriptor {
            entries: vec![
                Entry {
                    key: "provider".to_string(),
                    value: provider.to_string(),
                },
                Entry {
                    key: "model".to_string(),
                    value: model.to_string(),
                },
                Entry {
                    key: "quota_type".to_string(),
                    value: quota_type.to_string(),
                },
            ],
            limit: override_limit,
            hits_addend,
            is_negative_hits: false,
        }
    }

    #[test]
    fn matches_envoy_redis_quota_key_layout_and_windows() {
        let now_secs = 1_080_000_i64; // Matches RedisQuotaTest simulated timestamp (18000 * 60)

        let spec_1h = QuotaSpec::new(
            "model_router",
            "anthropic",
            "claude-sonnet-4-5",
            QuotaType::Requests,
            TimeUnit::Hour,
            1,
            2,
        )
        .unwrap();
        assert_eq!(spec_1h.window_seconds, 3600);
        assert_eq!(spec_1h.bucket_seconds, 60);
        assert_eq!(spec_1h.num_buckets, 60);
        assert_eq!(
            spec_1h.current_bucket_key(now_secs),
            "model_router:anthropic:claude-sonnet-4-5:req:1h:18000"
        );

        let spec_6h = QuotaSpec::new(
            "model_router",
            "anthropic",
            "claude-sonnet-4-5",
            QuotaType::Requests,
            TimeUnit::Hour,
            6,
            2,
        )
        .unwrap();
        assert_eq!(spec_6h.window_seconds, 21600);
        assert_eq!(spec_6h.bucket_seconds, 360);
        assert_eq!(spec_6h.num_buckets, 60);
        assert_eq!(
            spec_6h.current_bucket_key(now_secs),
            "model_router:anthropic:claude-sonnet-4-5:req:6h:3000"
        );

        let spec_1d = QuotaSpec::new(
            "model_router",
            "anthropic",
            "claude-sonnet-4-5",
            QuotaType::Tokens,
            TimeUnit::Day,
            1,
            86400,
        )
        .unwrap();
        assert_eq!(spec_1d.window_seconds, 86400);
        assert_eq!(spec_1d.bucket_seconds, 900);
        assert_eq!(spec_1d.num_buckets, 96);
        assert_eq!(
            spec_1d.current_bucket_key(now_secs),
            "model_router:anthropic:claude-sonnet-4-5:tok:1d:1200"
        );
    }

    #[test]
    fn resolves_hits_addend_and_rate_limit_overrides() {
        let cfg = ServiceConfig::from_yaml_str(
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
"#,
        )
        .unwrap();
        let catalog = QuotaCatalog::from_config(&cfg).unwrap();

        // Request-level hits_addend fallback when descriptor hits_addend is None.
        let resolved = catalog
            .resolve_descriptors(
                &[make_descriptor("openai", "gpt-4o", "requests", None, None)],
                5,
            )
            .unwrap();
        assert_eq!(resolved[0].hits_addend, 5);

        // Default to 1 when both descriptor and request hits_addend are unset/0.
        let resolved = catalog
            .resolve_descriptors(
                &[make_descriptor("openai", "gpt-4o", "requests", None, None)],
                0,
            )
            .unwrap();
        assert_eq!(resolved[0].hits_addend, 1);

        // RateLimitOverride supersedes catalog config.
        let override_desc = make_descriptor(
            "openai",
            "gpt-4o",
            "requests",
            Some(2),
            Some(RateLimitOverride {
                requests_per_unit: 25,
                unit: RateLimitUnit::Second as i32,
                unit_multiplier: Some(10),
            }),
        );
        let resolved = catalog.resolve_descriptors(&[override_desc], 0).unwrap();
        assert_eq!(resolved[0].hits_addend, 2);
        match &resolved[0].target {
            TargetQuotas::Active(specs) => {
                assert_eq!(specs.len(), 1);
                assert_eq!(specs[0].limit, 25);
                assert_eq!(specs[0].window_seconds, 10);
                assert_eq!(specs[0].num_buckets, 10);
                assert_eq!(specs[0].key_prefix, "model_router:openai:gpt-4o:req:10s:");
            }
            other => panic!("expected Active override, got {other:?}"),
        }
    }
}
