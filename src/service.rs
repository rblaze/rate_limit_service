use std::sync::Arc;

use prost_types::Duration as ProtoDuration;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::config::{ConfigError, ServiceConfig};
use crate::envoy::service::ratelimit::v3::rate_limit_response::{Code, DescriptorStatus};
use crate::envoy::service::ratelimit::v3::rate_limit_service_server::RateLimitService;
use crate::envoy::service::ratelimit::v3::{RateLimitRequest, RateLimitResponse};
use crate::quota::{
    Clock, DescriptorError, QuotaCatalog, QuotaSpec, ResolvedDescriptor, SystemClock, TargetQuotas,
};
use crate::redis_store::{BucketIncrement, RedisQuotaStore, StoreError};

pub struct RlsProxyService<C: Clock = SystemClock> {
    catalog: Arc<QuotaCatalog>,
    store: Arc<RedisQuotaStore>,
    clock: Arc<C>,
}

impl RlsProxyService<SystemClock> {
    pub fn from_config(config: &ServiceConfig) -> Result<Self, anyhow::Error> {
        Self::with_clock(config, Arc::new(SystemClock))
    }
}

impl<C: Clock> RlsProxyService<C> {
    pub fn with_clock(config: &ServiceConfig, clock: Arc<C>) -> Result<Self, anyhow::Error> {
        let catalog = QuotaCatalog::from_config(config).map_err(anyhow::Error::new)?;
        let store = RedisQuotaStore::new(&config.redis).map_err(anyhow::Error::new)?;
        Ok(Self {
            catalog: Arc::new(catalog),
            store: Arc::new(store),
            clock,
        })
    }

    pub fn new(catalog: Arc<QuotaCatalog>, store: Arc<RedisQuotaStore>, clock: Arc<C>) -> Self {
        Self {
            catalog,
            store,
            clock,
        }
    }

    fn validate_and_resolve(
        &self,
        req: &RateLimitRequest,
    ) -> Result<Vec<ResolvedDescriptor>, DescriptorError> {
        self.catalog.validate_domain(&req.domain)?;
        self.catalog
            .resolve_descriptors(&req.descriptors, req.hits_addend)
    }
}

fn evaluate_active_specs(
    specs: &[QuotaSpec],
    bucket_values: &[i64],
    mut offset: usize,
    desc: &ResolvedDescriptor,
    now_secs: i64,
) -> (DescriptorStatus, usize) {
    let delta = i128::from(desc.signed_delta());
    let mut best_breached: Option<(&QuotaSpec, i128)> = None;
    let mut tightest_ok: Option<(&QuotaSpec, u64)> = None;

    for spec in specs {
        let used: i128 = bucket_values[offset..offset + spec.num_buckets]
            .iter()
            .copied()
            .map(i128::from)
            .sum();
        offset += spec.num_buckets;

        let limit = i128::from(spec.limit);
        let projected = used + delta;
        if projected > limit {
            if best_breached.is_none() {
                best_breached = Some((spec, used));
            }
        } else {
            let remaining = u64::try_from((limit - used.max(0)).max(0)).unwrap_or(u64::MAX);
            match tightest_ok {
                None => tightest_ok = Some((spec, remaining)),
                Some((_, prev_rem)) if remaining < prev_rem => {
                    tightest_ok = Some((spec, remaining));
                }
                _ => {}
            }
        }
    }

    if let Some((spec, used)) = best_breached {
        let limit_i128 = i128::from(spec.limit);
        let remaining_u64 = u64::try_from((limit_i128 - used.max(0)).max(0)).unwrap_or(0);
        debug!(
            provider = %desc.provider,
            model = %desc.model,
            quota_type = %desc.quota_type,
            used = %used,
            hits_addend = desc.hits_addend,
            limit = spec.limit,
            window_s = spec.window_seconds,
            "quota check OVER_LIMIT"
        );
        let status = DescriptorStatus {
            code: Code::OverLimit as i32,
            current_limit: Some(spec.to_proto_rate_limit()),
            limit_remaining: u32::try_from(remaining_u64).unwrap_or(u32::MAX),
            duration_until_reset: Some(ProtoDuration {
                seconds: spec.seconds_until_bucket_rollover(now_secs),
                nanos: 0,
            }),
            quota: None,
            shadow_over_limit: false,
        };
        (status, offset)
    } else {
        let (spec, remaining_u64) = tightest_ok.expect("active specs is non-empty");
        let status = DescriptorStatus {
            code: Code::Ok as i32,
            current_limit: Some(spec.to_proto_rate_limit()),
            limit_remaining: u32::try_from(remaining_u64).unwrap_or(u32::MAX),
            duration_until_reset: Some(ProtoDuration {
                seconds: spec.seconds_until_bucket_rollover(now_secs),
                nanos: 0,
            }),
            quota: None,
            shadow_over_limit: false,
        };
        (status, offset)
    }
}

#[tonic::async_trait]
impl<C: Clock> RateLimitService for RlsProxyService<C> {
    async fn should_rate_limit(
        &self,
        _request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        Err(Status::unimplemented(
            "ShouldRateLimit is not implemented; use CheckRateLimit and ConsumeRateLimit",
        ))
    }

    async fn check_rate_limit(
        &self,
        request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        let req = request.into_inner();
        let resolved = self
            .validate_and_resolve(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let now_secs = self.clock.now_unix_secs();

        let total_keys: usize = resolved
            .iter()
            .map(|d| match &d.target {
                TargetQuotas::Active(specs) => specs.iter().map(|s| s.num_buckets).sum(),
                _ => 0,
            })
            .sum();

        let bucket_values = if total_keys > 0 {
            let mut keys = Vec::with_capacity(total_keys);
            for desc in &resolved {
                if let TargetQuotas::Active(specs) = &desc.target {
                    for spec in specs {
                        keys.extend(spec.window_keys(now_secs));
                    }
                }
            }
            self.store.mget_counts(&keys).await.map_err(|err| {
                warn!(error = %err, "Redis MGET failed during CheckRateLimit");
                Status::from(err)
            })?
        } else {
            Vec::new()
        };

        let mut offset = 0;
        let mut overall_code = Code::Ok;
        let mut statuses = Vec::with_capacity(resolved.len());

        for desc in &resolved {
            let status = match &desc.target {
                TargetQuotas::Unrestricted => DescriptorStatus {
                    code: Code::Ok as i32,
                    current_limit: None,
                    limit_remaining: u32::MAX,
                    duration_until_reset: None,
                    quota: None,
                    shadow_over_limit: false,
                },
                TargetQuotas::Disabled { current_limit } => {
                    overall_code = Code::OverLimit;
                    DescriptorStatus {
                        code: Code::OverLimit as i32,
                        current_limit: current_limit.clone(),
                        limit_remaining: 0,
                        duration_until_reset: None,
                        quota: None,
                        shadow_over_limit: false,
                    }
                }
                TargetQuotas::Active(specs) => {
                    let (st, next_offset) =
                        evaluate_active_specs(specs, &bucket_values, offset, desc, now_secs);
                    offset = next_offset;
                    if st.code == Code::OverLimit as i32 {
                        overall_code = Code::OverLimit;
                    }
                    st
                }
            };
            statuses.push(status);
        }

        info!(
            domain = %req.domain,
            descriptors = resolved.len(),
            result = ?overall_code,
            "CheckRateLimit completed"
        );

        Ok(Response::new(RateLimitResponse {
            overall_code: overall_code as i32,
            statuses,
            response_headers_to_add: Vec::new(),
            request_headers_to_add: Vec::new(),
            raw_body: Vec::new(),
            dynamic_metadata: None,
            quota: None,
        }))
    }

    async fn consume_rate_limit(
        &self,
        request: Request<RateLimitRequest>,
    ) -> Result<Response<RateLimitResponse>, Status> {
        let req = request.into_inner();
        let resolved = self
            .validate_and_resolve(&req)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let now_secs = self.clock.now_unix_secs();

        let mut increments = Vec::new();
        let mut statuses = Vec::with_capacity(resolved.len());

        for desc in &resolved {
            let delta = desc.signed_delta();
            if let TargetQuotas::Active(specs) = &desc.target
                && delta != 0
            {
                for spec in specs {
                    increments.push(BucketIncrement {
                        key: spec.current_bucket_key(now_secs),
                        delta,
                        expire_seconds: spec.window_seconds,
                    });
                }
            }

            let current_limit = match &desc.target {
                TargetQuotas::Active(specs) => specs.first().map(QuotaSpec::to_proto_rate_limit),
                TargetQuotas::Disabled { current_limit } => current_limit.clone(),
                TargetQuotas::Unrestricted => None,
            };

            statuses.push(DescriptorStatus {
                code: Code::Ok as i32,
                current_limit,
                limit_remaining: 0,
                duration_until_reset: None,
                quota: None,
                shadow_over_limit: false,
            });
        }

        if !increments.is_empty() {
            self.store
                .consume_buckets(&increments)
                .await
                .map_err(|err: StoreError| {
                    warn!(error = %err, "Redis pipeline failed during ConsumeRateLimit");
                    Status::from(err)
                })?;
        }

        info!(
            domain = %req.domain,
            descriptors = resolved.len(),
            updated_buckets = increments.len(),
            "ConsumeRateLimit completed"
        );

        Ok(Response::new(RateLimitResponse {
            overall_code: Code::Ok as i32,
            statuses,
            response_headers_to_add: Vec::new(),
            request_headers_to_add: Vec::new(),
            raw_body: Vec::new(),
            dynamic_metadata: None,
            quota: None,
        }))
    }
}

/// Re-export config error for callers constructing the service directly.
pub type BuildServiceError = ConfigError;
