use std::time::Duration;

use redis::ErrorKind;
use redis::aio::ConnectionManager;
use thiserror::Error;
use tokio::sync::RwLock;
use tonic::Status;

use crate::config::RedisConfig;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("Redis operation timed out after {0:?}")]
    Timeout(Duration),
    #[error("Redis connection unavailable: {0}")]
    Unavailable(String),
    #[error("Redis protocol/data error: {0}")]
    Internal(String),
}

impl From<StoreError> for Status {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::Timeout(_) => Status::deadline_exceeded(err.to_string()),
            StoreError::Unavailable(_) => Status::unavailable(err.to_string()),
            StoreError::Internal(_) => Status::internal(err.to_string()),
        }
    }
}

/// Represents a single bucket counter increment + TTL update for `ConsumeRateLimit`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketIncrement {
    pub key: String,
    pub delta: i64,
    pub expire_seconds: u64,
}

pub struct RedisQuotaStore {
    client: redis::Client,
    timeout: Duration,
    connection: RwLock<Option<ConnectionManager>>,
}

impl RedisQuotaStore {
    pub fn new(config: &RedisConfig) -> Result<Self, StoreError> {
        let url = config.connection_url();
        let client = redis::Client::open(url).map_err(|e| StoreError::Internal(e.to_string()))?;
        Ok(Self {
            client,
            timeout: config.timeout,
            connection: RwLock::new(None),
        })
    }

    async fn get_connection(&self) -> Result<ConnectionManager, StoreError> {
        if let Some(existing) = self.connection.read().await.as_ref() {
            return Ok(existing.clone());
        }

        let mut guard = self.connection.write().await;
        if let Some(existing) = guard.as_ref() {
            return Ok(existing.clone());
        }

        let manager =
            tokio::time::timeout(self.timeout, ConnectionManager::new(self.client.clone()))
                .await
                .map_err(|_| StoreError::Timeout(self.timeout))?
                .map_err(|e| StoreError::Unavailable(e.to_string()))?;

        *guard = Some(manager.clone());
        Ok(manager)
    }

    async fn invalidate_connection(&self) {
        let mut guard = self.connection.write().await;
        *guard = None;
    }

    fn classify_redis_error(err: redis::RedisError) -> StoreError {
        if err.is_io_error()
            || err.is_connection_dropped()
            || err.is_connection_refusal()
            || matches!(
                err.kind(),
                ErrorKind::IoError | ErrorKind::BusyLoadingError | ErrorKind::TryAgain
            )
        {
            StoreError::Unavailable(err.to_string())
        } else if err.is_timeout() {
            StoreError::Unavailable(format!("Redis timed out: {err}"))
        } else {
            StoreError::Internal(err.to_string())
        }
    }

    /// Fetches integer values for all `keys` in a single `MGET` call.
    /// Missing (`nil`) keys evaluate to `0`.
    pub async fn mget_counts(&self, keys: &[String]) -> Result<Vec<i64>, StoreError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut conn = self.get_connection().await?;
        let raw_values: Result<Vec<Option<i64>>, redis::RedisError> = tokio::time::timeout(
            self.timeout,
            redis::cmd("MGET").arg(keys).query_async(&mut conn),
        )
        .await
        .map_err(|_| StoreError::Timeout(self.timeout))?;

        match raw_values {
            Ok(vals) => Ok(vals.into_iter().map(|v| v.unwrap_or(0)).collect()),
            Err(err) => {
                let classified = Self::classify_redis_error(err);
                if matches!(classified, StoreError::Unavailable(_)) {
                    self.invalidate_connection().await;
                }
                Err(classified)
            }
        }
    }

    /// Atomically pipelines `INCRBY` and `EXPIRE` for all bucket increments in a single round-trip.
    pub async fn consume_buckets(&self, increments: &[BucketIncrement]) -> Result<(), StoreError> {
        if increments.is_empty() {
            return Ok(());
        }

        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        for inc in increments {
            pipe.cmd("INCRBY")
                .arg(&inc.key)
                .arg(inc.delta)
                .ignore()
                .cmd("EXPIRE")
                .arg(&inc.key)
                .arg(inc.expire_seconds)
                .ignore();
        }

        let res: Result<(), redis::RedisError> =
            tokio::time::timeout(self.timeout, pipe.query_async(&mut conn))
                .await
                .map_err(|_| StoreError::Timeout(self.timeout))?;

        match res {
            Ok(()) => Ok(()),
            Err(err) => {
                let classified = Self::classify_redis_error(err);
                if matches!(classified, StoreError::Unavailable(_)) {
                    self.invalidate_connection().await;
                }
                Err(classified)
            }
        }
    }
}
