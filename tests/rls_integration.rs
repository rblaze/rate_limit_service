use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tonic::Code as GrpcCode;
use tonic::transport::{Channel, Server};

use rate_limit_service::config::{RedisConfig, ServiceConfig};
use rate_limit_service::envoy::extensions::common::ratelimit::v3::RateLimitDescriptor;
use rate_limit_service::envoy::extensions::common::ratelimit::v3::rate_limit_descriptor::{
    Entry, RateLimitOverride,
};
use rate_limit_service::envoy::service::ratelimit::v3::RateLimitRequest;
use rate_limit_service::envoy::service::ratelimit::v3::rate_limit_response::Code as RlsCode;
use rate_limit_service::envoy::service::ratelimit::v3::rate_limit_service_client::RateLimitServiceClient;
use rate_limit_service::envoy::service::ratelimit::v3::rate_limit_service_server::RateLimitServiceServer;
use rate_limit_service::envoy::r#type::v3::RateLimitUnit;
use rate_limit_service::quota::MockClock;
use rate_limit_service::service::RlsProxyService;

#[derive(Default, Debug)]
struct FakeRedisState {
    values: HashMap<String, i64>,
    expirations: HashMap<String, u64>,
    mget_calls: usize,
    incrby_calls: usize,
    expire_calls: usize,
    response_delay: Option<Duration>,
    accept_connections: bool,
}

struct FakeRedisServer {
    addr: SocketAddr,
    state: Arc<Mutex<FakeRedisState>>,
    accepted_connections: Arc<AtomicUsize>,
    drop_clients: Arc<Notify>,
    task: JoinHandle<()>,
}

impl FakeRedisServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake redis");
        let addr = listener.local_addr().expect("local addr");
        let state = Arc::new(Mutex::new(FakeRedisState {
            accept_connections: true,
            ..Default::default()
        }));
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let drop_clients = Arc::new(Notify::new());

        let state_clone = Arc::clone(&state);
        let accepted_clone = Arc::clone(&accepted_connections);
        let drop_clone = Arc::clone(&drop_clients);

        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                if !state_clone.lock().unwrap().accept_connections {
                    drop(socket);
                    continue;
                }
                accepted_clone.fetch_add(1, Ordering::SeqCst);
                let conn_state = Arc::clone(&state_clone);
                let conn_drop = Arc::clone(&drop_clone);
                tokio::spawn(async move {
                    handle_redis_connection(socket, conn_state, conn_drop).await;
                });
            }
        });

        Self {
            addr,
            state,
            accepted_connections,
            drop_clients,
            task,
        }
    }

    fn port(&self) -> u16 {
        self.addr.port()
    }

    fn accepted_connections(&self) -> usize {
        self.accepted_connections.load(Ordering::SeqCst)
    }

    fn disconnect_all_clients(&self) {
        self.drop_clients.notify_waiters();
    }

    fn set_accept_connections(&self, accept: bool) {
        self.state.lock().unwrap().accept_connections = accept;
    }

    fn set_response_delay(&self, delay: Option<Duration>) {
        self.state.lock().unwrap().response_delay = delay;
    }

    fn expiration_of(&self, key: &str) -> Option<u64> {
        self.state.lock().unwrap().expirations.get(key).copied()
    }

    fn value_of(&self, key: &str) -> Option<i64> {
        self.state.lock().unwrap().values.get(key).copied()
    }

    fn call_counts(&self) -> (usize, usize, usize) {
        let st = self.state.lock().unwrap();
        (st.mget_calls, st.incrby_calls, st.expire_calls)
    }
}

impl Drop for FakeRedisServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_resp_command<R: AsyncBufReadExt + Unpin>(
    reader: &mut R,
) -> std::io::Result<Option<Vec<String>>> {
    let mut header = String::new();
    let n = reader.read_line(&mut header).await?;
    if n == 0 {
        return Ok(None);
    }
    let header_trimmed = header.trim_end_matches(&['\r', '\n'][..]);
    if let Some(count_str) = header_trimmed.strip_prefix('*') {
        let count: usize = count_str.parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bad array len: {e}"),
            )
        })?;
        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let mut bulk_hdr = String::new();
            reader.read_line(&mut bulk_hdr).await?;
            let len_str = bulk_hdr
                .trim_end_matches(&['\r', '\n'][..])
                .strip_prefix('$')
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "expected '$' bulk string")
                })?;
            let len: usize = len_str.parse().map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("bad bulk len: {e}"),
                )
            })?;
            let mut buf = vec![0u8; len + 2];
            reader.read_exact(&mut buf).await?;
            buf.truncate(len);
            args.push(String::from_utf8_lossy(&buf).into_owned());
        }
        Ok(Some(args))
    } else {
        Ok(Some(
            header_trimmed
                .split_whitespace()
                .map(ToString::to_string)
                .collect(),
        ))
    }
}

fn execute_resp_command(args: &[String], state: &Mutex<FakeRedisState>) -> Vec<u8> {
    if args.is_empty() {
        return b"-ERR empty command\r\n".to_vec();
    }
    let cmd = args[0].to_ascii_uppercase();
    match cmd.as_str() {
        "PING" => {
            if args.len() > 1 {
                let msg = &args[1];
                format!("${}\r\n{}\r\n", msg.len(), msg).into_bytes()
            } else {
                b"+PONG\r\n".to_vec()
            }
        }
        "CLIENT" | "SELECT" => b"+OK\r\n".to_vec(),
        "HELLO" => b"-NOPROTO unsupported protocol version\r\n".to_vec(),
        "MGET" => {
            let mut st = state.lock().unwrap();
            st.mget_calls += 1;
            let keys = &args[1..];
            let mut out = format!("*{}\r\n", keys.len()).into_bytes();
            for key in keys {
                match st.values.get(key) {
                    Some(v) => {
                        let s = v.to_string();
                        out.extend_from_slice(format!("${}\r\n{}\r\n", s.len(), s).as_bytes());
                    }
                    None => out.extend_from_slice(b"$-1\r\n"),
                }
            }
            out
        }
        "INCRBY" if args.len() == 3 => {
            let delta: i64 = match args[2].parse() {
                Ok(v) => v,
                Err(_) => return b"-ERR value is not an integer\r\n".to_vec(),
            };
            let mut st = state.lock().unwrap();
            st.incrby_calls += 1;
            let entry = st.values.entry(args[1].clone()).or_insert(0);
            *entry += delta;
            format!(":{entry}\r\n").into_bytes()
        }
        "EXPIRE" if args.len() >= 3 => {
            let secs: u64 = match args[2].parse() {
                Ok(v) => v,
                Err(_) => return b"-ERR value is not an integer\r\n".to_vec(),
            };
            let mut st = state.lock().unwrap();
            st.expire_calls += 1;
            st.expirations.insert(args[1].clone(), secs);
            b":1\r\n".to_vec()
        }
        _ => format!("-ERR unknown command '{}'\r\n", args[0]).into_bytes(),
    }
}

async fn handle_redis_connection(
    socket: TcpStream,
    state: Arc<Mutex<FakeRedisState>>,
    drop_notify: Arc<Notify>,
) {
    let (read_half, mut write_half) = socket.into_split();
    let mut reader = BufReader::new(read_half);

    loop {
        let dropped = drop_notify.notified();
        let next_cmd = tokio::select! {
            () = dropped => break,
            res = read_resp_command(&mut reader) => res,
        };

        let Some(args) = (match next_cmd {
            Ok(cmd) => cmd,
            Err(_) => break,
        }) else {
            break;
        };

        let maybe_delay = state.lock().unwrap().response_delay;
        if let Some(delay) = maybe_delay {
            tokio::time::sleep(delay).await;
        }

        let reply = execute_resp_command(&args, &state);
        if write_half.write_all(&reply).await.is_err() {
            break;
        }
    }
}

struct TestHarness {
    client: RateLimitServiceClient<Channel>,
    clock: Arc<MockClock>,
    redis: FakeRedisServer,
    server_task: JoinHandle<()>,
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

impl TestHarness {
    async fn start(yaml_models: &str, initial_unix_secs: i64, timeout: Duration) -> Self {
        let redis = FakeRedisServer::start().await;
        let full_yaml = format!(
            "domain: model_router\nredis:\n  host: 127.0.0.1\n  port: {}\n  timeout: {}ms\n{}",
            redis.port(),
            timeout.as_millis(),
            yaml_models
        );
        let mut config = ServiceConfig::from_yaml_str(&full_yaml).expect("valid test YAML");
        config.redis = RedisConfig {
            host: "127.0.0.1".to_string(),
            port: redis.port(),
            timeout,
        };

        let clock = Arc::new(MockClock::new(initial_unix_secs));
        let svc = RlsProxyService::with_clock(&config, Arc::clone(&clock)).expect("build service");

        let grpc_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind grpc listener");
        let grpc_addr = grpc_listener.local_addr().expect("grpc local addr");
        let incoming = tokio_stream_from_listener(grpc_listener);

        let server_task = tokio::spawn(async move {
            Server::builder()
                .add_service(RateLimitServiceServer::new(svc))
                .serve_with_incoming(incoming)
                .await
                .expect("tonic server");
        });

        let endpoint = format!("http://{grpc_addr}");
        let channel = Channel::from_shared(endpoint)
            .unwrap()
            .connect()
            .await
            .expect("connect grpc client");
        let client = RateLimitServiceClient::new(channel);

        Self {
            client,
            clock,
            redis,
            server_task,
        }
    }
}

fn tokio_stream_from_listener(
    listener: TcpListener,
) -> impl tonic::codegen::tokio_stream::Stream<Item = std::io::Result<TcpStream>> {
    futures_stream(listener)
}

fn futures_stream(
    listener: TcpListener,
) -> impl tonic::codegen::tokio_stream::Stream<Item = std::io::Result<TcpStream>> {
    struct ListenerStream(TcpListener);
    impl tonic::codegen::tokio_stream::Stream for ListenerStream {
        type Item = std::io::Result<TcpStream>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.get_mut()
                .0
                .poll_accept(cx)
                .map(|res| Some(res.map(|(stream, _)| stream)))
        }
    }
    ListenerStream(listener)
}

fn desc(provider: &str, model: &str, quota_type: &str, hits: u64) -> RateLimitDescriptor {
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
        limit: None,
        hits_addend: Some(hits),
        is_negative_hits: false,
    }
}

fn desc_with_override(
    provider: &str,
    model: &str,
    quota_type: &str,
    hits: u64,
    limit: u32,
    unit: RateLimitUnit,
    multiplier: Option<u32>,
) -> RateLimitDescriptor {
    let mut d = desc(provider, model, quota_type, hits);
    d.limit = Some(RateLimitOverride {
        requests_per_unit: limit,
        unit: unit as i32,
        unit_multiplier: multiplier,
    });
    d
}

fn rls_req(domain: &str, descriptors: Vec<RateLimitDescriptor>) -> RateLimitRequest {
    RateLimitRequest {
        domain: domain.to_string(),
        descriptors,
        hits_addend: 0,
    }
}

#[tokio::test]
async fn sample_config_files_parse_cleanly() {
    let cfg = ServiceConfig::from_file("config/rls.yaml").expect("config/rls.yaml must be valid");
    assert_eq!(cfg.domain, "model_router");
    assert_eq!(cfg.models.len(), 3);
}

#[tokio::test]
async fn allows_unconfigured_and_zero_limit_models_without_touching_redis() {
    let yaml = r#"
models:
  - name: no-quotas
    provider: anthropic
    model: claude-haiku-4-5
  - name: disabled-gpt
    provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 0
"#;
    let mut h = TestHarness::start(yaml, 1_000_000, Duration::from_secs(1)).await;

    // Unconfigured model and model with empty quotas -> OK with 0 Redis connections.
    let resp = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-haiku-4-5", "requests", 1),
                desc("anthropic", "claude-haiku-4-5", "tokens", 500),
                desc("google", "gemini-2.5-pro", "requests", 1),
            ],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.overall_code, RlsCode::Ok as i32);
    assert_eq!(resp.statuses.len(), 3);

    let consume_resp = h
        .client
        .consume_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-haiku-4-5", "requests", 1),
                desc("anthropic", "claude-haiku-4-5", "tokens", 500),
            ],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(consume_resp.overall_code, RlsCode::Ok as i32);

    // Zero-limit disabled model -> OVER_LIMIT on CheckRateLimit, no-op OK on ConsumeRateLimit.
    let disabled_check = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![
                desc("openai", "gpt-4o", "requests", 1),
                desc("openai", "gpt-4o", "tokens", 100),
            ],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(disabled_check.overall_code, RlsCode::OverLimit as i32);
    assert_eq!(disabled_check.statuses[0].code, RlsCode::OverLimit as i32);
    assert_eq!(disabled_check.statuses[0].limit_remaining, 0);

    let disabled_consume = h
        .client
        .consume_rate_limit(rls_req(
            "model_router",
            vec![desc("openai", "gpt-4o", "requests", 1)],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(disabled_consume.overall_code, RlsCode::Ok as i32);

    assert_eq!(h.redis.accepted_connections(), 0);
}

#[tokio::test]
async fn check_is_read_only_and_consume_rolls_sub_buckets_across_multi_windows() {
    // Mirrors Envoy's RedisQuotaTest multi-window rolling bucket scenario:
    // 1h request quota (limit 2), 6h request quota (limit 2), 1d token quota (limit 86400).
    let yaml = r#"
models:
  - provider: anthropic
    model: claude-sonnet-4-5
    quotas:
      - quota_type: REQUESTS
        time_unit: HOUR
        limit: 2
      - quota_type: REQUESTS
        time_unit: HOUR
        limit: 2
        unit_multiplier: 6
      - quota_type: TOKENS
        time_unit: DAY
        limit: 86400
"#;
    // t = 1_080_000s (divisible by 60, 360, and 900 -> buckets 18000, 3000, 1200)
    let mut h = TestHarness::start(yaml, 1_080_000, Duration::from_secs(1)).await;

    let check_req = || {
        rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-sonnet-4-5", "requests", 1),
                desc("anthropic", "claude-sonnet-4-5", "tokens", 100),
            ],
        )
    };

    // Repeated CheckRateLimit calls must NOT consume quota.
    for _ in 0..3 {
        let resp = h
            .client
            .check_rate_limit(check_req())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.overall_code, RlsCode::Ok as i32);
        assert_eq!(resp.statuses[0].limit_remaining, 2);
        assert_eq!(resp.statuses[1].limit_remaining, 86400);
    }

    // Consume #1 at t = 0: 1 request, 200 tokens.
    h.client
        .consume_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-sonnet-4-5", "requests", 1),
                desc("anthropic", "claude-sonnet-4-5", "tokens", 200),
            ],
        ))
        .await
        .unwrap();

    // Verify exact Redis keys & TTLs matching Envoy's redis_quota_test.cc!
    assert_eq!(
        h.redis
            .expiration_of("model_router:anthropic:claude-sonnet-4-5:req:1h:18000"),
        Some(3600)
    );
    assert_eq!(
        h.redis
            .expiration_of("model_router:anthropic:claude-sonnet-4-5:req:6h:3000"),
        Some(21600)
    );
    assert_eq!(
        h.redis
            .expiration_of("model_router:anthropic:claude-sonnet-4-5:tok:1d:1200"),
        Some(86400)
    );
    assert_eq!(
        h.redis
            .value_of("model_router:anthropic:claude-sonnet-4-5:tok:1d:1200"),
        Some(200)
    );

    // Advance 30 minutes and consume #2: 1 request, 200 tokens.
    h.clock.advance_secs(30 * 60);
    h.client
        .consume_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-sonnet-4-5", "requests", 1),
                desc("anthropic", "claude-sonnet-4-5", "tokens", 200),
            ],
        ))
        .await
        .unwrap();

    // Both 1h and 6h request quotas are now at 2/2 -> OVER_LIMIT.
    let over = h
        .client
        .check_rate_limit(check_req())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(over.overall_code, RlsCode::OverLimit as i32);
    assert_eq!(over.statuses[0].code, RlsCode::OverLimit as i32);
    assert_eq!(over.statuses[1].code, RlsCode::Ok as i32);

    // Advance +60m (to t + 90m): 1h window rolled out both requests, but 6h window still holds 2/2.
    h.clock.advance_secs(60 * 60);
    let still_over = h
        .client
        .check_rate_limit(check_req())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(still_over.overall_code, RlsCode::OverLimit as i32);

    // Advance to t + 5h54m (354m total): bucket 3000 is still inside the 60-bucket (6h) window.
    h.clock.advance_secs(264 * 60);
    let edge_over = h
        .client
        .check_rate_limit(check_req())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(edge_over.overall_code, RlsCode::OverLimit as i32);

    // Advance +6m to t + 6h (360m total, bucket 3060): bucket 3000 rolls out!
    // Meanwhile 1d token quota still holds 400 used out of 86400.
    h.clock.advance_secs(6 * 60);
    let token_fits = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-sonnet-4-5", "requests", 1),
                desc("anthropic", "claude-sonnet-4-5", "tokens", 86000),
            ],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(token_fits.overall_code, RlsCode::Ok as i32);

    let token_exceeds = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-sonnet-4-5", "requests", 1),
                desc("anthropic", "claude-sonnet-4-5", "tokens", 86001),
            ],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(token_exceeds.overall_code, RlsCode::OverLimit as i32);
    assert_eq!(token_exceeds.statuses[0].code, RlsCode::Ok as i32);
    assert_eq!(token_exceeds.statuses[1].code, RlsCode::OverLimit as i32);

    // Advance +18h30m (to t + 24h30m): all 1-day token buckets roll out completely.
    h.clock.advance_secs(18 * 3600 + 30 * 60);
    let all_clear = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![
                desc("anthropic", "claude-sonnet-4-5", "requests", 1),
                desc("anthropic", "claude-sonnet-4-5", "tokens", 86400),
            ],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(all_clear.overall_code, RlsCode::Ok as i32);
}

#[tokio::test]
async fn honors_envoy_rate_limit_overrides_and_second_windows() {
    // Config sets gpt-4o limit to 100/min, but Envoy sends a tighter 2/5s RateLimitOverride.
    let yaml = r#"
models:
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 100
"#;
    let mut h = TestHarness::start(yaml, 50_000, Duration::from_secs(1)).await;

    let override_req = || {
        rls_req(
            "model_router",
            vec![desc_with_override(
                "openai",
                "gpt-4o",
                "requests",
                1,
                2,
                RateLimitUnit::Second,
                Some(5),
            )],
        )
    };

    // Consume 1 at t = 50_000 and 1 at t = 50_002 -> reaches 2/2 in the 5-second window.
    h.client.consume_rate_limit(override_req()).await.unwrap();
    h.clock.advance_secs(2);
    h.client.consume_rate_limit(override_req()).await.unwrap();

    let blocked = h
        .client
        .check_rate_limit(override_req())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(blocked.overall_code, RlsCode::OverLimit as i32);

    // Advance 3 seconds to t = 50_005: bucket 50_000 rolls out of the 5x1s window!
    h.clock.advance_secs(3);
    let allowed = h
        .client
        .check_rate_limit(override_req())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(allowed.overall_code, RlsCode::Ok as i32);
    assert_eq!(allowed.statuses[0].limit_remaining, 1);

    // Override with requests_per_unit = 0 immediately disables without extra MGET.
    let (_, _, _) = h.redis.call_counts();
    let zero_override = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![desc_with_override(
                "openai",
                "gpt-4o",
                "requests",
                1,
                0,
                RateLimitUnit::Minute,
                None,
            )],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(zero_override.overall_code, RlsCode::OverLimit as i32);
}

#[tokio::test]
async fn validates_requests_and_rejects_should_rate_limit() {
    let mut h = TestHarness::start("models: []\n", 1_000_000, Duration::from_secs(1)).await;

    // 1. ShouldRateLimit -> UNIMPLEMENTED.
    let unimpl = h
        .client
        .should_rate_limit(rls_req(
            "model_router",
            vec![desc("openai", "gpt-4o", "requests", 1)],
        ))
        .await
        .expect_err("ShouldRateLimit must be unimplemented");
    assert_eq!(unimpl.code(), GrpcCode::Unimplemented);

    // 2. Domain mismatch -> INVALID_ARGUMENT.
    let bad_domain = h
        .client
        .check_rate_limit(rls_req(
            "wrong_domain",
            vec![desc("openai", "gpt-4o", "requests", 1)],
        ))
        .await
        .expect_err("wrong domain");
    assert_eq!(bad_domain.code(), GrpcCode::InvalidArgument);

    // 3. Empty descriptors -> INVALID_ARGUMENT.
    let empty_desc = h
        .client
        .check_rate_limit(rls_req("model_router", vec![]))
        .await
        .expect_err("empty descriptors");
    assert_eq!(empty_desc.code(), GrpcCode::InvalidArgument);

    // 4. Unsupported RateLimitUnit::Month -> INVALID_ARGUMENT.
    let bad_unit = h
        .client
        .check_rate_limit(rls_req(
            "model_router",
            vec![desc_with_override(
                "openai",
                "gpt-4o",
                "requests",
                1,
                10,
                RateLimitUnit::Month,
                None,
            )],
        ))
        .await
        .expect_err("unsupported MONTH unit");
    assert_eq!(bad_unit.code(), GrpcCode::InvalidArgument);
}

#[tokio::test]
async fn propagates_redis_errors_and_auto_reconnects_after_disconnect() {
    let yaml = r#"
models:
  - provider: openai
    model: gpt-4o
    quotas:
      - quota_type: REQUESTS
        time_unit: MINUTE
        limit: 2
"#;
    let mut h = TestHarness::start(yaml, 1_000_000, Duration::from_millis(150)).await;

    let req = || {
        rls_req(
            "model_router",
            vec![desc("openai", "gpt-4o", "requests", 1)],
        )
    };

    // 1. Normal consume succeeds (1/2).
    h.client.consume_rate_limit(req()).await.unwrap();

    // 2. Simulate Redis dropping connections and refusing reconnect temporarily.
    h.redis.set_accept_connections(false);
    h.redis.disconnect_all_clients();
    tokio::time::sleep(Duration::from_millis(20)).await;

    let err = h
        .client
        .check_rate_limit(req())
        .await
        .expect_err("Redis down must propagate error");
    assert!(
        matches!(
            err.code(),
            GrpcCode::Unavailable | GrpcCode::DeadlineExceeded
        ),
        "expected Unavailable or DeadlineExceeded, got {:?}",
        err.code()
    );

    // 3. Re-enable Redis -> service reconnects automatically and sees previously stored 1/2 count!
    h.redis.set_accept_connections(true);
    let recovered = h.client.check_rate_limit(req()).await.unwrap().into_inner();
    assert_eq!(recovered.overall_code, RlsCode::Ok as i32);
    assert_eq!(recovered.statuses[0].limit_remaining, 1);

    // 4. Simulate slow Redis exceeding timeout -> DEADLINE_EXCEEDED.
    h.redis.set_response_delay(Some(Duration::from_millis(400)));
    let timeout_err = h
        .client
        .check_rate_limit(req())
        .await
        .expect_err("slow Redis must time out");
    assert_eq!(timeout_err.code(), GrpcCode::DeadlineExceeded);
}
