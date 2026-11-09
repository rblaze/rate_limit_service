# Envoy RateLimitService Proxy (`CheckRateLimit` & `ConsumeRateLimit`)

Rust proof-of-concept implementation of Envoy's `envoy.service.ratelimit.v3.RateLimitService` supporting the new two-phase quota RPCs introduced on [`rblaze/envoy@rls`](https://github.com/rblaze/envoy/tree/rls):

- **`CheckRateLimit`**: Evaluates rolling-window request and token quotas in Redis via `MGET` without mutating counters, returning `OK` or `OVER_LIMIT` (plus per-descriptor `DescriptorStatus` metadata).
- **`ConsumeRateLimit`**: Atomically increments current sub-bucket counters and refreshes key TTLs in Redis via a single pipelined `INCRBY` + `EXPIRE` batch without performing a limit check.
- **`ShouldRateLimit`**: Returns gRPC `UNIMPLEMENTED` (`CheckRateLimit` and `ConsumeRateLimit` are used instead).

## Quota & Redis Design

Quotas follow the same sub-bucketed sliding-window design and Redis key layout as Envoy's in-process `redis_quota.cc`:

| `time_unit` | Window (`M` = `unit_multiplier`) | Sub-bucket size | Buckets (`num_buckets`) | Key tag |
|-------------|----------------------------------|-----------------|-------------------------|---------|
| `SECOND`    | `1s * M`                         | `1s`            | `M` (up to `3600`)      | `s`     |
| `MINUTE`    | `60s * M`                        | `1s * M`        | `60`                    | `m`     |
| `HOUR`      | `3600s * M`                      | `60s * M`       | `60`                    | `h`     |
| `DAY`       | `86400s * M`                     | `900s * M`      | `96`                    | `d`     |

Redis keys use the format:
```text
<domain>:<provider>:<model>:<req|tok>:<unit_multiplier><unit_tag>:<bucket_index>
```
For example, with default `domain: "model_router"`, a 6-hour request quota on `anthropic` / `claude-sonnet-5-5` writes keys of the form `model_router:anthropic:claude-sonnet-5-5:req:6h:<bucket>` with `EXPIRE 21600`.

### Precedence & Special Cases
- **`RateLimitOverride` (`descriptor.limit`)**: When Envoy's `ModelRouter` defines quotas on a `Model` locally, `RlsQuota` attaches `descriptor.limit` to the gRPC descriptor; this service honors that override. When `descriptor.limit` is omitted, the service falls back to its own YAML `models` configuration.
- **Unconfigured models**: Allowed (`OK` on `CheckRateLimit`, no-op on `ConsumeRateLimit`) without querying Redis.
- **`limit: 0`**: Disables the model (`OVER_LIMIT` on `CheckRateLimit`, no-op on `ConsumeRateLimit`) without querying Redis.
- **Redis errors/timeouts**: Propagated as gRPC `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `INTERNAL` so Envoy's `ModelRouter` filter logs a warning and fails open.

## Building & Testing

```bash
cargo build --release
cargo test
```

## Running a Demo with Envoy (`rls` branch)

1. **Start Redis** on `127.0.0.1:6379`:
   ```bash
   redis-server --port 6379
   ```

2. **Start `rate-limit-service`** with the sample quota config:
   ```bash
   cargo run -- --config config/rls.yaml --log-level debug
   ```

3. **Start an upstream mock LLM server** on `127.0.0.1:18080` (returning an Anthropic `/v1/messages` JSON payload with `usage.input_tokens` and `usage.output_tokens`) and **start Envoy** built from `rblaze/envoy@rls`:
   ```bash
   envoy -c config/envoy-demo.yaml
   ```

4. **Send OpenAI-format chat completion requests** to Envoy (`127.0.0.1:10000`):
   ```bash
   curl -s http://127.0.0.1:10000/v1/chat/completions \
     -H "Content-Type: application/json" \
     -d '{"model":"claude-auto","messages":[{"role":"user","content":"Hello!"}]}'
   ```
   - For the first 3 requests within a minute, `CheckRateLimit` returns `OK` for `claude-haiku-5-5`, Envoy routes to Haiku, and `ConsumeRateLimit` records `1` request plus `input + output` tokens in Redis.
   - Starting on the 4th request within the minute, `CheckRateLimit` returns `OVER_LIMIT` for `claude-haiku-5-5` and `OK` for `claude-sonnet-5-5`, so Envoy automatically routes to Sonnet and records consumption against Sonnet's buckets.
