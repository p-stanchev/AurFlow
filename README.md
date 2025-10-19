# Open RPC Load Balancer (ORLB)

ORLB is a lightweight JSON-RPC proxy for Solana that fans client traffic across multiple upstream RPC providers. It continuously probes each provider, scores them on latency and error rate, retries idempotent calls once, and exposes live Prometheus metrics plus a web dashboard. The goal of this MVP is to prove dependable routing with public RPCs in a single-container deploy.

## Core Capabilities
- **Smart routing** – weighted round-robin informed by latency EMA and failure streaks.
- **Read-only safety** – mutating RPC methods like `sendTransaction` are blocked.
- **Automatic retries** – read-only calls try a second provider on timeouts/429/5xx.
- **Health probes** – background `getSlot` checks every 5s update provider status.
- **Observability** – `/metrics` (Prometheus text) and `/metrics.json` plus `/dashboard` (Chart.js) showing live scores.
- **Simple ops** – single binary with embedded dashboard, configurable via env vars, ships with Dockerfile and CI.

## Project Layout
```
src/
  main.rs            – bootstrap (config, tracing, background tasks)
  config.rs          – env parsing with defaults
  registry.rs        – provider registry loader (`providers.json`)
  metrics.rs         – Prometheus collectors + dashboard snapshot logic
  forward.rs         – reqwest client builder and JSON-RPC forwarding
  health.rs          – async probe loop updating provider health
  router.rs          – Hyper HTTP server, routing, retry logic, tests
  dashboard.rs       – serves embedded Chart.js dashboard
static/dashboard.html – HTML/JS UI (pulls `/metrics.json` every 5s)
providers.json        – sample provider pool
Dockerfile            – multi-stage build producing minimal image
```

## Getting Started
### Prerequisites
- Rust 1.79+ (`rustup toolchain install 1.79.0`)
- `cargo` and `rustfmt` (`rustup component add rustfmt clippy`)

### Run Locally
```bash
cargo run
# ORLB listens on 0.0.0.0:8080 by default
```

Send a request:
```bash
curl http://localhost:8080/rpc \
  -H 'content-type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}'
```

View the dashboard at [http://localhost:8080/dashboard](http://localhost:8080/dashboard). Prometheus can scrape [http://localhost:8080/metrics](http://localhost:8080/metrics).

### Configuration
ORLB is configured via environment variables and a provider registry file:

| Variable | Default | Description |
|---|---|---|
| `ORLB_LISTEN_ADDR` | `0.0.0.0:8080` | HTTP listen address |
| `ORLB_PROVIDERS_PATH` | `providers.json` | Path to JSON provider registry |
| `ORLB_PROBE_INTERVAL_SECS` | `5` | Health probe interval |
| `ORLB_REQUEST_TIMEOUT_SECS` | `10` | Upstream request timeout |
| `ORLB_RETRY_READ_REQUESTS` | `true` | Retry read-only calls once on failure |
| `ORLB_DASHBOARD_DIR` | unset | Optional override directory for dashboard HTML |

`providers.json` format:
```json
[
  {"name": "Solana Foundation", "url": "https://api.mainnet-beta.solana.com", "weight": 1},
  {"name": "Ankr", "url": "https://rpc.ankr.com/solana", "weight": 1},
  {"name": "Helius", "url": "https://rpc.helius.xyz/?api-key=demo", "weight": 1}
]
```
Optional headers per provider can be supplied with a `headers` array (`[{ "name": "...", "value": "..." }]`).

### HTTP Endpoints
- `POST /rpc` – JSON-RPC forwarding (read-only enforced, retries once on retryable failures).
- `GET /metrics` – Prometheus text format metrics (`orlb_requests_total`, `orlb_provider_latency_ms`, etc.).
- `GET /metrics.json` – JSON snapshot powering the dashboard.
- `GET /dashboard` – live Chart.js UI summarising provider health.

### Metrics Highlights
- `orlb_requests_total{provider,method,status}` – counts per upstream and outcome.
- `orlb_request_failures_total{provider,reason}` – upstream failures grouped by reason.
- `orlb_retries_total{reason}` – retry invocations (transport/timeouts/status codes).
- `orlb_provider_latency_ms{provider}` – latest latency measurement.
- `orlb_provider_health{provider}` – `1` healthy, `0` degraded.

## Testing
Unit-style tests live alongside the router and cover forwarding success, retry failover, and write-method rejection. Run the full suite with:
```bash
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```
Continuous integration in `.github/workflows/ci.yml` enforces the same checks.

## Docker
Build and run the container:
```bash
docker build -t orlb .
docker run --rm -p 8080:8080 \
  -v $(pwd)/providers.json:/app/providers.json \
  -e ORLB_PROVIDERS_PATH=/app/providers.json \
  orlb
```

The runtime image is based on `debian:bookworm-slim`, bundles the compiled binary plus static assets, and installs CA certificates for TLS connections.

## Deploying to Fly.io (Example)
1. Create `fly.toml` (simplified):
   ```toml
   app = "orlb-mvp"

   [http_service]
   internal_port = 8080
   force_https = true
   auto_stop_machines = true
   auto_start_machines = true
   ```
2. `fly auth login`
3. `fly launch --no-deploy`
4. `fly deploy`

Set secrets for any private provider headers or API keys. The service is stateless so horizontal scaling is straightforward (just keep `providers.json` consistent across instances).

## Roadmap Ideas
- Commitment-aware routing (processed/confirmed/finalized scoring).
- Subscription/WebSocket fan-out.
- API key rate limiting and auth.
- Edge deployments across regions, optional caching (e.g., `getLatestBlockhash`).

## License
Apache 2.0 (or adapt as needed). Feel free to fork and extend for grant demos or production load-balancing.
