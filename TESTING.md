# Testing Adaptive Hedging

## Automated Tests

Run the test suite to verify adaptive hedging:

```bash
# Run all tests
cargo test

# Run only hedging tests
cargo test hedge

# Run only adaptive hedging test
cargo test adaptive_hedging
```

## Manual Testing

### 1. Set up test environment

```bash
# Enable hedging with adaptive mode (default)
export ORLB_HEDGE_REQUESTS=true
export ORLB_ADAPTIVE_HEDGING=true
export ORLB_HEDGE_DELAY_MS=60
export ORLB_HEDGE_MIN_DELAY_MS=10
export ORLB_HEDGE_MAX_DELAY_MS=200

# Optional: resolve Vault-managed API keys
export ORLB_SECRET_BACKEND=vault
export ORLB_VAULT_ADDR=https://vault.example.com
export ORLB_VAULT_TOKEN=replace-me
# export ORLB_VAULT_NAMESPACE=solana    # if you use namespaces
```

### 2. Create test providers.json

Create a `providers.json` with one slow and one fast provider:

```json
[
  {
    "name": "Slow Provider",
    "url": "http://slow-endpoint:8080",
    "weight": 1,
    "tags": []
  },
  {
    "name": "Fast Provider", 
    "url": "http://fast-endpoint:8080",
    "weight": 1,
    "tags": []
  }
]
```

### 3. Start the load balancer

```bash
cargo run
```

### 4. Monitor metrics

Watch the hedging metrics:

```bash
# Check hedge counts by reason
curl http://localhost:8080/metrics | grep hedges_total

# Expected output shows:
# - orlb_hedges_total{reason="adaptive"} - adaptive hedges
# - orlb_hedges_total{reason="timer"} - fixed delay hedges
```

### 5. Test scenarios

#### Scenario A: Slow Provider
1. Configure slow provider with 200ms latency
2. Make requests through the load balancer
3. Adaptive hedging should trigger earlier (shorter delay)
4. Fast provider should win most requests
5. Check metrics: `orlb_hedges_total{reason="adaptive"}` should increment

#### Scenario B: Fast Provider
1. Configure fast provider with <50ms latency  
2. Make requests through the load balancer
3. Adaptive hedging should use normal/longer delays
4. Primary provider should win (fewer unnecessary hedges)
5. Check that hedge count is lower than scenario A

#### Scenario C: Compare Fixed vs Adaptive

**Fixed mode:**
```bash
export ORLB_ADAPTIVE_HEDGING=false
export ORLB_HEDGE_DELAY_MS=60
```

**Adaptive mode:**
```bash
export ORLB_ADAPTIVE_HEDGING=true
export ORLB_HEDGE_DELAY_MS=60
```

Compare p99 latency metrics between the two modes - adaptive should show lower p99 when providers have variable latency.

### 6. Doctor sample transaction checks

1. Add a `sample_signature` field to each provider in `providers.json` that points to a recent, known-good transaction for that cluster.
2. If any provider header values reference `vault://mount/path#field`, make sure the Vault env vars above are exported so secrets can be fetched.
3. Run `cargo run -- doctor`.
4. The output should now include a `sample tx ... (slot …)` line per provider and fail fast if Vault lookups or `getTransaction` calls cannot be satisfied.
5. If you see authorization failures, verify the Vault token policy grants read access to the referenced path and that the secret JSON contains the named key.

### 7. WebSocket fan-out sanity check

1. Ensure at least one provider exposes a WebSocket endpoint (`ws_url` in `providers.json` when it is not a straight `https://` to `wss://` swap).
2. Start ORLB and connect with a client such as `websocat ws://localhost:8080/ws` or `npx wscat -c ws://localhost:8080/ws`.
3. Issue a subscription payload (for example, a Solana `logsSubscribe`) and verify notifications arrive.
4. Stop or firewall one upstream WebSocket to confirm the stream continues via the remaining providers without reconnecting.
5. Resume the upstream and ensure fan-out resumes without dropping the client connection.

## Verify Adaptive Behavior

### Check latency history impact

1. Send several slow requests to build latency history
2. Make a new request
3. The adaptive algorithm should use shorter hedge delay based on historical latency
4. You can see this in logs or by checking the hedge delay timing

### Dashboard Verification

1. Open `http://localhost:8080/dashboard`
2. Check provider cards for:
   - Latency EMA values
   - Success/error counts
3. Adaptive hedging works better when EMA is high (slow providers)

## Expected Behavior

- **Slow providers (EMA > 50ms)**: Hedge delay should be shorter than base delay
- **Fast providers (EMA < 50ms)**: Hedge delay should be similar or slightly longer than base delay  
- **Failing providers**: Hedge delay should be 50% of calculated delay
- **All delays**: Clamped between min (10ms) and max (200ms) by default

## Troubleshooting

If adaptive hedging doesn't seem to work:

1. Check that `ORLB_HEDGE_REQUESTS=true`
2. Verify `ORLB_ADAPTIVE_HEDGING=true` (default)
3. Ensure you have at least 2 providers
4. Check that requests are retryable read methods
5. Look at logs for hedge reason: "adaptive" vs "timer"
6. When Vault-backed headers are configured, confirm `ORLB_SECRET_BACKEND` and Vault env vars are set before launching ORLB; missing secrets will surface as transport errors when forwarding.
