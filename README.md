# Deterministic Lead Architecture: PostgreSQL Outbox & Test Harness

A reference implementation and verification harness proving failure boundaries for AI-driven B2B lead workflows. Demonstrates how to separate non-deterministic LLM decision making from deterministic state management.

## Architectural Guarantees & Edge-Case Remediation

1. **Deterministic State Boundary**: LLMs propose state transitions via structured JSON schemas; PostgreSQL commits state transitions within ACID transactions.
2. **Webhook Idempotency**: Deduplicates incoming webhook events using provider delivery IDs (`evt_...` / `X-Delivery-ID`) backed by unique constraints, with cryptographic content hashing as defensive fallback.
3. **Lease-Based Worker Crash Recovery (Point 1 Remediation)**: If a worker terminates abruptly while in `PROCESSING` state, subsequent polling sweeps reclaim records whose processing lease has expired (`lease_timeout_seconds`, default 60s), ensuring no event remains orphaned.
4. **Outbox Worker Resilience & Provider Deduplication (Point 2 Remediation)**: Client-side deterministic idempotency keys (`outbox_{lead_id}_{step_version}`) are sent to external mail providers (Resend/SendGrid). When network timeouts cause worker retries, the provider returns the cached response without double-sending. Verified via mock provider simulation asserting single external dispatch.
5. **Automated DLQ Scheduled Evaluation (Point 3 Remediation)**: Background evaluator runs every 5 minutes (`evaluate_and_replay_dlq`), evaluating backoff thresholds, attempting automated replay up to max attempts, and escalating to human alerting when required.
6. **Dual Test Suite (Point 4 Remediation)**: Includes fast-feedback mock unit tests (`pytest tests/`) and live concurrency/crash integration tests executed directly against PostgreSQL (`tests/integration/`).

---

## Repository Structure

```
├── docker-compose.yml              # PostgreSQL test instance
├── schema.sql                      # DDL with transactional outbox & idempotency tables
├── src/
│   ├── db.py                       # Connection & migration utilities
│   ├── webhook_handler.py          # Idempotent webhook receiver
│   ├── outbox_worker.py            # Transactional outbox polling worker (SKIP LOCKED + Lease Reclaim)
│   └── dlq_manager.py              # Automated 5-min DLQ evaluator & replay manager
└── tests/
    ├── test_webhooks.py            # Concurrency & duplicate delivery verification
    ├── test_outbox_worker.py       # Worker crash lease recovery & provider deduplication
    ├── test_dlq_replay.py          # Automated 5-min DLQ lifecycle & escalation
    └── integration/
        └── test_postgres_live.py   # Real PostgreSQL concurrency & failure tests
```

---

## Quickstart

### 1. Start PostgreSQL
```bash
docker compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run Unit & Contract Tests
```bash
pytest tests/ -v
```

### 4. Run Live Concurrency & Crash Integration Tests
```bash
RUN_LIVE_POSTGRES=1 pytest tests/integration/ -v
```
