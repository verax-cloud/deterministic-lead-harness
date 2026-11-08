# Deterministic Lead Architecture: PostgreSQL Outbox & Test Harness

A reference implementation and verification harness proving failure boundaries for AI-driven B2B lead workflows. Demonstrates how to separate non-deterministic LLM decision making from deterministic state management.

## Architectural Guarantees

1. **Deterministic State Boundary**: LLMs propose state transitions via structured JSON schemas; PostgreSQL commits state transitions within ACID transactions.
2. **Webhook Idempotency**: Deduplicates incoming webhook events using provider delivery IDs (`evt_...` / `X-Delivery-ID`) backed by unique constraints, with cryptographic content hashing as defensive fallback.
3. **Outbox Worker Resilience (Two Generals Problem)**: Client-side deterministic idempotency keys (`outbox_{lead_id}_{step_version}`) prevent duplicate outbound emails if workers crash after provider acceptance.
4. **Resilient Retry & DLQ Lifecycle**: 3-tier worker retry cadence (1s, 5s, 30s) followed by a 5-minute DLQ re-evaluator before escalating to human alerting.

---

## Repository Structure

```
├── docker-compose.yml          # PostgreSQL test instance
├── schema.sql                  # DDL with transactional outbox & idempotency tables
├── src/
│   ├── db.py                   # Connection & migration utilities
│   ├── webhook_handler.py      # Idempotent webhook receiver
│   ├── outbox_worker.py        # Transactional outbox polling worker (SKIP LOCKED)
│   └── dlq_manager.py          # Dead-letter queue re-evaluation & replay
└── tests/
    ├── test_webhooks.py        # Concurrency & duplicate delivery verification
    ├── test_outbox_worker.py   # Worker crash recovery & outbound deduplication
    └── test_dlq_replay.py      # Exponential backoff & DLQ lifecycle
```

---

## Quickstart

### Prerequisites
- Python 3.10+
- Docker & Docker Compose

### 1. Start PostgreSQL
```bash
docker compose up -d
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Run Test Suite
```bash
pytest tests/ -v
```

---

## Failure Boundary Verification

### 1. Duplicate Webhooks (`test_webhooks.py`)
Fires concurrent requests with identical delivery IDs. Verifies that exactly one transaction succeeds while duplicates return cached HTTP 200 responses with zero state corruption.

### 2. Worker Crash Simulation (`test_outbox_worker.py`)
Mocks worker termination immediately following external mail provider (e.g. Resend/SendGrid) acceptance (HTTP 200) before marking the outbox row as processed. Verifies that the restarted worker sends the identical `Idempotency-Key`, causing the provider to return the cached response rather than sending duplicate emails.

### 3. Dead-Letter Queue Lifecycle (`test_dlq_replay.py`)
Simulates 3 consecutive transient provider outages. Verifies that the message routes to `dlq_events`, undergoes scheduled re-evaluation, and can be safely replayed.
