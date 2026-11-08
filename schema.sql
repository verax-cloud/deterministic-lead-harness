-- Deterministic Lead Architecture Schema

CREATE TABLE IF NOT EXISTS leads (
    id VARCHAR(64) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    current_state VARCHAR(64) NOT NULL DEFAULT 'NEW',
    step_version INT NOT NULL DEFAULT 1,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS processed_webhooks (
    delivery_id VARCHAR(128) PRIMARY KEY,
    payload_hash VARCHAR(64) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(32) NOT NULL DEFAULT 'PROCESSED'
);

CREATE TABLE IF NOT EXISTS transactional_outbox (
    id BIGSERIAL PRIMARY KEY,
    lead_id VARCHAR(64) NOT NULL REFERENCES leads(id),
    step_version INT NOT NULL,
    destination VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    idempotency_key VARCHAR(128) NOT NULL UNIQUE,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    processing_started_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dlq_events (
    id BIGSERIAL PRIMARY KEY,
    outbox_id BIGINT NOT NULL,
    lead_id VARCHAR(64) NOT NULL,
    failure_reason TEXT NOT NULL,
    payload JSONB NOT NULL,
    replayed BOOLEAN NOT NULL DEFAULT FALSE,
    replay_count INT NOT NULL DEFAULT 0,
    escalated_to_human BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_outbox_polling ON transactional_outbox (status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_outbox_stale_processing ON transactional_outbox (status, processing_started_at) WHERE status = 'PROCESSING';
