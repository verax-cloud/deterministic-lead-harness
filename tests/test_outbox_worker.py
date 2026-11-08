import pytest
from src.outbox_worker import OutboxWorker

class MockProvider:
    def __init__(self):
        self.dispatched_keys = []

    def send_email(self, headers, payload):
        idempotency_key = headers.get("Idempotency-Key")
        self.dispatched_keys.append(idempotency_key)
        return {"status_code": 200, "message_id": f"msg_for_{idempotency_key}"}

class MockCursorWorker:
    def __init__(self):
        self.outbox_row = (1, "lead_42", 1, "outbound_email", '{"email":"test@test.com"}', "outbox_lead_42_1_demo", 0)
        self.status = "PENDING"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        if "UPDATE transactional_outbox SET status = 'PROCESSING'" in query:
            self.status = "PROCESSING"
        elif "UPDATE transactional_outbox SET status = 'COMPLETED'" in query:
            self.status = "COMPLETED"

    def fetchone(self):
        if self.status == "COMPLETED":
            return None
        return self.outbox_row

class MockConnectionWorker:
    def __init__(self):
        self.cursor_obj = MockCursorWorker()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

def test_worker_crash_and_client_side_idempotency_recovery():
    conn = MockConnectionWorker()
    provider = MockProvider()
    worker = OutboxWorker(conn, provider.send_email)

    with pytest.raises(RuntimeError, match="Worker process terminated abruptly!"):
        worker.poll_and_dispatch_single(crash_before_commit=True)

    assert len(provider.dispatched_keys) == 1

    result = worker.poll_and_dispatch_single(crash_before_commit=False)
    assert result["status"] == "dispatched"
    assert len(provider.dispatched_keys) == 2
    assert provider.dispatched_keys[0] == provider.dispatched_keys[1] == "outbox_lead_42_1_demo"
