import pytest
from src.outbox_worker import OutboxWorker
from src.dlq_manager import DLQManager

class FailingProvider:
    def send_email(self, headers, payload):
        return {"status_code": 500, "error": "Internal upstream service error"}

class MockCursorDLQ:
    def __init__(self):
        self.retry_count = 0
        self.status = "PENDING"
        self.dlq_entries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        if "UPDATE transactional_outbox" in query and "retry_count = retry_count + 1" in query:
            self.retry_count += 1
            self.status = "FAILED"
        elif "status = 'DLQ'" in query:
            self.status = "DLQ"
        elif "INSERT INTO dlq_events" in query:
            self.dlq_entries.append((1, params[0], params[1], params[2], params[3], False, False))
        elif "SELECT id, outbox_id, lead_id" in query:
            pass
        elif "UPDATE dlq_events SET replayed = TRUE" in query:
            pass

    def fetchone(self):
        if self.status in ["PENDING", "FAILED"]:
            return (1, "lead_77", 1, "outbound_email", '{"email":"lead77@test.com"}', "outbox_lead_77_1", self.retry_count)
        return None

    def fetchall(self):
        return [(1, 1, "lead_77", "Provider returned error", '{"email":"lead77@test.com"}')]

class MockConnectionDLQ:
    def __init__(self):
        self.cursor_obj = MockCursorDLQ()

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        pass

def test_dlq_escalation_after_max_retries():
    conn = MockConnectionDLQ()
    failing_provider = FailingProvider()
    worker = OutboxWorker(conn, failing_provider.send_email)
    dlq_mgr = DLQManager(conn)

    worker.poll_and_dispatch_single()
    assert conn.cursor_obj.retry_count == 1
    worker.poll_and_dispatch_single()
    assert conn.cursor_obj.retry_count == 2
    worker.poll_and_dispatch_single()
    assert conn.cursor_obj.retry_count == 3

    worker.poll_and_dispatch_single()
    assert conn.cursor_obj.status == "DLQ"

    unresolved = dlq_mgr.get_unresolved_events()
    assert len(unresolved) == 1
    assert unresolved[0]["lead_id"] == "lead_77"

    replayed = dlq_mgr.replay_event(unresolved[0]["dlq_id"])
    assert replayed is True
