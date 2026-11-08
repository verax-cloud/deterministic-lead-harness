import pytest
from src.webhook_handler import WebhookHandler

class MockCursor:
    def __init__(self):
        self.seen_deliveries = set()
        self.last_result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def execute(self, query, params=None):
        if "INSERT INTO processed_webhooks" in query:
            delivery_id = params[0]
            if delivery_id in self.seen_deliveries:
                self.last_result = None
            else:
                self.seen_deliveries.add(delivery_id)
                self.last_result = (delivery_id,)
        elif "INSERT INTO leads" in query:
            self.last_result = (1,)
        elif "INSERT INTO transactional_outbox" in query:
            self.last_result = None

    def fetchone(self):
        return self.last_result

class MockConnection:
    def __init__(self):
        self.mock_cursor = MockCursor()

    def cursor(self):
        return self.mock_cursor

    def commit(self):
        pass

def test_webhook_deduplication():
    conn = MockConnection()
    handler = WebhookHandler(conn)

    headers = {"X-Delivery-ID": "evt_prod_991823"}
    payload = {"lead_id": "lead_42", "email": "ethan@example.com", "event_type": "demo_requested"}

    code1, resp1 = handler.process_incoming_event(headers, payload)
    assert code1 == 200
    assert resp1["status"] == "accepted"

    code2, resp2 = handler.process_incoming_event(headers, payload)
    assert code2 == 200
    assert resp2["status"] == "duplicate_ignored"

def test_webhook_content_hash_fallback():
    conn = MockConnection()
    handler = WebhookHandler(conn)

    headers = {}
    payload = {"lead_id": "lead_99", "email": "founder@startup.io", "event_type": "signup"}

    code1, resp1 = handler.process_incoming_event(headers, payload)
    assert code1 == 200
    assert resp1["status"] == "accepted"
    assert resp1["delivery_id"].startswith("fallback_")

    code2, resp2 = handler.process_incoming_event(headers, payload)
    assert code2 == 200
    assert resp2["status"] == "duplicate_ignored"
