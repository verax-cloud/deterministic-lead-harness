import hashlib
import json
from typing import Dict, Any, Tuple

class WebhookHandler:
    def __init__(self, db_connection):
        self.conn = db_connection

    @staticmethod
    def compute_content_hash(payload: Dict[str, Any]) -> str:
        serialized = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def process_incoming_event(self, headers: Dict[str, str], payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        delivery_id = headers.get("X-Delivery-ID") or headers.get("evt_id")
        content_hash = self.compute_content_hash(payload)

        if not delivery_id:
            delivery_id = f"fallback_{content_hash}"

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO processed_webhooks (delivery_id, payload_hash, status)
                VALUES (%s, %s, 'PROCESSED')
                ON CONFLICT (delivery_id) DO NOTHING
                RETURNING delivery_id;
                """,
                (delivery_id, content_hash),
            )
            inserted = cur.fetchone()

            if not inserted:
                return 200, {"status": "duplicate_ignored", "delivery_id": delivery_id}

            lead_id = payload.get("lead_id")
            event_type = payload.get("event_type")

            cur.execute(
                """
                INSERT INTO leads (id, email, current_state, step_version)
                VALUES (%s, %s, 'NEW', 1)
                ON CONFLICT (id) DO UPDATE SET updated_at = CURRENT_TIMESTAMP
                RETURNING step_version;
                """,
                (lead_id, payload.get("email", "unknown@test.com")),
            )
            step_version = cur.fetchone()[0]

            idempotency_key = f"outbox_{lead_id}_{step_version}_{event_type}"
            cur.execute(
                """
                INSERT INTO transactional_outbox (lead_id, step_version, destination, payload, idempotency_key)
                VALUES (%s, %s, 'outbound_email', %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING;
                """,
                (lead_id, step_version, json.dumps(payload), idempotency_key),
            )

            self.conn.commit()

        return 200, {"status": "accepted", "delivery_id": delivery_id}
