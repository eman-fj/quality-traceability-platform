"""QTP inspection-station simulator.

Publishes one inspection event per part to Kafka topic `quality.inspections`,
for three production lines, at roughly one event/second/line. Most parts are
clean (empty defect_codes); a configurable fraction carry a defect, and a
CRITICAL defect is force-injected on a timer so the demo always has something
to follow through the pipeline.

Event shape (matches the brief):
    part_id, line_id, inspection_timestamp, measurements (dict),
    defect_codes (list), material_batch_id, operator_id

All behaviour is driven by environment variables (see docker-compose.yml /
.env.example) so nothing is hardcoded.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

# ---------------------------------------------------------------- config
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("INSPECTIONS_TOPIC", "quality.inspections")
LINES = [s.strip() for s in os.getenv("LINES", "line_1,line_2,line_3").split(",") if s.strip()]
RATE = float(os.getenv("EVENTS_PER_SEC_PER_LINE", "1"))
DEFECT_RATE = float(os.getenv("DEFECT_RATE", "0.08"))
CRITICAL_EVERY = float(os.getenv("CRITICAL_EVERY_SECONDS", "45"))

# Batch IDs MUST exist in the MySQL ERP seed (docker/mysql/init.sql).
BATCHES = [f"BATCH-{n}" for n in range(1001, 1011)]
OPERATORS = ["OP-101", "OP-102", "OP-103", "OP-104", "OP-105"]

# Defect codes grouped by the severity the classify API assigns.
CRITICAL_CODES = ["SUR-001", "MAT-001"]
MAJOR_CODES = ["DIM-001", "DIM-002", "MAT-002"]
MINOR_CODES = ["SUR-002", "ASS-001"]
NON_CRITICAL = MAJOR_CODES + MINOR_CODES

SCHEMA_VERSION = 1


def current_shift(now: datetime) -> str:
    """Three 8-hour shifts: A 06-14, B 14-22, C 22-06."""
    h = now.hour
    if 6 <= h < 14:
        return "A"
    if 14 <= h < 22:
        return "B"
    return "C"


def make_measurements(defective: bool) -> dict:
    """Realistic dimensional / surface / material readings."""
    return {
        "length_mm": round(random.uniform(119.5, 120.5), 3),
        "diameter_mm": round(random.uniform(24.8, 25.2), 3),
        "surface_score": round(random.uniform(0.0, 0.4 if not defective else 0.95), 3),
        "hardness_hrc": round(random.uniform(48 if not defective else 38, 56), 1),
    }


def make_event(line_id: str, seq: int, force_critical: bool = False) -> dict:
    now = datetime.now(timezone.utc)

    if force_critical:
        defect_codes = [random.choice(CRITICAL_CODES)]
    elif random.random() < DEFECT_RATE:
        defect_codes = [random.choice(NON_CRITICAL)]
    else:
        defect_codes = []

    return {
        "part_id": f"PRT-{line_id}-{now.strftime('%Y%m%d')}-{seq:06d}",
        "line_id": line_id,
        "inspection_timestamp": now.isoformat(),
        "measurements": make_measurements(bool(defect_codes)),
        "defect_codes": defect_codes,
        "material_batch_id": random.choice(BATCHES),
        "operator_id": random.choice(OPERATORS),
        "shift": current_shift(now),
        "schema_version": SCHEMA_VERSION,
    }


def connect() -> KafkaProducer:
    """Retry until Kafka is reachable (handles startup ordering)."""
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                linger_ms=50,
            )
        except Exception as exc:  # noqa: BLE001 - simulator, keep it simple
            print(f"[simulator] Kafka not ready ({exc}); retrying in 3s", flush=True)
            time.sleep(3)


def main() -> None:
    print(
        f"[simulator] -> {BOOTSTRAP} topic={TOPIC} lines={LINES} "
        f"rate={RATE}/s/line defect_rate={DEFECT_RATE} critical_every={CRITICAL_EVERY}s",
        flush=True,
    )
    producer = connect()
    seq = 0
    last_critical = time.monotonic()
    interval = 1.0 / RATE if RATE > 0 else 1.0

    while True:
        cycle_start = time.monotonic()
        force_critical_line = None
        if time.monotonic() - last_critical >= CRITICAL_EVERY:
            force_critical_line = random.choice(LINES)
            last_critical = time.monotonic()

        for line_id in LINES:
            seq += 1
            event = make_event(line_id, seq, force_critical=(line_id == force_critical_line))
            producer.send(TOPIC, key=line_id, value=event)
            if event["defect_codes"]:
                tag = "CRITICAL" if event["defect_codes"][0] in CRITICAL_CODES else "defect"
                print(
                    f"[simulator] {tag:8s} {event['part_id']} "
                    f"codes={event['defect_codes']} batch={event['material_batch_id']}",
                    flush=True,
                )

        producer.flush()
        elapsed = time.monotonic() - cycle_start
        time.sleep(max(0.0, interval - elapsed))


if __name__ == "__main__":
    main()
