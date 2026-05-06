"""Kafka playground for demo-hub: metadata, producer knobs, short consumer polls."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from typing import Any, Literal

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
DEFAULT_LAB_TOPIC = os.environ.get("KAFKA_LAB_TOPIC", "demo-hub.kafka.lab")

_TOPIC_OK = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")


def bootstrap_list() -> list[str]:
    return [h.strip() for h in KAFKA_BOOTSTRAP.split(",") if h.strip()]


def validate_topic(name: str) -> str:
    t = (name or "").strip()
    if not _TOPIC_OK.match(t):
        raise ValueError(
            "topic must be 1–249 chars: letters, digits, dot, underscore, hyphen only"
        )
    return t


def metadata() -> dict[str, Any]:
    try:
        from kafka import KafkaConsumer  # type: ignore
    except ImportError:
        return {"ok": False, "error": "kafka-python is not installed"}

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_list(),
        consumer_timeout_ms=9000,
        client_id="demo-hub-kafka-lab-metadata",
    )
    try:
        raw = consumer.topics()
        topics = sorted(t for t in raw if not str(t).startswith("__"))
        brokers = (
            len(consumer.cluster.brokers())
            if hasattr(consumer, "cluster") and consumer.cluster
            else None
        )
        return {
            "ok": True,
            "bootstrap": KAFKA_BOOTSTRAP,
            "broker_nodes_seen": brokers,
            "topics": topics[:500],
            "topic_count": len(topics),
            "truncated_list": len(topics) > 500,
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "bootstrap": KAFKA_BOOTSTRAP}
    finally:
        consumer.close()


def produce_burst(
    *,
    topic: str,
    count: int,
    key_mode: Literal["none", "fixed", "random", "per_message"],
    fixed_key: str,
    acks: str,
    linger_ms: int,
    batch_size: int,
    compression: Literal["none", "gzip", "snappy", "lz4", "zstd"],
    value_pad_kb: int,
    enable_idempotence: bool,
) -> dict[str, Any]:
    try:
        from kafka import KafkaProducer  # type: ignore
    except ImportError:
        return {"ok": False, "error": "kafka-python is not installed"}

    topic = validate_topic(topic)
    pad = ("x" * max(0, value_pad_kb)) * 1024
    if acks == "0":
        eff_acks: Any = 0
    elif acks == "1":
        eff_acks = 1
    else:
        eff_acks = "all"
    if enable_idempotence:
        eff_acks = "all"

    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap_list(),
        "value_serializer": lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        "key_serializer": lambda k: (k if k is not None else "").encode("utf-8"),
        "acks": eff_acks,
        "linger_ms": int(linger_ms),
        "batch_size": int(batch_size),
        "request_timeout_ms": 60_000,
        "api_version_auto_timeout_ms": 15_000,
    }
    if compression != "none":
        kwargs["compression_type"] = compression
    if enable_idempotence:
        kwargs["enable_idempotence"] = True
        kwargs["acks"] = "all"

    producer = KafkaProducer(**kwargs)
    t0 = time.perf_counter()
    errs: list[str] = []
    try:
        for i in range(count):
            if key_mode == "none":
                key = None
            elif key_mode == "fixed":
                key = fixed_key or "lab"
            elif key_mode == "random":
                key = uuid.uuid4().hex[:12]
            else:
                key = f"{i:08d}"

            payload = {
                "lab": True,
                "seq": i,
                "ts": time.time(),
                "pad": pad,
            }
            try:
                producer.send(topic, key=key, value=payload)
            except Exception as e:
                errs.append(f"seq {i}: {e}")
                break
        producer.flush(timeout=120)
    finally:
        producer.close()

    elapsed = time.perf_counter() - t0
    return {
        "ok": len(errs) == 0,
        "topic": topic,
        "requested": count,
        "errors": errs,
        "elapsed_sec": round(elapsed, 4),
        "approx_throughput_rps": round(count / elapsed, 2) if elapsed > 0 else None,
        "effective_acks": kwargs.get("acks"),
        "compression": compression,
        "linger_ms": linger_ms,
        "batch_size": batch_size,
        "enable_idempotence": enable_idempotence,
        "key_mode": key_mode,
        "value_approx_bytes": len(json.dumps({"lab": True, "seq": 0, "ts": 0.0, "pad": pad}).encode()),
    }


def consume_poll(
    *,
    topic: str,
    group_id: str,
    max_messages: int,
    timeout_ms: int,
    auto_offset_reset: Literal["earliest", "latest"],
) -> dict[str, Any]:
    try:
        from kafka import KafkaConsumer  # type: ignore
    except ImportError:
        return {"ok": False, "error": "kafka-python is not installed"}

    topic = validate_topic(topic)
    gid = (group_id or "").strip() or f"demo-hub-kafka-lab-{uuid.uuid4().hex[:14]}"

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_list(),
        group_id=gid,
        enable_auto_commit=False,
        auto_offset_reset=auto_offset_reset,
        consumer_timeout_ms=max(timeout_ms, 3000),
        max_poll_records=min(max(1, max_messages), 500),
        client_id="demo-hub-kafka-lab-consumer",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    out: list[dict[str, Any]] = []
    t0 = time.perf_counter()
    deadline = t0 + timeout_ms / 1000.0
    try:
        consumer.subscribe([topic])
        # Wait for partition assignment
        while time.perf_counter() < deadline and not consumer.assignment():
            consumer.poll(timeout_ms=300)
        assigned = consumer.assignment()
        if not assigned:
            return {
                "ok": False,
                "error": "no partition assignment (topic missing or timeout)",
                "topic": topic,
                "group_id": gid,
            }

        while len(out) < max_messages and time.perf_counter() < deadline:
            remaining_ms = max(100, int((deadline - time.perf_counter()) * 1000))
            polled = consumer.poll(timeout_ms=min(2000, remaining_ms))
            if not polled:
                continue
            for _tp, batch in polled.items():
                for rec in batch:
                    out.append(
                        {
                            "partition": rec.partition,
                            "offset": rec.offset,
                            "timestamp": rec.timestamp,
                            "key": rec.key,
                            "value": rec.value,
                        }
                    )
                    if len(out) >= max_messages:
                        break
                if len(out) >= max_messages:
                    break
    except Exception as e:
        return {"ok": False, "error": str(e), "topic": topic, "group_id": gid}
    finally:
        consumer.close()

    elapsed = time.perf_counter() - t0
    return {
        "ok": True,
        "topic": topic,
        "group_id": gid,
        "messages": out,
        "count": len(out),
        "elapsed_sec": round(elapsed, 4),
        "auto_offset_reset": auto_offset_reset,
    }


def describe_snippet() -> dict[str, Any]:
    """Short cheat-sheet for learning paths (no broker AdminClient — avoids extra deps)."""
    return {
        "producer_knobs": {
            "acks": "0 (fire-and-forget), 1 (leader ack), all / idempotent (durable)",
            "linger_ms": "batching delay vs latency",
            "batch_size": "bytes before sender flush pressure",
            "compression": "CPU vs network trade-off",
            "key_mode": "same key → single partition → ordering for that key",
        },
        "consumer_notes": {
            "group_id": "empty → random ephemeral group each request",
            "auto_offset_reset": "earliest reads from beginning for new group",
            "max_messages": "poll loop stops early when enough records collected",
        },
        "next_steps": [
            "Compare throughput with linger_ms=0 vs 20 and acks=1 vs all.",
            "Use key_mode=fixed and observe single-partition stickiness via partition field.",
            "Open two browsers with different group_id strings to mimic competing consumers.",
        ],
    }
