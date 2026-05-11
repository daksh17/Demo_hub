"""Kafka playground for demo-hub: metadata, producer knobs, short consumer polls."""
from __future__ import annotations

import json
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    enable_auto_commit: bool,
    consumer_instance_tag: str = "",
) -> dict[str, Any]:
    try:
        from kafka import KafkaConsumer  # type: ignore
    except ImportError:
        return {"ok": False, "error": "kafka-python is not installed"}

    topic = validate_topic(topic)
    gid = (group_id or "").strip() or f"demo-hub-kafka-lab-{uuid.uuid4().hex[:14]}"

    c_kwargs: dict[str, Any] = dict(
        bootstrap_servers=bootstrap_list(),
        group_id=gid,
        enable_auto_commit=enable_auto_commit,
        auto_offset_reset=auto_offset_reset,
        consumer_timeout_ms=max(timeout_ms, 3000),
        max_poll_records=min(max(1, max_messages), 500),
        value_deserializer=lambda b: json.loads(b.decode("utf-8")) if b else None,
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    if enable_auto_commit:
        c_kwargs["auto_commit_interval_ms"] = 5000
    client_id = "demo-hub-kafka-lab-consumer"
    if (consumer_instance_tag or "").strip():
        suf = re.sub(r"[^a-zA-Z0-9._-]", "", consumer_instance_tag.strip())[:80]
        client_id = f"{client_id}-{suf}"[:240]
    c_kwargs["client_id"] = client_id
    consumer = KafkaConsumer(**c_kwargs)
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
                "enable_auto_commit": enable_auto_commit,
            }

        assigned_partitions_sorted = sorted(tp.partition for tp in assigned)

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
        return {
            "ok": False,
            "error": str(e),
            "topic": topic,
            "group_id": gid,
            "enable_auto_commit": enable_auto_commit,
        }
    finally:
        if enable_auto_commit:
            try:
                consumer.commit()
            except Exception:
                pass
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
        "enable_auto_commit": enable_auto_commit,
        "assigned_partitions": assigned_partitions_sorted,
        "partitions_seen_in_messages": sorted({m["partition"] for m in out}),
    }


def _topics_for_parallel_workers(
    primary: str, topic_consumer_2: str, topic_consumer_3: str, n: int
) -> list[str]:
    t0 = validate_topic(primary.strip() or DEFAULT_LAB_TOPIC)
    out: list[str] = [t0]
    if n >= 2:
        s2 = (topic_consumer_2 or "").strip()
        out.append(validate_topic(s2) if s2 else t0)
    if n >= 3:
        s3 = (topic_consumer_3 or "").strip()
        out.append(validate_topic(s3) if s3 else t0)
    return out


def consume_poll_parallel(
    *,
    topic: str,
    topic_consumer_2: str = "",
    topic_consumer_3: str = "",
    group_id: str,
    parallel_consumers: int,
    share_consumer_group: bool,
    max_messages: int,
    timeout_ms: int,
    auto_offset_reset: Literal["earliest", "latest"],
    enable_auto_commit: bool,
) -> dict[str, Any]:
    """Run multiple kafka-python consumers concurrently (same process).

    ``share_consumer_group=True``: identical ``group.id`` → cooperative assignment splits partitions.
    ``share_consumer_group=False``: distinct groups ``{base}-inst{i}`` (or independent random ids if base empty).
    Optional ``topic_consumer_2`` / ``topic_consumer_3`` select different topics for parallel instances (blank repeats ``topic``).
    Each consumer may fetch up to ``max_messages`` records (cap applies **per instance**).
    """
    if parallel_consumers <= 1:
        return consume_poll(
            topic=topic,
            group_id=group_id,
            max_messages=max_messages,
            timeout_ms=timeout_ms,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
        )

    topics_used = _topics_for_parallel_workers(
        topic, topic_consumer_2, topic_consumer_3, parallel_consumers
    )
    base = (group_id or "").strip()
    if share_consumer_group:
        gids = [base or f"demo-hub-kafka-lab-{uuid.uuid4().hex[:14]}"] * parallel_consumers
    else:
        if base:
            gids = [f"{base}-inst{i}" for i in range(parallel_consumers)]
        else:
            gids = [f"demo-hub-kafka-lab-{uuid.uuid4().hex[:14]}" for _ in range(parallel_consumers)]

    def _worker(idx: int) -> dict[str, Any]:
        tag = f"p{idx}-{uuid.uuid4().hex[:8]}"
        r = consume_poll(
            topic=topics_used[idx],
            group_id=gids[idx],
            max_messages=max_messages,
            timeout_ms=timeout_ms,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            consumer_instance_tag=tag,
        )
        merged = dict(r)
        merged["parallel_index"] = idx
        return merged

    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=parallel_consumers) as pool:
        futures = [pool.submit(_worker, i) for i in range(parallel_consumers)]
        for fu in as_completed(futures):
            results.append(fu.result())

    results.sort(key=lambda x: int(x.get("parallel_index", 0)))
    ok_all = all(bool(r.get("ok")) for r in results)
    total = sum(int(r.get("count") or 0) for r in results if r.get("ok"))

    return {
        "ok": ok_all,
        "topic": topics_used[0],
        "topics_per_consumer": topics_used,
        "parallel_consumers": parallel_consumers,
        "share_consumer_group": share_consumer_group,
        "group_ids_used": list(gids),
        "total_messages_across_consumers": total,
        "max_messages_per_consumer": max_messages,
        "consumers": results,
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
            "enable_auto_commit": "on + fixed group_id commits offsets after poll — next run resumes after last commit",
            "parallel_consumers": "2–3: shared group.id splits partitions across hub threads; unchecked → separate groups `{base}-inst0`, `-inst1`, …",
            "topics_per_consumer": "optional topic_consumer_2 / topic_consumer_3 fields assign different topics to parallel instances (blank repeats main topic)",
            "max_messages": "poll loop stops early when enough records collected",
            "assigned_partitions": "after subscribe, lists partition IDs owned by that consumer member (same group → disjoint assignment when topic has enough partitions)",
        },
        "next_steps": [
            "Compare throughput with linger_ms=0 vs 20 and acks=1 vs all.",
            "Use key_mode=fixed and observe single-partition stickiness via partition field.",
            "Open two browsers with different group_id strings to mimic competing consumers.",
        ],
    }
