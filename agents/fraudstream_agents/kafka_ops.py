"""Observabilité Kafka pour l'agent Data Ops : lag des consommateurs, débit, santé des topics."""
from __future__ import annotations

from typing import Any

from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

from .config import Settings, settings as default_settings


class KafkaOps:
    def __init__(self, cfg: Settings = default_settings):
        self.bootstrap = cfg.kafka_bootstrap_servers

    def topics(self) -> list[str]:
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
        try:
            return sorted(t for t in admin.list_topics() if not t.startswith("__"))
        finally:
            admin.close()

    def consumer_lag(self, group_id: str) -> dict[str, Any]:
        """Lag = end_offset - committed_offset, par partition, pour un groupe de consommateurs."""
        admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap, enable_auto_commit=False)
        try:
            committed = admin.list_consumer_group_offsets(group_id)
            if not committed:
                return {"group": group_id, "partitions": [], "total_lag": None, "note": "aucun offset committé"}
            tps = list(committed.keys())
            ends = consumer.end_offsets(tps)
            parts = []
            for tp in tps:
                lag = ends[tp] - committed[tp].offset
                parts.append({"topic": tp.topic, "partition": tp.partition, "committed": committed[tp].offset,
                              "end": ends[tp], "lag": lag})
            return {"group": group_id, "partitions": parts, "total_lag": sum(p["lag"] for p in parts)}
        finally:
            consumer.close()
            admin.close()

    def topic_offsets(self, topic: str) -> dict[str, Any]:
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap, enable_auto_commit=False)
        try:
            partitions = consumer.partitions_for_topic(topic) or set()
            tps = [TopicPartition(topic, p) for p in partitions]
            begin, end = consumer.beginning_offsets(tps), consumer.end_offsets(tps)
            return {"topic": topic, "partitions": len(tps),
                    "messages": sum(end[tp] - begin[tp] for tp in tps),
                    "end_offsets": {tp.partition: end[tp] for tp in tps}}
        finally:
            consumer.close()
