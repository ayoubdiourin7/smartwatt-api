from __future__ import annotations

import json
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Protocol


CacheKey = tuple[str, str]


class LatestValueCache(Protocol):
    def get(self, row_key: CacheKey) -> Any | None:
        ...

    def set(self, row_key: CacheKey, value: Any) -> None:
        ...


class InMemoryLatestValueCache:
    def __init__(self):
        self.values: dict[CacheKey, Any] = {}

    def get(self, row_key: CacheKey) -> Any | None:
        return self.values.get(row_key)

    def set(self, row_key: CacheKey, value: Any) -> None:
        self.values[row_key] = value


class RedisLatestValueCache:
    def __init__(
        self,
        redis_url: str,
        key_prefix: str = "device_property",
        ttl_seconds: int | None = None,
        client=None,
    ):
        self.key_prefix = key_prefix
        self.ttl_seconds = ttl_seconds

        if client is not None:
            self.client = client
            return

        from redis import Redis

        self.client = Redis.from_url(redis_url, decode_responses=True)

    def get(self, row_key: CacheKey) -> Any | None:
        payload = self.client.get(self._build_key(row_key))
        if payload is None:
            return None
        return json.loads(payload)

    def set(self, row_key: CacheKey, value: Any) -> None:
        self.client.set(
            self._build_key(row_key),
            json.dumps(value),
            ex=self.ttl_seconds,
        )

    def _build_key(self, row_key: CacheKey) -> str:
        dev_id, code = row_key
        return f"{self.key_prefix}:{dev_id}:{code}"


class LatestValueDeduplicator:
    def __init__(self, latest_value_cache: LatestValueCache):
        self.latest_value_cache = latest_value_cache

    def filter_changed_rows(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[CacheKey, Any]]:
        rows_to_store = []
        latest_values_to_cache: dict[CacheKey, Any] = {}

        for row in rows:
            row_key = (row["devId"], row["code"])
            previous_value = latest_values_to_cache.get(row_key)
            if previous_value is None:
                previous_value = self.latest_value_cache.get(row_key)

            if previous_value == row["value"]:
                continue

            rows_to_store.append(row)
            latest_values_to_cache[row_key] = row["value"]

        return rows_to_store, latest_values_to_cache

    def commit(self, latest_values_to_cache: dict[CacheKey, Any]) -> None:
        for row_key, value in latest_values_to_cache.items():
            self.latest_value_cache.set(row_key, value)


class BigQuerySercice:
    def __init__(
        self,
        gcp_project_id: str,
        dataset: str,
        table: str,
        test_local: bool = True,
    ):
        self.test_local = test_local
        self.rows: list[dict[str, Any]] = []

        if not self.test_local:
            from google.cloud import bigquery

            self.bigquery = bigquery
            self.client = bigquery.Client(project=gcp_project_id)
            self.table_id = f"{gcp_project_id}.{dataset}.{table}"

    def load_rows(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0

        if self.test_local:
            self.rows.extend(rows)
            return len(rows)

        job_config = self.bigquery.LoadJobConfig(
            write_disposition=self.bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = self.client.load_table_from_json(
            rows,
            self.table_id,
            job_config=job_config,
        )
        load_job.result()
        if load_job.errors:
            raise RuntimeError(f"BigQuery batch load failed: {load_job.errors}")
        return len(rows)

    def add(self, rows: list[dict[str, Any]]) -> int:
        return self.load_rows(rows)

    def generate_report(self):
        if not self.test_local:
            query = f"""
                SELECT
                  DATE(TIMESTAMP_MILLIS(time)) AS date,
                  FORMAT_TIMESTAMP('%H:00', TIMESTAMP_MILLIS(time), 'UTC') AS hr,
                  SUM(value) AS valuebyhour
                FROM `{self.table_id}`
                WHERE code = 'instant_power'
                GROUP BY date, hr
                ORDER BY date, hr
            """
            results = self.client.query(query).result()

            report: dict[str, dict[str, Any]] = {}
            for row in results:
                date = str(row["date"])
                hour = row["hr"]
                value_by_hour = row["valuebyhour"]
                day_values = report.setdefault(date, {})
                day_values[hour] = value_by_hour
            return report

        report: dict[str, dict[str, Any]] = {}
        for row in self.rows:
            if row["code"] != "instant_power":
                continue

            dt = datetime.fromtimestamp(row["time"] / 1000, tz=timezone.utc)
            date = dt.date().isoformat()
            hour = dt.strftime("%H:00")
            day_values = report.setdefault(date, {})
            day_values[hour] = day_values.get(hour, 0) + row["value"]

        sorted_report: dict[str, dict[str, Any]] = {}
        for date in sorted(report):
            sorted_report[date] = {
                hour: report[date][hour] for hour in sorted(report[date])
            }
        return sorted_report


@dataclass
class LocalPulledMessage:
    topic: str
    payload: dict[str, Any]


@dataclass
class PulledPubSubMessage:
    subscription_path: str
    ack_id: str
    payload: dict[str, Any]


class PubSubSercice:
    def __init__(self, gcp_project_id: str, test_local: bool = True):
        self.test_local = test_local
        self.messages: list[tuple[str, dict[str, Any]]] = []
        self.topics: dict[str, deque[dict[str, Any]]] = defaultdict(deque)

        if not self.test_local:
            from google.cloud import pubsub_v1

            self.publisher_client = pubsub_v1.PublisherClient()
            self.subscriber_client = pubsub_v1.SubscriberClient()
            self.project_id = gcp_project_id

    def publish(self, topic: str, msg: dict[str, Any]):
        if self.test_local:
            self.messages.append((topic, msg))
            self.topics[topic].append(msg)
            return "mock publish"

        topic_path = self.publisher_client.topic_path(self.project_id, topic)
        message_bytes = json.dumps(msg).encode("utf-8")
        return self.publisher_client.publish(topic_path, data=message_bytes).result()

    def publish_many(self, topic: str, messages: list[dict[str, Any]]) -> int:
        published_count = 0
        for message in messages:
            self.publish(topic, message)
            published_count += 1
        return published_count

    def pull(
        self,
        *,
        topic: str | None = None,
        subscription: str | None = None,
        max_messages: int = 100,
    ) -> list[LocalPulledMessage | PulledPubSubMessage]:
        if self.test_local:
            if topic is None:
                raise ValueError("topic is required in local mode")

            queue = self.topics[topic]
            pulled_messages: list[LocalPulledMessage] = []
            for _ in range(min(max_messages, len(queue))):
                pulled_messages.append(
                    LocalPulledMessage(topic=topic, payload=queue.popleft())
                )
            return pulled_messages

        if subscription is None:
            raise ValueError("subscription is required in production mode")

        subscription_path = self._build_subscription_path(subscription)
        response = self.subscriber_client.pull(
            request={
                "subscription": subscription_path,
                "max_messages": max_messages,
            }
        )

        pulled_messages: list[PulledPubSubMessage] = []
        for received_message in response.received_messages:
            payload = json.loads(received_message.message.data.decode("utf-8"))
            pulled_messages.append(
                PulledPubSubMessage(
                    subscription_path=subscription_path,
                    ack_id=received_message.ack_id,
                    payload=payload,
                )
            )
        return pulled_messages

    def ack(self, pulled_messages: list[LocalPulledMessage | PulledPubSubMessage]) -> None:
        if self.test_local or not pulled_messages:
            return

        subscription_path = pulled_messages[0].subscription_path
        ack_ids = [message.ack_id for message in pulled_messages]
        self.subscriber_client.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )

    def requeue(self, topic: str, pulled_messages: list[LocalPulledMessage]) -> None:
        if not self.test_local or not pulled_messages:
            return

        queue = self.topics[topic]
        for message in reversed(pulled_messages):
            queue.appendleft(message.payload)

    def _build_subscription_path(self, subscription: str) -> str:
        if subscription.startswith("projects/"):
            return subscription
        return self.subscriber_client.subscription_path(self.project_id, subscription)


class BigQueryBatchWorker:
    def __init__(
        self,
        publisher: PubSubSercice,
        storage: BigQuerySercice,
        ingestion_topic: str,
        latest_value_cache: LatestValueCache | None = None,
        batch_size: int = 500,
        flush_interval_seconds: float = 60.0,
        max_pull_messages: int = 500,
        subscription_name: str | None = None,
        clock: Callable[[], float] | None = None,
    ):
        self.publisher = publisher
        self.storage = storage
        self.ingestion_topic = ingestion_topic
        self.subscription_name = subscription_name
        self.batch_size = batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.max_pull_messages = max_pull_messages
        self.clock = clock or time.monotonic
        self.deduplicator = LatestValueDeduplicator(
            latest_value_cache or InMemoryLatestValueCache()
        )
        self.buffered_messages: list[LocalPulledMessage | PulledPubSubMessage] = []
        self.buffer_started_at: float | None = None

    def run_once(self) -> dict[str, int]:
        pulled_messages = self._pull_messages()
        if pulled_messages:
            self._add_to_buffer(pulled_messages)

        rows_loaded = 0
        while len(self.buffered_messages) >= self.batch_size:
            rows_loaded += self._flush_batch(self.batch_size)

        if self._is_flush_due():
            rows_loaded += self._flush_batch()

        return {
            "messages_pulled": len(pulled_messages),
            "rows_loaded": rows_loaded,
            "buffer_size": len(self.buffered_messages),
        }

    def drain(self) -> dict[str, int]:
        total_messages_pulled = 0
        total_rows_loaded = 0

        while True:
            pulled_messages = self._pull_messages()
            if not pulled_messages:
                break

            total_messages_pulled += len(pulled_messages)
            self._add_to_buffer(pulled_messages)

            while len(self.buffered_messages) >= self.batch_size:
                total_rows_loaded += self._flush_batch(self.batch_size)

        if self.buffered_messages:
            total_rows_loaded += self._flush_batch()

        return {
            "messages_pulled": total_messages_pulled,
            "rows_loaded": total_rows_loaded,
        }

    def flush(self) -> int:
        return self._flush_batch()

    def _pull_messages(self) -> list[LocalPulledMessage | PulledPubSubMessage]:
        if self.publisher.test_local:
            return self.publisher.pull(
                topic=self.ingestion_topic,
                max_messages=self.max_pull_messages,
            )

        if self.subscription_name is None:
            raise ValueError("subscription_name is required in production mode")

        return self.publisher.pull(
            subscription=self.subscription_name,
            max_messages=self.max_pull_messages,
        )

    def _add_to_buffer(
        self, pulled_messages: list[LocalPulledMessage | PulledPubSubMessage]
    ) -> None:
        if not self.buffered_messages:
            self.buffer_started_at = self.clock()
        self.buffered_messages.extend(pulled_messages)

    def _is_flush_due(self) -> bool:
        if not self.buffered_messages or self.buffer_started_at is None:
            return False
        return (self.clock() - self.buffer_started_at) >= self.flush_interval_seconds

    def _flush_batch(self, batch_size: int | None = None) -> int:
        if not self.buffered_messages:
            return 0

        if batch_size is None or batch_size >= len(self.buffered_messages):
            messages_to_flush = self.buffered_messages
            self.buffered_messages = []
        else:
            messages_to_flush = self.buffered_messages[:batch_size]
            self.buffered_messages = self.buffered_messages[batch_size:]

        payloads = [message.payload for message in messages_to_flush]
        changed_rows, latest_values_to_cache = self.deduplicator.filter_changed_rows(
            payloads
        )

        try:
            rows_loaded = self.storage.load_rows(changed_rows)
            self.deduplicator.commit(latest_values_to_cache)
            self.publisher.ack(messages_to_flush)
        except Exception:
            if self.publisher.test_local:
                self.publisher.requeue(self.ingestion_topic, messages_to_flush)
            raise
        finally:
            self.buffer_started_at = self.clock() if self.buffered_messages else None

        return rows_loaded
