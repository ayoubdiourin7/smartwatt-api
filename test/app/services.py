import json
from typing import Any, Protocol


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


class BigQuerySercice:
    def __init__(
        self,
        gcp_project_id: str,
        dataset: str,
        table: str,
        test_local: bool = True,
        latest_value_cache: LatestValueCache | None = None,
        redis_url: str | None = None,
        redis_ttl_seconds: int | None = None,
    ):
        self.test_local = test_local
        self.rows: list[dict[str, Any]] = []

        if not self.test_local:
            from google.cloud import bigquery

            self.bigquery = bigquery
            self.client = bigquery.Client(project=gcp_project_id)
            self.table_id = f"{gcp_project_id}.{dataset}.{table}"

        if latest_value_cache is not None:
            self.latest_value_cache = latest_value_cache
        elif self.test_local:
            self.latest_value_cache = InMemoryLatestValueCache()
        elif redis_url:
            self.latest_value_cache = RedisLatestValueCache(
                redis_url=redis_url,
                ttl_seconds=redis_ttl_seconds,
            )
        else:
            raise ValueError(
                "Production mode requires a shared latest value cache such as Redis."
            )

    def add(self, rows: list[dict[str, Any]]):
        if not rows:
            return 0

        rows_to_save, latest_values_to_cache = self._filter_unchanged_rows(rows)
        if not rows_to_save:
            return 0

        if self.test_local:
            self.rows.extend(rows_to_save)
            self._store_latest_values(latest_values_to_cache)
            return len(rows_to_save)

        errors = self.client.insert_rows_json(self.table_id, rows_to_save)
        if errors:
            raise RuntimeError(f"BigQuery inserting is failed: {errors}")

        self._store_latest_values(latest_values_to_cache)
        return len(rows_to_save)

    def _filter_unchanged_rows(self, rows: list[dict[str, Any]]):
        rows_to_save = []
        latest_values_to_cache: dict[CacheKey, Any] = {}

        for row in rows:
            row_key = (row["devId"], row["code"])
            previous_value = latest_values_to_cache.get(row_key)
            if previous_value is None:
                previous_value = self._get_latest_value(row_key)

            if previous_value == row["value"]:
                continue

            rows_to_save.append(row)
            latest_values_to_cache[row_key] = row["value"]

        return rows_to_save, latest_values_to_cache

    def _store_latest_values(self, latest_values_to_cache: dict[CacheKey, Any]):
        for row_key, value in latest_values_to_cache.items():
            self.latest_value_cache.set(row_key, value)

    def _get_latest_value(self, row_key: CacheKey):
        previous_value = self.latest_value_cache.get(row_key)
        if previous_value is not None:
            return previous_value

        if self.test_local:
            return None

        query = f"""
            SELECT value
            FROM `{self.table_id}`
            WHERE devId = @dev_id
              AND code = @code
            ORDER BY time DESC
            LIMIT 1
        """
        job_config = self.bigquery.QueryJobConfig(
            query_parameters=[
                self.bigquery.ScalarQueryParameter("dev_id", "STRING", row_key[0]),
                self.bigquery.ScalarQueryParameter("code", "STRING", row_key[1]),
            ]
        )
        results = list(self.client.query(query, job_config=job_config).result())
        if not results:
            return None

        previous_value = results[0]["value"]
        self.latest_value_cache.set(row_key, previous_value)
        return previous_value

    def generate_report(self) :
        if not self.test_local:
            query = f"""
                SELECT
                DATE(TIMESTAMP_MILLIS(time)) as date,
                FORMAT_TIMESTAMP('%H:00', TIMESTAMP_MILLIS(time), 'UTC') AS hr,
                SUM(value) AS valuebyhour
                FROM `{self.table_id}`
                WHERE code = 'instant_power'
                GROUP BY date, hr
                ORDER BY date, hr
            """
            results = self.client.query(query).result()

            report = {}
            for row in results:
                date = str(row["date"])
                hr = row["hr"]
                valuebyhour = row["valuebyhour"]
                datevalues = report.get(date, {})
                datevalues[hr] = valuebyhour
                report[date] = datevalues
            return report

        else:
            print("Mock generating report")
            return {
                "2024-12-18": {
                    "00:00": 24577,
                    "01:00": 42304,
                    "23:00": 99228
                },
                "2024-12-19": {
                    "00:00": 24577,
                    "01:00": 42304,
                    "23:00": 99228
                }
            }





class PubSubSercice:
    def __init__(self, gcp_project_id: str, test_local: bool = True):
        self.test_local = test_local
        self.messages: list[tuple[str, dict[str, Any]]] = []

        if not self.test_local:
            from google.cloud import pubsub_v1
            self.publisher_client = pubsub_v1.PublisherClient()
            self.project_id = gcp_project_id

    def publish(self, topic: str, msg: dict[str, Any]) :
        if self.test_local:
            self.messages.append((topic, msg))
            return "mock publish"

        topic_path = self.publisher_client.topic_path(self.project_id, topic)
        msgBytes = json.dumps(msg).encode("utf-8")
        res = self.publisher_client.publish(topic_path, data=msgBytes).result()
        return res
