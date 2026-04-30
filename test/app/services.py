import json
from typing import Any


class BigQuerySercice:
    def __init__(self, gcp_project_id: str, dataset: str, table: str, test_local: bool = True):
        self.test_local = test_local
        self.rows: list[dict[str, Any]] = []
        self.latest_values: dict[tuple[str, str, str], Any] = {}

        if not self.test_local:
            from google.cloud import bigquery
            self.bigquery = bigquery
            self.client = bigquery.Client(project=gcp_project_id)
            self.table_id = f"{gcp_project_id}.{dataset}.{table}"

    def add(self, rows: list[dict[str, Any]]):
        if not rows:
            return 0

        rows_to_save = self._filter_unchanged_rows(rows)
        if not rows_to_save:
            return 0

        if self.test_local:
            self.rows.extend(rows_to_save)
            return len(rows_to_save)

        errors = self.client.insert_rows_json(self.table_id, rows_to_save)
        if errors:
            raise RuntimeError(f"BigQuery inserting is failed: {errors}")
        return len(rows_to_save)

    def _filter_unchanged_rows(self, rows: list[dict[str, Any]]):
        rows_to_save = []

        for row in rows:
            row_key = (row["devId"], row["productId"], row["code"])
            previous_value = self._get_latest_value(row_key)
            if previous_value == row["value"]:
                continue

            rows_to_save.append(row)
            self.latest_values[row_key] = row["value"]

        return rows_to_save

    def _get_latest_value(self, row_key: tuple[str, str, str]):
        if row_key in self.latest_values:
            return self.latest_values[row_key]

        if self.test_local:
            return None

        query = f"""
            SELECT value
            FROM `{self.table_id}`
            WHERE devId = @dev_id
              AND productId = @product_id
              AND code = @code
            ORDER BY time DESC
            LIMIT 1
        """
        job_config = self.bigquery.QueryJobConfig(
            query_parameters=[
                self.bigquery.ScalarQueryParameter("dev_id", "STRING", row_key[0]),
                self.bigquery.ScalarQueryParameter("product_id", "STRING", row_key[1]),
                self.bigquery.ScalarQueryParameter("code", "STRING", row_key[2]),
            ]
        )
        results = list(self.client.query(query, job_config=job_config).result())
        if not results:
            return None

        previous_value = results[0]["value"]
        self.latest_values[row_key] = previous_value
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
