import json
from typing import Any


class BigQuerySercice:
    def __init__(self, gcp_project_id: str, dataset: str, table: str, test_local: bool = True):
        self.test_local = test_local
        self.rows: list[dict[str, Any]] = []

        if not self.test_local:
            from google.cloud import bigquery
            self.client = bigquery.Client(project=gcp_project_id)
            self.table_id = f"{gcp_project_id}.{dataset}.{table}"

    def add(self, rows: list[dict[str, Any]]):
        if not rows:
            return

        if self.test_local:
            self.rows.extend(rows)
            return

        errors = self.client.insert_rows_json(self.table_id, rows)
        if errors:
            raise RuntimeError(f"BigQuery inserting is failed: {errors}")

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

