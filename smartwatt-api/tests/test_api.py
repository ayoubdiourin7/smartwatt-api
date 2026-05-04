from fastapi.testclient import TestClient

from app.api import INGESTION_TOPIC, init_app
from app.services import (
    BigQueryBatchWorker,
    BigQuerySercice,
    PubSubSercice,
    RedisLatestValueCache,
)


class FakeLatestValueCache:
    def __init__(self, initial_values=None):
        self.values = dict(initial_values or {})

    def get(self, row_key):
        return self.values.get(row_key)

    def set(self, row_key, value):
        self.values[row_key] = value


class FakeRedisClient:
    def __init__(self):
        self.values = {}
        self.set_calls = []

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value, ex=None):
        self.values[key] = value
        self.set_calls.append((key, value, ex))


class FakeClock:
    def __init__(self, start=0.0):
        self.current = start

    def __call__(self):
        return self.current

    def advance(self, seconds):
        self.current += seconds


def new_app(
    *,
    batch_size=500,
    flush_interval_seconds=60.0,
    latest_value_cache=None,
    clock=None,
):
    storage = BigQuerySercice(gcp_project_id="", dataset="", table="", test_local=True)
    publisher = PubSubSercice(gcp_project_id="", test_local=True)
    worker = BigQueryBatchWorker(
        publisher=publisher,
        storage=storage,
        ingestion_topic=INGESTION_TOPIC,
        latest_value_cache=latest_value_cache,
        batch_size=batch_size,
        flush_interval_seconds=flush_interval_seconds,
        clock=clock,
    )
    app = init_app(storage=storage, publisher=publisher, worker=worker)
    return TestClient(app), worker, publisher, storage


def build_message(properties, product_id="ixhko1cls7lzpwsf"):
    return {
        "bizCode": "devicePropertyMessage",
        "bizData": {
            "devId": "bfadafebb608a154206aqu",
            "dataId": "000627D1B97B56724DF59C2F6647000F",
            "productId": product_id,
            "properties": properties,
        },
        "ts": 1732631573782,
    }


def test_post_message_enqueues_supported_codes():
    client, _, publisher, _ = new_app()

    message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
            {"code": "temp_interior", "dpId": 2, "time": 1732631573783, "value": 21},
            {"code": "tofilter", "dpId": 3, "time": 1732631573784, "value": 2502},
        ]
    )

    response = client.post("/message", json=message)

    assert response.status_code == 200
    assert response.json() == {"rows_enqueued": 2}
    assert list(publisher.topics[INGESTION_TOPIC]) == [
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2500,
            "time": 1732631573782,
        },
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "temp_interior",
            "value": 21,
            "time": 1732631573783,
        },
    ]


def test_batch_worker_deduplicates_unchanged_values_before_loading():
    client, worker, _, storage = new_app()

    first_message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
        ]
    )
    duplicate_message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631574782, "value": 2500},
        ]
    )
    changed_message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631575782, "value": 2600},
        ]
    )

    assert client.post("/message", json=first_message).json() == {"rows_enqueued": 1}
    assert client.post("/message", json=duplicate_message).json() == {"rows_enqueued": 1}
    assert client.post("/message", json=changed_message).json() == {"rows_enqueued": 1}

    drain_stats = worker.drain()

    assert drain_stats == {"messages_pulled": 3, "rows_loaded": 2}
    assert storage.rows == [
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2500,
            "time": 1732631573782,
        },
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2600,
            "time": 1732631575782,
        },
    ]


def test_batch_worker_uses_shared_cache_for_deduplication():
    cache = FakeLatestValueCache(
        {
            ("bfadafebb608a154206aqu", "instant_power"): 2500,
        }
    )
    _, worker, publisher, storage = new_app(latest_value_cache=cache)

    publisher.publish_many(
        INGESTION_TOPIC,
        [
            {
                "devId": "bfadafebb608a154206aqu",
                "productId": "ixhko1cls7lzpwsf",
                "code": "instant_power",
                "value": 2500,
                "time": 1732631576782,
            },
            {
                "devId": "bfadafebb608a154206aqu",
                "productId": "ixhko1cls7lzpwsf",
                "code": "instant_power",
                "value": 2600,
                "time": 1732631577782,
            },
        ],
    )

    drain_stats = worker.drain()

    assert drain_stats == {"messages_pulled": 2, "rows_loaded": 1}
    assert storage.rows == [
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2600,
            "time": 1732631577782,
        }
    ]
    assert cache.values == {
        ("bfadafebb608a154206aqu", "instant_power"): 2600,
    }


def test_redis_latest_value_cache_serializes_values():
    redis_client = FakeRedisClient()
    cache = RedisLatestValueCache(
        redis_url="redis://localhost:6379/0",
        key_prefix="device_property",
        ttl_seconds=300,
        client=redis_client,
    )
    row_key = ("bfadafebb608a154206aqu", "instant_power")

    cache.set(row_key, 2500)

    assert cache.get(row_key) == 2500
    assert redis_client.set_calls == [
        (
            "device_property:bfadafebb608a154206aqu:instant_power",
            "2500",
            300,
        )
    ]


def test_batch_worker_deduplicates_per_device_and_code_across_products():
    client, worker, _, storage = new_app()

    first_message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
        ],
        product_id="product-a",
    )
    duplicate_message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631574782, "value": 2500},
        ],
        product_id="product-b",
    )
    changed_message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631575782, "value": 2600},
        ],
        product_id="product-b",
    )

    client.post("/message", json=first_message)
    client.post("/message", json=duplicate_message)
    client.post("/message", json=changed_message)
    worker.drain()

    assert storage.rows == [
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "product-a",
            "code": "instant_power",
            "value": 2500,
            "time": 1732631573782,
        },
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "product-b",
            "code": "instant_power",
            "value": 2600,
            "time": 1732631575782,
        },
    ]


def test_batch_worker_flushes_when_batch_size_is_reached():
    client, worker, _, storage = new_app(batch_size=2)

    client.post(
        "/message",
        json=build_message(
            [
                {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
            ]
        ),
    )
    client.post(
        "/message",
        json=build_message(
            [
                {"code": "instant_power", "dpId": 1, "time": 1732631574782, "value": 2600},
            ]
        ),
    )

    run_stats = worker.run_once()

    assert run_stats == {"messages_pulled": 2, "rows_loaded": 2, "buffer_size": 0}
    assert storage.rows == [
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2500,
            "time": 1732631573782,
        },
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2600,
            "time": 1732631574782,
        },
    ]


def test_batch_worker_flushes_when_flush_interval_is_reached():
    clock = FakeClock()
    client, worker, _, storage = new_app(
        batch_size=10,
        flush_interval_seconds=5.0,
        clock=clock,
    )

    client.post(
        "/message",
        json=build_message(
            [
                {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
            ]
        ),
    )

    first_run = worker.run_once()
    clock.advance(5.0)
    second_run = worker.run_once()

    assert first_run == {"messages_pulled": 1, "rows_loaded": 0, "buffer_size": 1}
    assert second_run == {"messages_pulled": 0, "rows_loaded": 1, "buffer_size": 0}
    assert storage.rows == [
        {
            "devId": "bfadafebb608a154206aqu",
            "productId": "ixhko1cls7lzpwsf",
            "code": "instant_power",
            "value": 2500,
            "time": 1732631573782,
        }
    ]


def test_post_send():
    client, _, publisher, _ = new_app()

    response = client.post(
        "/send",
        json={"device_id": "bfadafebb608a154206aqu", "switch": True},
    )

    assert response.status_code == 200
    assert response.json() == {"result": "mock publish"}
    assert publisher.messages[-1] == (
        "send_command",
        {"switch": True, "devId": "bfadafebb608a154206aqu"},
    )


def test_get_report_aggregates_loaded_rows_by_day_and_hour():
    client, worker, _, _ = new_app()

    client.post(
        "/message",
        json=build_message(
            [
                {"code": "instant_power", "dpId": 1, "time": 1734480600000, "value": 100},
            ]
        ),
    )
    client.post(
        "/message",
        json=build_message(
            [
                {"code": "instant_power", "dpId": 1, "time": 1734483000000, "value": 150},
            ]
        ),
    )
    client.post(
        "/message",
        json=build_message(
            [
                {"code": "temp_interior", "dpId": 2, "time": 1734483300000, "value": 20},
                {"code": "instant_power", "dpId": 1, "time": 1734483900000, "value": 90},
            ]
        ),
    )
    client.post(
        "/message",
        json=build_message(
            [
                {"code": "instant_power", "dpId": 1, "time": 1734567000000, "value": 60},
            ]
        ),
    )
    worker.drain()

    response = client.get("/report")

    assert response.status_code == 200
    assert response.json() == {
        "2024-12-18": {
            "00:00": 250,
            "01:00": 90,
        },
        "2024-12-19": {
            "00:00": 60,
        },
    }
