
from fastapi.testclient import TestClient

from app.api import init_app
from app.services import BigQuerySercice, PubSubSercice, RedisLatestValueCache


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


# new app for each test to ensure isolation 
def newApp() :
    storage = BigQuerySercice(gcp_project_id="", dataset="", table="", test_local=True)
    publisher = PubSubSercice(gcp_project_id="", test_local=True)
    app = init_app(storage=storage, publisher=publisher)
    return TestClient(app)


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


def test_post_message_filters_supported_codes():
    client = newApp()

    message = build_message(
        [
            {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
            {"code": "temp_interior", "dpId": 2, "time": 1732631573783, "value": 2501},
            {"code": "tofilter", "dpId": 3, "time": 1732631573784, "value": 2502},
        ]
    )

    response = client.post("/message", json=message)

    assert response.status_code == 200
    assert response.json() == {"rows_saved": 2}
    assert client.app.state.storage.rows == [
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
            "value": 2501,
            "time": 1732631573783,
        },
    ]


def test_post_message_skips_unchanged_supported_values():
    client = newApp()

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

    first_response = client.post("/message", json=first_message)
    second_response = client.post("/message", json=duplicate_message)
    third_response = client.post("/message", json=changed_message)

    assert first_response.status_code == 200
    assert second_response.status_code == 200
    assert third_response.status_code == 200
    assert first_response.json() == {"rows_saved": 1}
    assert second_response.json() == {"rows_saved": 0}
    assert third_response.json() == {"rows_saved": 1}
    assert client.app.state.storage.rows == [
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


def test_storage_uses_shared_cache_for_deduplication():
    cache = FakeLatestValueCache(
        {
            ("bfadafebb608a154206aqu", "instant_power"): 2500,
        }
    )
    storage = BigQuerySercice(
        gcp_project_id="",
        dataset="",
        table="",
        test_local=True,
        latest_value_cache=cache,
    )

    rows_saved = storage.add(
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
        ]
    )

    assert rows_saved == 1
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


def test_post_message_deduplicates_per_device_and_code_across_products():
    client = newApp()

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

    first_response = client.post("/message", json=first_message)
    second_response = client.post("/message", json=duplicate_message)
    third_response = client.post("/message", json=changed_message)

    assert first_response.json() == {"rows_saved": 1}
    assert second_response.json() == {"rows_saved": 0}
    assert third_response.json() == {"rows_saved": 1}
    assert client.app.state.storage.rows == [
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


def test_post_send() -> None:
    client= newApp()

    response = client.post("/send", json={"device_id": "bfadafebb608a154206aqu", "switch": True})

    assert response.status_code == 200
    assert response.json() == {"result": "mock publish"}
    


def test_get_report():
    client= newApp()

    response = client.get("/report")

    assert response.status_code == 200
    assert response.json() == {
        "2024-12-18": {
            "00:00": 24577,
            "01:00": 42304,
            "23:00": 99228,
        },
        "2024-12-19": {
            "00:00": 24577,
            "01:00": 42304,
            "23:00": 99228,
        },
    }
