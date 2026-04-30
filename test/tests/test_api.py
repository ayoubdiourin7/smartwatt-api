
from fastapi.testclient import TestClient

from app.api import init_app
from app.services import BigQuerySercice, PubSubSercice


# new app for each test to ensure isolation 
def newApp() :
    storage = BigQuerySercice(gcp_project_id="", dataset="", table="", test_local=True)
    publisher = PubSubSercice(gcp_project_id="", test_local=True)
    app = init_app(storage=storage, publisher=publisher)
    return TestClient(app)


def build_message(properties):
    return {
        "bizCode": "devicePropertyMessage",
        "bizData": {
            "devId": "bfadafebb608a154206aqu",
            "dataId": "000627D1B97B56724DF59C2F6647000F",
            "productId": "ixhko1cls7lzpwsf",
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
