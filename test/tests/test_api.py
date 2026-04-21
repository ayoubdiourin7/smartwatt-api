
from fastapi.testclient import TestClient

from app.api import init_app
from app.services import BigQuerySercice, PubSubSercice


# new app for each test to ensure isolation 
def newApp() :
    storage = BigQuerySercice(gcp_project_id="", dataset="", table="", test_local=True)
    publisher = PubSubSercice(gcp_project_id="", test_local=True)
    app = init_app(storage=storage, publisher=publisher)
    return TestClient(app)


def test_post_message():
    client = newApp()

    message = {
        "bizCode": "devicePropertyMessage",
        "bizData": {
            "devId": "bfadafebb608a154206aqu",
            "dataId": "000627D1B97B56724DF59C2F6647000F",
            "productId": "ixhko1cls7lzpwsf",
            "properties": [
                {"code": "instant_power", "dpId": 1, "time": 1732631573782, "value": 2500},
                {"code": "temp_interior", "dpId": 2, "time": 1732631573783, "value": 2501},
                {"code": "tofilter", "dpId": 3, "time": 1732631573784, "value": 2502},
            ],
        },
        "ts": 1732631573782,
    }

    response = client.post("/message", json=message)

    assert response.status_code == 200
    assert response.json() == {"rows_saved": 2}


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
