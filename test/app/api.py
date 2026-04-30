from __future__ import annotations

from fastapi import FastAPI

from app.schemas import CommandOnOFF, DevMessage

SUPPORTED_PROPERTY_CODES = {"temp_interior", "instant_power"}


def init_app(storage, publisher) -> FastAPI:
    app = FastAPI()
    app.state.storage = storage
    app.state.publisher = publisher

    # expecting to receive the decoded message
    @app.post("/message")
    def handle_message(message: DevMessage):
        # The API keeps only supported codes, then the storage layer
        # skips rows whose value did not change since the last saved reading.
        rows_to_save = [
            {
                "devId": message.bizData.devId,
                "productId": message.bizData.productId,
                "code": prop.code,
                "value": prop.value,
                "time": prop.time,
            }
            for prop in message.bizData.properties
            if prop.code in SUPPORTED_PROPERTY_CODES
        ]

        rows_saved = app.state.storage.add(rows_to_save)
        return {"rows_saved": rows_saved}

    @app.post("/send")
    def handle_command(command: CommandOnOFF):
        commande = {"switch": command.switch, "devId": command.device_id}
        res = app.state.publisher.publish("send_command", commande)
        return {"result": res}  

    @app.get("/report")
    def generate_report() :
        return app.state.storage.generate_report()

    return app
