from __future__ import annotations

from fastapi import FastAPI, HTTPException

from app.schemas import CommandOnOFF, DevMessage


def init_app(storage, publisher) -> FastAPI:
    app = FastAPI()
    app.state.storage = storage
    app.state.publisher = publisher

    # expecting to receive the decoded message 
    @app.post("/message")
    def handle_message(message: DevMessage):
        

        rows_to_save = []
        # Only save properties with code "temp_interior" or "instant_power"
        # each property will be saved as a separate row in BigQuery with the same devId and productId
        for prop in message.bizData.properties:
            if prop.code in ("temp_interior", "instant_power"):
                rows_to_save.append(
                    {
                        "devId": message.bizData.devId,
                        "productId": message.bizData.productId,
                        "code": prop.code,
                        "value": prop.value,
                        "time": prop.time,
                    }
                )

        app.state.storage.add(rows_to_save)
        return {"rows_saved": len(rows_to_save)}

    @app.post("/send")
    def handle_command(command: CommandOnOFF):
        commande = {"switch": command.switch, "devId": command.device_id}
        res = app.state.publisher.publish("send_command", commande)
        return {"result": res}  

    @app.get("/report")
    def generate_report() :
        return app.state.storage.generate_report()

    return app
