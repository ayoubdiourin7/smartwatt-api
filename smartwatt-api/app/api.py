from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.schemas import CommandOnOFF, DevMessage

INGESTION_TOPIC = "device_property_updates"
SUPPORTED_PROPERTY_CODES = {"temp_interior", "instant_power"}


def init_app(storage, publisher, worker=None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            yield
        finally:
            if app.state.batch_worker is not None:
                app.state.batch_worker.flush()

    app = FastAPI(lifespan=lifespan)
    app.state.storage = storage
    app.state.publisher = publisher
    app.state.batch_worker = worker

    # expecting to receive the decoded message
    @app.post("/message")
    def handle_message(message: DevMessage):
        # The API keeps only supported codes and pushes them into Pub/Sub.
        # Deduplication happens in the batch worker right before the BigQuery load.
        rows_to_enqueue = [
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

        rows_enqueued = app.state.publisher.publish_many(INGESTION_TOPIC, rows_to_enqueue)
        return {"rows_enqueued": rows_enqueued}

    @app.post("/send")
    def handle_command(command: CommandOnOFF):
        commande = {"switch": command.switch, "devId": command.device_id}
        res = app.state.publisher.publish("send_command", commande)
        return {"result": res}  

    @app.get("/report")
    def generate_report() :
        return app.state.storage.generate_report()

    return app
