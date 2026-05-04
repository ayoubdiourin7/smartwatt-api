# SmartWatt API

This project is a simplified backend for connected heating devices. It exposes three HTTP endpoints:

- `POST /message` to receive device telemetry
- `POST /send` to send on or off commands to a device
- `GET /report` to return aggregated hourly power consumption

The implementation is intentionally designed around buffered ingestion instead of writing directly from the API into BigQuery.

## Goal of the Architecture

The technical test states that:

- the fleet can be very large
- the platform can receive a high number of messages
- only changed values should be stored
- the data team does not need real-time writes and hourly freshness is acceptable

Because of that, the architecture used here is:

1. Accept HTTP requests quickly
2. Put accepted telemetry into a buffer
3. Consume that buffer in batches
4. Remove unchanged values just before persistence
5. Write batched rows to BigQuery

This avoids using BigQuery as part of the synchronous request path.

## Architecture Diagram

### Ingestion and Reporting Flow

```text
                      +----------------------+
                      | Connected devices    |
                      | or Pub/Sub callback  |
                      +----------+-----------+
                                 |
                                 | POST /message
                                 v
                      +----------------------+
                      | FastAPI application  |
                      | app/api.py           |
                      +----------+-----------+
                                 |
                                 | keep only supported fields
                                 | devId, productId, code, value, time
                                 v
               +------------------------------------------+
               | Pub/Sub topic: device_property_updates   |
               | durable ingestion buffer                 |
               +-------------------+----------------------+
                                   |
                                   | pull messages
                                   v
                    +----------------------------------+
                    | BigQueryBatchWorker              |
                    | app/services.py                  |
                    | - buffer in memory              |
                    | - flush on batch size           |
                    | - flush on time interval        |
                    | - deduplicate unchanged values  |
                    +----------------+-----------------+
                                     |
                                     | changed rows only
                                     v
                    +----------------------------------+
                    | BigQuery                         |
                    | batch append load               |
                    +----------------+-----------------+
                                     |
                                     | aggregated query
                                     v
                    +----------------------------------+
                    | GET /report                      |
                    +----------------------------------+
```

### Command Flow

```text
Client
  |
  | POST /send
  v
FastAPI
  |
  | publish {"switch": bool, "devId": "..."}
  v
Pub/Sub topic: send_command
  |
  v
Device control pipeline
```

## How to Read the Diagram

The diagram has four important stages.

### 1. HTTP ingestion

The entry point is `POST /message`.

The API does not try to write to BigQuery directly anymore. Instead, it:

- validates the request body through Pydantic
- keeps only supported telemetry codes
- converts each supported property into a flat row
- publishes those rows to a Pub/Sub topic named `device_property_updates`

This logic is in [app/api.py](./app/api.py).

The important design point is that the API returns after enqueueing. It does not wait for BigQuery.

### 2. Pub/Sub as the buffer

Pub/Sub is the buffer between the API and BigQuery.

That means:

- the API can continue accepting traffic even if BigQuery is slower
- rows are stored in a durable queue instead of a per-request in-memory list
- ingestion and persistence are decoupled

In local tests, the project simulates Pub/Sub with an in-memory queue so the behavior stays testable without GCP.

### 3. Batch worker

The worker is implemented by `BigQueryBatchWorker` in [app/services.py](./app/services.py).

Its responsibilities are:

- pull messages from the ingestion topic
- place them into an internal buffer
- flush that buffer when one of two conditions is met

The two flush conditions are:

1. `batch_size` is reached
2. `flush_interval_seconds` has elapsed

This is the core batching rule:

```text
flush when:
  buffered_rows >= batch_size
or
  oldest_buffered_row_age >= flush_interval
```

This pattern is often called micro-batching.

### 4. BigQuery persistence

When the worker flushes:

1. it extracts rows from the buffer
2. it removes unchanged values
3. it writes only changed rows to BigQuery
4. it updates the latest-value cache only after the write succeeds
5. it acknowledges the Pub/Sub messages

This order matters for correctness.

## Why Deduplication Happens in the Worker

The requirement says that only changed values should be stored.

At first glance it may seem simpler to do that inside `POST /message`, but that creates a consistency risk.

If the API:

1. decides a value is new
2. updates the "latest known value" cache
3. then fails to write to BigQuery

the system becomes inconsistent:

- the cache says the value was stored
- BigQuery does not actually contain that row

The next identical message may then be discarded as a duplicate even though persistence never happened.

To avoid that, the current implementation does this:

1. API publishes rows
2. worker pulls rows
3. worker deduplicates rows
4. worker writes changed rows
5. worker updates cache only after success

That is why the deduplication logic was moved out of the API path.

## What Buffering Means Here

Buffering and batching are closely related but not identical.

- Buffering means rows are temporarily held before writing
- Batching means multiple buffered rows are written together in one persistence operation

In this project there are two buffering layers:

1. Pub/Sub itself buffers accepted messages durably
2. the worker keeps a temporary in-memory batch buffer before flushing

That gives the system both durability and efficiency.

## Why This Solution Is Better Than Direct BigQuery Writes

### 1. Faster API responses

The API no longer waits for BigQuery on every request.

That reduces latency for `POST /message` and keeps the HTTP layer focused on validation and enqueueing.

### 2. Better scalability

If you imagine millions of devices sending data, direct request-to-BigQuery writes create unnecessary pressure on the database and on the API workers.

With a queue in the middle:

- the API can scale independently
- the worker can scale independently
- the persistence rate can be tuned with batch settings

### 3. Better reliability

Pub/Sub acts as a shock absorber.

If traffic spikes or BigQuery slows down:

- messages can stay in the topic
- workers can catch up later
- the API is less likely to fail because of downstream pressure

### 4. Lower write amplification

Instead of one BigQuery write per HTTP request, the system writes many rows together.

That is generally more efficient and better aligned with the statement that hourly freshness is enough.

### 5. Safer deduplication

Because cache updates happen only after successful batch persistence, the system avoids false duplicates caused by partial failures.

### 6. Cleaner separation of concerns

The API is responsible for:

- receiving requests
- validating payloads
- publishing accepted rows

The worker is responsible for:

- batching
- deduplication
- persistence

This separation makes the code easier to reason about and easier to evolve.

## Current Implementation Details

### `POST /message`

Implemented in [app/api.py](./app/api.py).

Current behavior:

- parse `DevMessage`
- keep only `temp_interior` and `instant_power`
- flatten the nested payload into row dictionaries
- publish rows to `device_property_updates`
- return `rows_enqueued`

The API no longer returns the number of rows stored in BigQuery because persistence is asynchronous.

### `BigQueryBatchWorker`

Implemented in [app/services.py](./app/services.py).

Main methods:

- `run_once()`
  - pull messages
  - buffer them
  - flush if batch size or time threshold is reached

- `drain()`
  - used mainly in tests
  - consume everything and flush remaining buffered rows

- `flush()`
  - force a final flush
  - useful on shutdown

### `LatestValueDeduplicator`

Implemented in [app/services.py](./app/services.py).

It compares incoming rows against a latest-value cache keyed by:

```text
(devId, code)
```

If the same device sends the same code with the same value twice, the second row is skipped.

If the value changes, the new row is kept.

### Cache implementations

Two cache implementations exist:

- `InMemoryLatestValueCache`
  - used for local execution and tests
- `RedisLatestValueCache`
  - intended for production or multi-worker deployments

Redis is useful in production because different worker instances need to share the same latest-value state.

### BigQuery writes

`BigQuerySercice.load_rows()` appends batched rows to BigQuery using a load job.

In test mode:

- rows are appended to an in-memory list
- `GET /report` aggregates from that list

In production mode:

- rows are appended to the configured BigQuery table
- `GET /report` uses a BigQuery aggregation query

## Local Test Mode Versus Production Mode

### Local mode

Used by the automated tests.

Behavior:

- Pub/Sub is simulated by in-memory queues
- BigQuery is simulated by an in-memory row list
- the worker is triggered explicitly by tests

This keeps the code easy to verify without needing cloud infrastructure.

### Production mode

Intended deployment model:

- FastAPI application receives HTTP traffic
- Pub/Sub stores telemetry rows
- one or more worker processes pull from the ingestion topic
- Redis stores the latest values for deduplication
- BigQuery receives batch writes

In a full deployment, the worker should run as a separate long-lived process.

## Tradeoffs of the Proposed Solution

No architecture is free. This one has clear tradeoffs.

### Advantages

- lower API latency
- better resilience under traffic spikes
- better alignment with hourly freshness requirements
- safer deduplication semantics
- clear separation between ingestion and persistence
- more scalable than direct synchronous writes

### Costs and tradeoffs

- more moving parts than a direct write path
- asynchronous persistence means `/message` success does not guarantee immediate BigQuery visibility
- operating a separate worker process adds deployment complexity
- deduplication state becomes an infrastructure concern in production and should be shared through Redis or a similar system

These tradeoffs are acceptable here because the test explicitly allows non-real-time data freshness.

## File Overview

```text
app/
  api.py        HTTP endpoints and application wiring
  schemas.py    Pydantic request models
  services.py   Pub/Sub service, deduplication, batch worker, BigQuery service

tests/
  test_api.py   local tests for enqueueing, batching, deduplication, reporting
```

## Running the Tests

If your local environment has the dependencies installed, run:

```bash
python3 -m pytest -q
```

In this workspace the packaged dependencies were executed with:

```bash
PYTHONPATH=.venv/lib/python3.11/site-packages python3 -m pytest -q
```

## Suggested Next Step

The next production-oriented improvement would be to add a dedicated worker entrypoint, for example `worker_main.py`, so the batch worker can run continuously as a separate process instead of only being driven from tests.
