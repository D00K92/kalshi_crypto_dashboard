# GCS Tick Storage Pipeline Plan

## Context

The Binance WebSocket ingestion pipeline is successfully publishing trade data to the Redis Stream `stream:ticks`. The next service will consume those events and archive them as compressed Apache Parquet objects in Google Cloud Storage for backtesting and offline analytics.

The completed [`GCS_Pipeline_SPEC.md`](../GCS_Pipeline_SPEC.md) defines the required format, batching thresholds, baseline object layout, Redis consumer group behavior, container target, GKE deployment, and GCS bucket. This plan turns that reference implementation into a production-grade service while preserving its required behavior.

## Specification Reconciliation

The completed specification resolves these decisions:

- Source: `stream:ticks` only
- Consumer group starts at Redis ID `0`, so retained history is archived
- Consumer group name: `gcs_archiver_group`
- GCS bucket: `kalshi-crypto-tick-data`
- Batch triggers: 10,000 ticks or 60 seconds
- Parquet compression: Snappy
- Numeric storage: price as `float64`, volume as `float32`
- Runtime: Python 3.11+
- Deployment: one GKE replica in `asia-northeast3`
- Authentication: GKE Workload Identity, with no service-account JSON key
- Target image platform: `linux/amd64`
- Parquet field name: source-compatible `quantity`
- Object layout: partition by venue, instrument, UTC date, and UTC hour; include the Redis ID range
- Storage window: organize Parquet objects into one-hour UTC partitions without deleting them after one hour
- Redis outage tolerance target: 24 hours
- Malformed records: persist to a GCS dead-letter prefix before acknowledging them

The Python, Dockerfile, and Kubernetes snippets in the specification are treated as behavioral reference examples, not production-ready code to copy literally. The final implementation must additionally address the actual Redis payload envelope, asynchronous blocking, pending-entry recovery, idempotent GCS creation, object-name collisions, least-privilege IAM, immutable image tags, health signaling, and testability.

## Recommended Architecture

```text
Binance WebSocket
    ↓
Ingestion service
    ↓ XADD
Redis stream:ticks
    ↓ XREADGROUP
GCS exporter service
    ↓ PyArrow + Snappy, in memory
GCS Parquet objects
    ↓
Backtest / BigQuery / ClickHouse
```

The exporter should be an independent `services/gcs_exporter` deployment rather than part of the ingestion process. A slow GCS upload or PyArrow conversion must not interrupt the WebSocket connections.

## Current Redis Data Contract

Each Redis Stream entry contains these fields:

```text
event_id
event_type
venue
instrument
payload
```

The JSON document inside `payload` contains:

```text
event_id
event_type
venue
instrument
trade_id
price                 decimal string
quantity              decimal string
taker_side
exchange_ts_ms
received_ts_ms
schema_version
```

The specification's simplified `timestamp`, `symbol`, and `volume` fields therefore require an explicit mapping.

### Proposed Parquet Schema

```text
redis_id          string
event_id          string
venue             string
instrument        string
trade_id          string
price             float64
quantity          float32 — source-compatible field name
taker_side        string
exchange_ts_ms    int64
received_ts_ms    int64
schema_version    int16
```

Keeping both `event_id` and `redis_id` allows duplicate detection and removal during backtests.

## Reliability Model

Use at-least-once Redis delivery with idempotent GCS uploads:

1. Create the Redis consumer group `gcs_archiver_group` at Redis ID `0`.
2. Read with `XREADGROUP`; never use `NOACK`.
3. Keep the Redis messages pending while constructing the batch.
4. Convert the batch to compressed Parquet in memory.
5. Upload the object successfully to GCS.
6. Only then call `XACK` for the included Redis IDs.
7. Recover abandoned pending entries with `XAUTOCLAIM`.
8. Use deterministic object names and `if_generation_match=0` so a retry cannot overwrite an existing object.

Expected failure behavior:

- Crash before upload: Redis redelivers the pending messages.
- Crash after upload but before ACK: the deterministic upload is retried, GCS reports that the object already exists, and the messages can then be acknowledged.
- A message is never acknowledged before durable storage succeeds.

Official references:

- [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)
- [GCS generation preconditions](https://docs.cloud.google.com/python/docs/reference/storage/latest/generation_metageneration)
- [GKE Workload Identity Federation](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)

## Object Layout

The specification currently proposes:

```text
ticks/date=YYYY-MM-DD/btc_ticks_YYYYMMDD_HHMMSS.parquet
```

A name containing only second-level precision can collide during retries or multiple flushes in one second. The recommended layout is:

```text
ticks/
  venue=binance/
    instrument=BTCUSDT/
      date=2026-08-26/
        hour=14/
          btc_ticks_20260826_140501_<first-id>_<last-id>.parquet
```

Partition dates and hours should be calculated from `exchange_ts_ms` in UTC, not upload time. A buffered batch that crosses a UTC date or hour boundary should be split into separate objects.

## Implementation Plan

### Phase 1 — Lock Down the Storage Contract

Implement these design decisions before writing service code:

- Complete the Parquet schema.
- Decide quantity precision.
- Define UTC partition behavior.
- Define deterministic object naming.
- Decide whether to archive existing Redis entries or only new entries.
- Document at-least-once delivery and downstream deduplication behavior.

Verification:

- Create a schema fixture from an actual Binance Redis payload.
- Explicitly test decimal-to-floating-point behavior.
- Prove that retrying the same Redis batch produces the same object name.

Anti-pattern guards:

- Do not invent fields that are absent from the actual producer payload.
- Do not discard `event_id` or Redis entry IDs.
- Do not treat Redis-to-GCS delivery as exactly-once.

### Phase 2 — Create GCS Infrastructure

- Create a globally unique regional bucket in `asia-northeast3`.
- Use Standard storage initially.
- Enable uniform bucket-level access.
- Enable public-access prevention.
- Add a lifecycle policy later after measuring daily storage volume and confirming retention requirements.
- Create a dedicated Kubernetes service account for the exporter.
- Grant only `roles/storage.objectCreator` on the bucket through GKE Workload Identity Federation.
- Use Application Default Credentials through GKE; do not create or store a service-account JSON key.

Specification-defined bucket name:

```text
kalshi-crypto-tick-data
```

The name must first be checked for global availability and ownership. If it is unavailable, the replacement name must be recorded in the specification, deployment manifest, and runbook.

Verification:

- Confirm bucket region, storage class, public-access prevention, and uniform access.
- Confirm the exporter Kubernetes service account can create an object.
- Confirm it cannot delete or overwrite objects.

### Phase 3 — Scaffold `services/gcs_exporter`

Create:

```text
services/gcs_exporter/
├── Dockerfile
├── pyproject.toml
├── uv.lock
├── README.md
├── src/gcs_exporter/
│   ├── __init__.py
│   ├── __main__.py
│   ├── config.py
│   ├── models.py
│   ├── redis_consumer.py
│   ├── parquet_writer.py
│   ├── gcs_uploader.py
│   └── service.py
└── tests/
```

Dependencies:

- `redis`
- `orjson`
- `pyarrow`
- `google-cloud-storage`
- `google-crc32c`
- `uvloop`
- Test dependencies

Pandas is unnecessary for this path. Direct PyArrow arrays avoid an extra DataFrame copy and make schema enforcement explicit.

### Phase 4 — Implement Redis Consumption

- Create `gcs_archiver_group` at ID `0` with `XGROUP CREATE ... MKSTREAM`.
- Consume `stream:ticks` using `XREADGROUP`.
- Decode the `payload` field using `orjson`.
- Validate required fields and `schema_version`.
- Buffer up to 10,000 rows.
- Flush when either:
  - 10,000 rows are buffered, or
  - the oldest buffered row has waited 60 seconds.
- Apply bounded memory and backpressure.
- Periodically reclaim abandoned messages with `XAUTOCLAIM`.
- Handle malformed events with structured logging and a defined dead-letter policy.

The producer currently applies approximate stream trimming with a default maximum near 1,000,000 entries. This retention must cover the longest exporter outage we intend to survive. Redis trimming can remove a pending entry's payload before the exporter recovers it.

Verification:

- Prove the count trigger flushes exactly at its configured threshold.
- Prove the time trigger flushes a non-empty partial batch.
- Prove empty buffers do not create objects.
- Prove pending entries can be reclaimed after a simulated worker crash.

Anti-pattern guards:

- Do not use `NOACK`.
- Do not ACK before durable upload.
- Do not assume the Redis Stream length cap protects old pending payloads.

### Phase 5 — Implement Parquet Serialization and GCS Upload

- Build a fixed `pyarrow.Schema`.
- Create Arrow data using explicit arrays or tables.
- Use `pa.BufferOutputStream()`.
- Use `pyarrow.parquet.write_table(..., compression="snappy")`.
- Upload the completed buffer with:
  - `content_type="application/vnd.apache.parquet"`
  - `checksum="crc32c"`
  - `if_generation_match=0`
  - Bounded timeout and retry behavior
- Run synchronous GCS upload work through `asyncio.to_thread()` so it cannot block Redis reads and timers.
- ACK Redis IDs only after a successful upload or a safely recognized idempotent retry.

Verification:

- Read the generated in-memory Parquet data back with PyArrow.
- Assert exact field names, data types, values, Snappy compression, and row count.
- Simulate an upload failure and prove that Redis entries remain pending.
- Retry an already uploaded deterministic batch and prove it does not overwrite data or create a duplicate object.

Anti-pattern guards:

- Do not write temporary Parquet files to local disk.
- Do not upload without a GCS generation precondition.
- Do not create one GCS object per tick.

### Phase 6 — Graceful Shutdown and Observability

When Kubernetes sends `SIGTERM`:

1. Stop requesting new Redis messages.
2. Flush the non-empty buffer.
3. Upload it.
4. ACK its Redis entries.
5. Close Redis cleanly.
6. Exit within the Kubernetes termination grace period.

Structured logs and metrics should expose:

- Buffered row count
- Oldest buffered row age
- Redis consumer lag
- Pending entries
- Reclaimed entries
- Upload duration
- Parquet byte size
- Compression ratio
- Uploaded object name
- Parse and upload failures

Verification:

- Send `SIGTERM` with a partial batch buffered.
- Confirm that an object is uploaded and its entries are acknowledged before exit.
- Confirm shutdown timeout behavior leaves uncommitted messages pending rather than acknowledging them.

### Phase 7 — Deploy to GKE

Add:

```text
k8s/gcs-exporter-deployment.yaml
```

The manifest should contain:

- A separate Deployment
- One initial replica
- The dedicated exporter Kubernetes service account
- Redis host and port
- GCS bucket name
- Consumer group and consumer name
- Batch count and flush interval
- Resource requests and limits
- Startup, readiness, and liveness checks
- A sufficient termination grace period

Build the container as `linux/amd64` and push it to the existing Artifact Registry repository.

Verification:

- Confirm the pod uses the expected AMD64 image.
- Confirm Application Default Credentials resolve through Workload Identity Federation.
- Confirm the exporter connects to Redis and creates its consumer group.
- Confirm it creates objects only in the configured bucket and prefix.

### Phase 8 — End-to-End Verification

- Unit-test Redis payload parsing.
- Unit-test exact Parquet types.
- Unit-test both flush triggers.
- Unit-test that ACK never precedes upload.
- Unit-test retry and idempotency behavior.
- Unit-test UTC partition splitting.
- Deploy the exporter.
- Confirm its consumer group appears in Redis.
- Confirm pending messages return to zero after a successful flush.
- Download one Parquet object.
- Read it with PyArrow and compare its rows with the source Redis events.
- Run a backtest-style query filtered by date, venue, and instrument.
- Restart the pod while it owns a batch and prove that no source events disappear.

## Confirmed Implementation Decisions

1. **Field mapping:** Preserve the source-compatible field name `quantity` and store it as the specification-required `float32`.

2. **Object naming:** Partition objects by venue, instrument, UTC date, and UTC hour. Include the first and last Redis IDs in every object name to prevent same-second collisions and make retries deterministic.

3. **Outage tolerance:** Redis must retain at least 24 hours of peak tick traffic without losing data that has not reached GCS. Stream retention will be sized from measured peak volume rather than assumed from the current approximate entry cap.

4. **Malformed records:** Store the raw Redis entry, its Redis ID, and validation error in a GCS dead-letter prefix. ACK the malformed Redis entry only after the dead-letter object is durably created.

5. **Storage window and retention:** Organize Parquet objects into one-hour UTC partitions. The one-hour boundary describes the partition window, not object deletion. Retain the files for historical backtesting; no automatic GCS deletion lifecycle is included in the initial release.

## Explicit Initial Scope

The confirmed first release is limited to:

- Trade events from `stream:ticks`
- Binance `BTCUSDT` data initially, while preserving venue and instrument fields for upcoming Coinbase and Deribit feeds
- One GCS exporter replica
- Snappy-compressed Parquet generated entirely in memory
- At-least-once processing with idempotent, create-only GCS uploads
- UTC event-time partitioning
