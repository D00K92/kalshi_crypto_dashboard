# TASK SPECIFICATION: GCS Parquet Exporter for Redis Stream Ticks

## 1. Overview
We are building a production-grade **GCS Consumer Service** in Python that consumes real-time crypto market tick data from GCP Memorystore for Redis (`stream:ticks`) and exports it to Google Cloud Storage (GCS) as compressed **Apache Parquet** files.

This service acts as a durable, highly efficient cold-storage pipeline to minimize storage footprint and cloud expenses while preserving sub-second analytical access.

---

## 2. Technical Stack
- **Language:** Python 3.11+
- **Asynchronous Execution / I/O:** `asyncio` or structured loop
- **Data Layers:**
  - `redis-py` (Redis 7 Stream reader)
  - `pandas` / `pyarrow` (Parquet serialization with Snappy compression)
  - `google-cloud-storage` (GCS Blob management)
- **Containerization:** Docker (`linux/amd64` multi-stage build)
- **Orchestration:** GKE Autopilot (`asia-northeast3`)

---

## 3. Key Requirements & Architectural Constraints

### A. Performance & Compression
1. **Format:** Must write directly to **Apache Parquet** using **Snappy** compression.
2. **In-Memory Pipeline:** Convert Redis Stream payloads to PyArrow tables and stream directly to GCS via in-memory binary streams (`pa.BufferOutputStream()`). **Do not write temporary files to local disk.**
3. **Data Type Optimization:** Explicitly cast fields to save memory and storage:
   - `timestamp`: `int64` (Unix epoch milliseconds or nanoseconds)
   - `symbol`: `string` / `category`
   - `price`: `float64`
   - `volume`: `float32`

### B. Micro-Batching & Flush Strategy
The worker must buffer incoming ticks in memory and trigger a GCS flush when **EITHER** of the following conditions is met:
- **Condition 1 (Count Trigger):** Buffer accumulates `10,000` ticks.
- **Condition 2 (Time Trigger):** `60 seconds` have elapsed since the last flush (if buffer is non-empty).

### C. Partitioning & Object Path Naming
Files must be uploaded to GCS following Hive-style partition naming for optimal BigQuery/ClickHouse scanning:

```text
gs://<BUCKET_NAME>/ticks/date=YYYY-MM-DD/btc_ticks_YYYYMMDD_HHMMSS.parquet
```

### D. Reliability & Consumer Group Mechanics
1. Use **Redis Consumer Groups** (`XREADGROUP`) with `XACK` to ensure no data loss in case of pod restarts.
2. Gracefully handle `SIGTERM` / `SIGINT` signals by flushing all buffered items before shutting down the process.

---

## 4. Proposed Implementation (`gcs_exporter.py`)

```python
import os
import signal
import sys
import time
import logging
import asyncio
from typing import List, Dict, Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Environment Variables
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "kalshi-crypto-tick-data")
STREAM_NAME = os.getenv("STREAM_NAME", "stream:ticks")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "gcs_archiver_group")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", f"pod-{os.getenv('HOSTNAME', 'local')}")

FLUSH_SIZE = int(os.getenv("FLUSH_SIZE", "10000"))
FLUSH_INTERVAL_SEC = float(os.getenv("FLUSH_INTERVAL_SEC", "60.0"))

class GCSExporter:
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.gcs_client = storage.Client()
        self.bucket = self.gcs_client.bucket(GCS_BUCKET_NAME)
        self.buffer: List[Dict[str, Any]] = []
        self.pending_msg_ids: List[bytes] = []
        self.last_flush_time = time.time()
        self.running = True

        self._setup_consumer_group()

    def _setup_consumer_group(self):
        try:
            self.redis_client.xgroup_create(STREAM_NAME, CONSUMER_GROUP, id="0", mkstream=True)
            logging.info(f"Created consumer group '{CONSUMER_GROUP}' on stream '{STREAM_NAME}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logging.info(f"Consumer group '{CONSUMER_GROUP}' already exists.")
            else:
                raise e

    def flush_to_gcs(self):
        if not self.buffer:
            return

        count = len(self.buffer)
        start_time = time.time()

        # Convert to Pandas DataFrame
        df = pd.DataFrame(self.buffer)

        # Type Optimization
        if "timestamp" in df.columns:
            df["timestamp"] = df["timestamp"].astype("int64")
        if "price" in df.columns:
            df["price"] = df["price"].astype("float64")
        if "volume" in df.columns:
            df["volume"] = df["volume"].astype("float32")

        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)

        # Write to In-Memory Buffer
        out_stream = pa.BufferOutputStream()
        pq.write_table(table, out_stream, compression="snappy")
        parquet_bytes = out_stream.getvalue().to_pybytes()

        # GCS Object Path Formulation
        now_struct = time.gmtime()
        date_partition = time.strftime("%Y-%m-%d", now_struct)
        filename_timestamp = time.strftime("%Y%m%d_%H%M%S", now_struct)
        gcs_path = f"ticks/date={date_partition}/btc_ticks_{filename_timestamp}.parquet"

        # Upload
        blob = self.bucket.blob(gcs_path)
        blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

        # Acknowledge Redis Messages
        if self.pending_msg_ids:
            self.redis_client.xack(STREAM_NAME, CONSUMER_GROUP, *self.pending_msg_ids)

        elapsed = time.time() - start_time
        logging.info(
            f"Flushed {count} ticks to gs://{GCS_BUCKET_NAME}/{gcs_path} "
            f"[{len(parquet_bytes)/1024:.2f} KB] in {elapsed:.3f}s"
        )

        # Reset Buffer
        self.buffer.clear()
        self.pending_msg_ids.clear()
        self.last_flush_time = time.time()

    def run(self):
        logging.info("GCS Exporter worker started.")
        while self.running:
            try:
                # Read from Stream
                entries = self.redis_client.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={STREAM_NAME: ">"},
                    count=500,
                    block=1000,
                )

                if entries:
                    for stream, messages in entries:
                        for msg_id, payload in messages:
                            tick = {k.decode("utf-8"): v.decode("utf-8") for k, v in payload.items()}
                            self.buffer.append(tick)
                            self.pending_msg_ids.append(msg_id)

                # Check Flush Triggers
                time_since_flush = time.time() - self.last_flush_time
                if len(self.buffer) >= FLUSH_SIZE or (time_since_flush >= FLUSH_INTERVAL_SEC and self.buffer):
                    self.flush_to_gcs()

            except Exception as e:
                logging.error(f"Error in consumer loop: {e}", exc_info=True)
                time.sleep(1)

        # Graceful Shutdown Flush
        logging.info("Shutting down... Executing final flush.")
        self.flush_to_gcs()

    def stop(self, signum, frame):
        logging.info(f"Signal {signum} received, stopping worker...")
        self.running = False

if __name__ == "__main__":
    exporter = GCSExporter()
    signal.signal(signal.SIGTERM, exporter.stop)
    signal.signal(signal.SIGINT, exporter.stop)
    exporter.run()
```

---

## 5. Dockerfile Specification

Must build for **`linux/amd64`** compatibility on GKE Autopilot:

```dockerfile
FROM --platform=linux/amd64 python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY gcs_exporter.py .

# Run worker
CMD ["python", "-u", "gcs_exporter.py"]
```

`requirements.txt`:
```text
redis>=5.0.0
pandas>=2.0.0
pyarrow>=14.0.0
google-cloud-storage>=2.10.0
```

---

## 6. Kubernetes Deployment Manifest (`gcs-exporter-deployment.yaml`)

> **Note:** Relies on GKE Workload Identity for GCP authentication. No explicit service account JSON key needed.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gcs-exporter
  labels:
    app: gcs-exporter
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gcs-exporter
  template:
    metadata:
      labels:
        app: gcs-exporter
    spec:
      containers:
      - name: gcs-exporter
        image: asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo/gcs-exporter:latest
        imagePullPolicy: Always
        env:
        - name: REDIS_HOST
          value: "<REDIS_PRIVATE_IP>"
        - name: REDIS_PORT
          value: "6379"
        - name: GCS_BUCKET_NAME
          value: "kalshi-crypto-tick-data"
        - name: FLUSH_SIZE
          value: "10000"
        - name: FLUSH_INTERVAL_SEC
          value: "60"
        resources:
          requests:
            cpu: "250m"
            memory: "512Mi"
          limits:
            cpu: "500m"
            memory: "1Gi"
```

---

## 7. Action Items for Agent

1. Create `gcs_exporter.py`, `Dockerfile`, `requirements.txt`, and `deployment.yaml` as specified.
2. Ensure Docker image is built with `--platform linux/amd64` and pushed to Artifact Registry (`asia-northeast3-docker.pkg.dev/kalshi-crypto-506614/quant-repo/gcs-exporter:latest`).
3. Deploy the workload using `kubectl apply -f deployment.yaml`.
4. Verify execution logs via `kubectl logs -f deployment/gcs-exporter` and confirm `.parquet` file generation in GCS bucket `gs://kalshi-crypto-tick-data`.