# Guide to Writing Effective Dask Pipelines

This guide documents the patterns used by `batch_etl` to resample market data
from GCS and run multiple venue/frequency workloads safely.

## 1. Separate the two levels of parallelism

Use Kubernetes for coarse-grained parallelism and Dask for work inside one
worker:

```text
Indexed Kubernetes Job
├── worker 0: Binance
├── worker 1: Bitstamp
├── worker 2: Coinbase
├── worker 3: Crypto.com
├── worker 4: Gemini
└── worker 5: Kraken

Each worker:
└── one persisted raw venue dataset
    ├── 1s resample task
    ├── 5s resample task
    ├── 1m resample task
    ├── 5m resample task
    ├── 10m resample task
    ├── 30m resample task
    └── 1h resample task
```

The venue is a natural isolation boundary: failures, memory usage, and output
partitions are independent. The frequency tasks can share the same persisted
raw inputs.

## 2. Build one graph, then compute once

Avoid calling `.compute()` in a loop when tasks are independent. Construct one
task per frequency and compute them together:

```python
tick_indexed = tick_indexed.persist()
book_indexed = book_indexed.persist()

def resample_frequency(frequency: str):
    # Keep Dask collections in the closure.
    return _resample_events(
        tick_indexed,
        book_indexed,
        venue,
        FREQUENCIES[frequency],
        start=previous_timestamp,
        end=target_end,
    )

tasks = [dask.delayed(resample_frequency)(frequency) for frequency in frequencies]
results = dask.compute(*tasks, scheduler="threads", num_workers=4)
```

Persisting once prevents each frequency from independently rereading raw GCS
inputs. Computing the task collection together allows Dask to schedule the
independent frequency work concurrently.

### Important delayed-task pitfall

Passing a Dask DataFrame directly as a `delayed` argument can cause Dask to
traverse and materialize it as a pandas DataFrame before the task runs. The
symptom is:

```text
AttributeError: 'DataFrame' object has no attribute 'npartitions'
```

Keep persisted Dask collections in a task closure instead of passing them as
delayed arguments. `traverse=False` is not sufficient for this case.

## 3. Make partition boundaries safe for rolling/resampling operations

Dask resampling and rolling operations need overlap between partitions. Raw
export files can create tiny adjacent partitions, especially when events arrive
in bursts. Repartition time-indexed inputs to a safe hourly boundary before
resampling:

```python
if tick_indexed.npartitions > 1:
    tick_indexed = tick_indexed.repartition(freq="1h")
if book_indexed.npartitions > 1:
    book_indexed = book_indexed.repartition(freq="1h")
```

This prevents errors such as:

```text
Partition size is less than overlapping window size
```

The partition size should be larger than the maximum overlap window while
remaining small enough to bound worker memory.

## 4. Preserve state columns and aggregate flow columns correctly

Market data has two different aggregation semantics:

- State columns: latest price/depth snapshot (`last`), then forward-fill.
- Flow columns: volume and trade counts (`sum`), then fill missing bins with 0.

For example:

```python
book_prices = book_indexed[price_columns].resample(freq).last()
book_quantities = book_indexed[quantity_columns].resample(freq).last()
trade_volume = tick_indexed["v_trade"].resample(freq).sum()
```

Summing book quantities across updates produces accumulated quantities rather
than book depth. That makes WAP, microprice, OBI, slope, and HHI incorrect.

For feature engineering, retain enough information from trades and bars:

```text
p_open, p_trade, p_close, p_trade_mean, p_high, p_low
v_trade, v_buy, v_sell, cnt_trade
p_bid_1..p_bid_10, p_ask_1..p_ask_10
q_bid_1..q_bid_10, q_ask_1..q_ask_10
```

## 5. Handle sparse GCS partitions explicitly

A calendar hour is not necessarily present in raw storage. If a Dask read is
given only empty globs, it may infer an empty schema and report a misleading
missing-column error.

Filter source globs before reading:

```python
existing = [path for path in paths if fs.glob(path)]
if not existing:
    raise FileNotFoundError("no source files for requested venue/hour")
```

For backfills, discover the intersection of available tick and book hours per
venue. Do not assume every venue started at midnight or that ticks and books
started simultaneously.

## 6. Design hourly jobs to be restartable

Each target hour should be independently replaceable:

1. Load the target hour plus the previous hour for context.
2. Compute the requested frequencies.
3. Validate expected row counts.
4. Write only the target hour partition.
5. Skip the hour when all expected outputs already exist and have the current
   schema.

The `run_hourly()` implementation follows this pattern. It makes retries safe
after pod preemption or transient GCS failures.

## 7. Choose a scheduler based on the actual graph

Dask supports threaded, multiprocessing, and distributed schedulers:

```python
with dask.config.set(scheduler="processes", num_workers=4):
    result.compute()
```

However, multiprocessing only helps when the graph contains multiple
independent partitions/tasks. If the pipeline collapses to one partition before
`.compute()`, extra processes do not improve the resampling stage.

For the current pipeline, concurrent delayed frequency tasks with the threaded
scheduler are the smallest safe optimization. Measure CPU and memory before
switching to processes or adding `dask.distributed`.

## 8. Size Kubernetes workers deliberately

The indexed Job provides venue-level parallelism. `parallelism` controls how
many venues run at once; `completions` equals the number of venue assignments.

```yaml
completions: 6
parallelism: 6
completionMode: Indexed
```

Keep resource requests aligned with the selected Dask worker count. Six pods
each running four Dask threads may require substantially more CPU and memory
than six sequential pods. If scheduling becomes unstable, reduce Job
parallelism or reduce `DASK_RESAMPLE_WORKERS` before increasing cluster size.

Spot-only selectors can leave jobs permanently pending when no eligible Spot
capacity or quota exists. One-off backfills should use available standard nodes
unless the cost tradeoff is intentional.

## 9. Treat failures by category

Inspect the Job, pod phase, events, and application logs separately:

- `Pending` + `FailedScheduling`: cluster capacity, affinity, taint, or quota.
- `Preempted`: worker was evicted; indexed retry should resume it.
- Python traceback: application/data/schema failure.
- Missing-column error with no matching objects: empty source glob, not
  necessarily corrupted Parquet.

Do not diagnose a Job from `failed` count alone. A preempted pod may be
replaced successfully, while an application traceback needs a code/data fix.

## 10. Test the graph at two levels

Unit tests should cover:

- source decoding and schema;
- state versus flow aggregation;
- empty-source filtering;
- overlap-safe repartitioning;
- expected row counts;
- target-hour-only writes;
- delayed task construction and completion order.

Then run a small integration slice against representative GCS data and verify:

```text
all expected frequency partitions exist
row counts match the frequency
new columns are present
Parquet files are non-zero
no duplicate target-hour outputs exist
```

The goal is not maximum parallelism. The goal is a bounded, restartable graph
whose outputs are correct even when raw data is sparse, venues begin at
different times, or workers are preempted.
