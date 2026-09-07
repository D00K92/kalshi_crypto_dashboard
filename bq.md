Developer Specification: GCS to BigQuery Tick Resampling Pipeline
Pipeline Overview
This pipeline ingests high-frequency, raw financial tick/order book data from Google Cloud Storage (GCS), loads it into a partitioned/clustered Google BigQuery table, and executes an optimized SQL query to resample the ticks into 1-minute or 5-minute Open-High-Low-Close-Volume (OHLCV) bars.

Part 1: Prerequisites & Resource Creation
Step 1: Identify Your Data Schema
Ensure the coding agent maps your raw tick schema to these standard target BigQuery types:

timestamp: TIMESTAMP (The exact time of the transaction/quote)
symbol: STRING (e.g., BTC-USD, AAPL)
price: NUMERIC or FLOAT64 (The transaction or mid price)
volume: NUMERIC or FLOAT64 (The quantity of the transaction)
Step 2: Create a BigQuery Dataset
The coding agent should run the following command to create a BigQuery dataset in your desired region:

bq mk \
  --location=<YOUR_GCP_REGION> \
  --dataset <YOUR_PROJECT_ID>:<YOUR_DATASET_NAME>
bq mk \
  --location=<YOUR_GCP_REGION> \
  --dataset <YOUR_PROJECT_ID>:<YOUR_DATASET_NAME>
Generated code may be subject to licence restrictions not shown here. Use code with care.Learn more 

(Replace <YOUR_GCP_REGION> with regions like us-central1 or europe-west1 depending on where your GCS bucket is located.)

Part 2: Loading GCS Data into BigQuery (Optimized)
To keep costs low and queries incredibly fast, the ingestion table must be partitioned by day and clustered by ticker symbol.

The coding agent should run the following command to auto-detect the schema from your Parquet files in GCS and load them directly into BigQuery:

bq load \
  --source_format=PARQUET \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=DAY \
  --clustering_fields=symbol \
  <YOUR_PROJECT_ID>:<YOUR_DATASET_NAME>.raw_ticks \
  "gs://<YOUR_GCS_BUCKET_NAME>/<PATH_TO_FILES>/*.parquet"
bq load \
  --source_format=PARQUET \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=DAY \
  --clustering_fields=symbol \
  <YOUR_PROJECT_ID>:<YOUR_DATASET_NAME>.raw_ticks \
  "gs://<YOUR_GCS_BUCKET_NAME>/<PATH_TO_FILES>/*.parquet"
Generated code may be subject to licence restrictions not shown here. Use code with care.Learn more 

(If your files are CSV or JSON, replace --source_format=PARQUET with CSV or NEWLINE_DELIMITED_JSON and use --autodetect.)

Part 3: The Resampling SQL Query (OHLCV)
This query uses integer division of Unix timestamps to group ticks into clean 1-minute or 5-minute boundaries, and uses ROW_NUMBER() windowing to find the exact first (Open) and last (Close) ticks of each window without performing expensive sorting.

Template: 5-Minute OHLCV Generator
WITH bucketed_ticks AS (
  SELECT
    symbol,
    -- Truncate timestamp to 5-minute (300 seconds) boundaries
    TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) AS time_bucket,
    price,
    volume,
    -- Assign a sequence order to find the Open price
    ROW_NUMBER() OVER (
      PARTITION BY symbol, TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) 
      ORDER BY timestamp ASC
    ) AS row_asc,
    -- Assign a sequence order to find the Close price
    ROW_NUMBER() OVER (
      PARTITION BY symbol, TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) 
      ORDER BY timestamp DESC
    ) AS row_desc
  FROM
    `<YOUR_PROJECT_ID>.<YOUR_DATASET_NAME>.raw_ticks`
  WHERE
    -- CRITICAL: Always filter on the partition field to control costs
    timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT
  symbol,
  time_bucket,
  -- Extract values using the sequence filters
  MAX(IF(row_asc = 1, price, NULL)) AS open,
  MAX(price) AS high,
  MIN(price) AS low,
  MAX(IF(row_desc = 1, price, NULL)) AS close,
  SUM(volume) AS volume
FROM
  bucketed_ticks
GROUP BY
  symbol,
  time_bucket
ORDER BY
  time_bucket DESC,
  symbol ASC;
WITH bucketed_ticks AS (
  SELECT
    symbol,
    -- Truncate timestamp to 5-minute (300 seconds) boundaries
    TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) AS time_bucket,
    price,
    volume,
    -- Assign a sequence order to find the Open price
    ROW_NUMBER() OVER (
      PARTITION BY symbol, TIMESTAMP_SECONDS(DIV(UNIX_SECONDS(timestamp), 300) * 300) 

Generated code may be subject to licence restrictions not shown here. Use code with care.Learn more 

Note for 1-Minute Resampling:
Instruct your coding agent to change 300 in the SQL above (the number of seconds in 5 minutes) to 60 in all places.

Part 4: Automation Options
To make this pipeline continuous, the coding agent can implement one of the following automated strategies:

Option A: BigQuery Scheduled Queries (Easiest & Free)
Run the resampling query hourly/daily and append the resampled data to a final table. The coding agent can set up a scheduled query using this command:

bq mk \
  --transfer_config \
  --project_id=<YOUR_PROJECT_ID> \
  --data_source=scheduled_query \
  --display_name="Hourly_Tick_Resample" \
  --target_dataset=<YOUR_DATASET_NAME> \
  --params='{
    "query": "INSERT INTO `<YOUR_PROJECT_ID>.<YOUR_DATASET_NAME>.resampled_prices` (symbol, time_bucket, open, high, low, close, volume) ... (Paste the SQL Query from Part 3 here)",
    "schedule": "every 1 hours"
  }'
bq mk \
  --transfer_config \
  --project_id=<YOUR_PROJECT_ID> \
  --data_source=scheduled_query \
  --display_name="Hourly_Tick_Resample" \
  --target_dataset=<YOUR_DATASET_NAME> \
  --params='{
    "query": "INSERT INTO `<YOUR_PROJECT_ID>.<YOUR_DATASET_NAME>.resampled_prices` (symbol, time_bucket, open, high, low, close, volume) ... (Paste the SQL Query from Part 3 here)",
    "schedule": "every 1 hours"
  }'
Generated code may be subject to licence restrictions not shown here. Use code with care.Learn more 

Option B: Cloud Workflows + BigQuery (Event-Driven)
If you want to resample the second a new file lands in GCS:

Set up a GCS bucket notification to trigger when a file is uploaded.
Route the notification to a Cloud Function or Cloud Workflow.
Have the Function trigger a BigQuery loading job, followed immediately by the resampling query.