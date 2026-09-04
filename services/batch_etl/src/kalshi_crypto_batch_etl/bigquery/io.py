"""Small, explicit BigQuery write boundary used by ETL jobs."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from uuid import uuid4

from google.cloud import bigquery


def table_reference(project: str, dataset: str, table: str) -> str:
    """Return a fully qualified BigQuery table identifier."""
    if not all((project, dataset, table)):
        raise ValueError("project, dataset, and table are required")
    return f"{project}.{dataset}.{table}"


def write_frame(
    frame,
    *,
    table: str,
    partition_date: date | str,
    partition_field: str = "event_timestamp",
    time_start=None,
    time_end=None,
    filters: Mapping[str, str] | None = None,
    client=None,
) -> int:
    """Atomically replace one partition through a temporary staging table.

    The dataframe must have the same column names/order as the destination
    table. DML runs in a transaction; the staging table is always cleaned up.
    """
    if frame is None or frame.empty:
        return 0
    client = client or bigquery.Client(location="asia-northeast3")
    staging = f"{table}__staging_{uuid4().hex}"
    load_job = client.load_table_from_dataframe(
        frame,
        staging,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
    )
    load_job.result()
    if (time_start is None) != (time_end is None):
        raise ValueError("time_start and time_end must be provided together")
    if time_start is not None:
        conditions = [f"{partition_field} >= @time_start", f"{partition_field} < @time_end"]
        parameters = [
            bigquery.ScalarQueryParameter("time_start", "TIMESTAMP", time_start),
            bigquery.ScalarQueryParameter("time_end", "TIMESTAMP", time_end),
        ]
    else:
        conditions = [f"DATE({partition_field}) = @partition_date"]
        parameters = [bigquery.ScalarQueryParameter("partition_date", "DATE", str(partition_date))]
    for index, (key, value) in enumerate((filters or {}).items()):
        name = f"filter_{index}"
        conditions.append(f"{key} = @{name}")
        parameters.append(bigquery.ScalarQueryParameter(name, "STRING", value))
    predicate = " AND ".join(conditions)
    query = f"""
    BEGIN TRANSACTION;
    DELETE FROM `{table}` WHERE {predicate};
    INSERT INTO `{table}` SELECT * FROM `{staging}` WHERE {predicate};
    COMMIT TRANSACTION;
    """
    try:
        client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=parameters)).result()
        return len(frame)
    finally:
        client.delete_table(staging, not_found_ok=True)


def load_partition(*, table: str, partition_date, client=None):
    """Read one partition for validation or Feast source preparation."""
    client = client or bigquery.Client(location="asia-northeast3")
    query = f"SELECT * FROM `{table}` WHERE DATE(event_timestamp) = @partition_date"
    config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("partition_date", "DATE", str(partition_date)),
    ])
    return client.query(query, job_config=config).to_dataframe()


def table_options(*, partition_field: str, clustering_fields: tuple[str, ...]) -> Mapping[str, object]:
    """Describe required partition and clustering settings for provisioning."""
    return {"partition_field": partition_field, "clustering_fields": clustering_fields}
