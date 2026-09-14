from pathlib import Path


SQL = Path(__file__).parents[1] / "sql"
SCRIPTS = Path(__file__).parents[1] / "scripts"


def test_active_contract_tables_are_created_before_feature_registration():
    schema = (SQL / "017_create_v2_v3_contract_tables.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS `${project}.feature_store.realized_volatility_v2_10s`" in schema
    assert "CREATE TABLE IF NOT EXISTS `${project}.feature_store.realized_volatility_v3_10s`" in schema
    assert "CREATE TABLE IF NOT EXISTS `${project}.feature_store.realized_volatility_v4_10s`" in schema
    assert "CREATE TABLE IF NOT EXISTS `${project}.training_labels.future_realized_volatility_v2_10s`" in schema


def test_hourly_queries_only_generate_data_not_schema():
    for name in (
        "014_compute_v2_10s_features.sql",
        "015_compute_v2_10s_targets.sql",
        "016_compute_v3_10s_features.sql",
        "018_compute_v4_10s_volume_features.sql",
    ):
        assert "CREATE TABLE" not in (SQL / name).read_text()


def test_v2_10s_feature_sql_writes_its_versioned_table_and_entity_contract():
    text = (SQL / "014_compute_v2_10s_features.sql").read_text()

    assert "realized_volatility_v2_10s" in text
    assert "'v2_10s' AS feature_version" in text
    assert "'BTCUSD' AS asset" in text
    assert "MERGE `${project}.feature_store.realized_volatility_v1`" not in text


def test_v2_10s_label_sql_writes_its_versioned_table_and_matching_entity():
    text = (SQL / "015_compute_v2_10s_targets.sql").read_text()

    assert "future_realized_volatility_v2_10s" in text
    assert "'v2_10s' AS label_version" in text
    assert "'BTCUSD' AS market_id" in text
    assert "target_rv_1m" in text
    assert "MERGE `${project}.training_labels.future_realized_volatility_v1`" not in text


def test_v3_har_features_use_complete_trailing_windows():
    text = (SQL / "016_compute_v3_10s_features.sql").read_text()

    assert "realized_volatility_v3_10s" in text
    assert "FROM `${project}.feature_store.realized_volatility_v2_10s`" in text
    assert "FROM `${project}.market_data.bars`" not in text
    assert "'v3_10s' AS feature_version" in text
    assert "RANGE BETWEEN 20 PRECEDING AND CURRENT ROW" in text
    assert "observations_30s = 3" in text
    assert "RANGE BETWEEN 50 PRECEDING AND CURRENT ROW" in text
    assert "RANGE BETWEEN 10790 PRECEDING AND CURRENT ROW" in text
    assert "observations_3h = 1080" in text
    assert "seconds_per_year / 10800" in text


def test_v4_5m_features_use_causal_buyer_volume_windows():
    text = (SQL / "018_compute_v4_10s_volume_features.sql").read_text()

    assert "SUM(v_buy) AS buy_volume" in text
    assert "FROM `${project}.feature_store.realized_volatility_v3_10s`" in text
    assert "RANGE BETWEEN 20 PRECEDING AND CURRENT ROW" in text
    assert "RANGE BETWEEN 290 PRECEDING AND CURRENT ROW" in text
    assert "RANGE BETWEEN 590 PRECEDING AND CURRENT ROW" in text
    assert "observations_10m = 60" in text
    assert "LN(1 + buy_volume_10m)" in text


def test_hourly_active_feature_job_includes_v4_after_its_dependencies():
    text = (SCRIPTS / "run_bigquery_features.py").read_text()

    assert '"v4_10s": "018_compute_v4_10s_volume_features.sql"' in text
    assert 'ACTIVE_FEATURE_VERSIONS = ("v2_10s", "v3_10s", "v4_10s")' in text


def test_hourly_bar_sql_deduplicates_identified_trades_before_aggregation():
    text = (SQL / "010_resample_bars_1m.sql").read_text()

    assert "QUALIFY trade_id IS NULL OR ROW_NUMBER() OVER" in text
    assert "PARTITION BY venue, instrument, trade_id" in text
    assert "ORDER BY COALESCE(received_timestamp, event_timestamp), ingested_at," in text
