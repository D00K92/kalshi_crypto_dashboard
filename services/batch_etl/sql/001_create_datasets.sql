-- BigQuery datasets for the ETL-to-Feast contract.
-- Run with the deployment project selected; these statements are idempotent.
CREATE SCHEMA IF NOT EXISTS `kalshi-crypto-506614.market_data`
  OPTIONS(location = 'asia-northeast3');
CREATE SCHEMA IF NOT EXISTS `kalshi-crypto-506614.feature_store`
  OPTIONS(location = 'asia-northeast3');
CREATE SCHEMA IF NOT EXISTS `kalshi-crypto-506614.training_labels`
  OPTIONS(location = 'asia-northeast3');
