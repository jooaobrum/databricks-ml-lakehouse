-- DEV
CREATE CATALOG IF NOT EXISTS uc_bronze_dev;
CREATE CATALOG IF NOT EXISTS uc_silver_dev;
CREATE CATALOG IF NOT EXISTS uc_gold_datascience_dev;
CREATE SCHEMA IF NOT EXISTS uc_bronze_dev.olist;
CREATE SCHEMA IF NOT EXISTS uc_silver_dev.olist;
CREATE SCHEMA IF NOT EXISTS uc_gold_datascience_dev.olist;

-- QA
CREATE CATALOG IF NOT EXISTS uc_bronze_qa;
CREATE CATALOG IF NOT EXISTS uc_silver_qa;
CREATE CATALOG IF NOT EXISTS uc_gold_datascience_qa;
CREATE SCHEMA IF NOT EXISTS uc_bronze_qa.olist;
CREATE SCHEMA IF NOT EXISTS uc_silver_qa.olist;
CREATE SCHEMA IF NOT EXISTS uc_gold_datascience_qa.olist;

-- PROD
CREATE CATALOG IF NOT EXISTS uc_bronze_prod;
CREATE CATALOG IF NOT EXISTS uc_silver_prod;
CREATE CATALOG IF NOT EXISTS uc_gold_datascience_prod;
CREATE SCHEMA IF NOT EXISTS uc_bronze_prod.olist;
CREATE SCHEMA IF NOT EXISTS uc_silver_prod.olist;
CREATE SCHEMA IF NOT EXISTS uc_gold_datascience_prod.olist;
