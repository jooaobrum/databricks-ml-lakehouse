-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS uc_dev.olist_bronze;
CREATE SCHEMA IF NOT EXISTS uc_qa.olist_bronze;
CREATE SCHEMA IF NOT EXISTS uc_prod.olist_bronze;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS uc_dev.olist_silver;
CREATE SCHEMA IF NOT EXISTS uc_qa.olist_silver;
CREATE SCHEMA IF NOT EXISTS uc_prod.olist_silver;


-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS uc_dev.olist_gold;
CREATE SCHEMA IF NOT EXISTS uc_qa.olist_gold;
CREATE SCHEMA IF NOT EXISTS uc_prod.olist_gold;


DROP
