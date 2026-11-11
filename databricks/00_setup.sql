-- Databricks notebook source
-- BusLight - setup do lakehouse
-- Cria o schema, o volume de pouso e as pastas de cada fonte.

CREATE SCHEMA IF NOT EXISTS workspace.buslight
COMMENT 'BusLight - Global Solution 2026.2 (FIAP)';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.buslight.chegada
COMMENT 'Pasta de pouso: arquivos gerados pelo Airflow e enviados em lotes';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC base = "/Volumes/workspace/buslight/chegada"
-- MAGIC for pasta in ["sptrans_posicao", "sptrans_previsao", "clima", "ibge", "usuarios", "_controle"]:
-- MAGIC     dbutils.fs.mkdirs(f"{base}/{pasta}")
-- MAGIC display(dbutils.fs.ls(base))
