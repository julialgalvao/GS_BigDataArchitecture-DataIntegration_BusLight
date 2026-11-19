# Databricks notebook source
# BusLight - verificações finais
# 1) rodar o MERGE da Prata uma segunda vez e mostrar que nada novo entra
# 2) conferir a integridade da Ouro (chaves únicas e soma do fato igual à da Prata)

SCHEMA = "workspace.buslight"

# COMMAND ----------

# MAGIC %md ## 1. Segunda execução do MERGE da Prata
# MAGIC Contagem antes, MERGE com o mesmo lote e contagem depois. Na segunda vez `num_inserted_rows` deve ser 0.

# COMMAND ----------

# clima por hora (chave: data_hora)
antes = spark.table(f"{SCHEMA}.prata_clima_hora").count()

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW lote_clima AS
SELECT to_timestamp(h.time) AS data_hora,
       CAST(h.temperature_2m AS DOUBLE) AS temperatura_c,
       COALESCE(CAST(h.precipitation AS DOUBLE), 0.0) AS precipitacao_mm,
       COALESCE(CAST(h.rain AS DOUBLE), 0.0) AS chuva_mm
FROM (SELECT explode(arrays_zip(dados.hourly.time, dados.hourly.temperature_2m,
                                dados.hourly.precipitation, dados.hourly.rain)) AS h
      FROM {SCHEMA}.bronze_clima)
WHERE h.time IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY h.time ORDER BY h.time) = 1
""")

resultado = spark.sql(f"""
MERGE INTO {SCHEMA}.prata_clima_hora AS alvo
USING lote_clima AS novo
ON alvo.data_hora = novo.data_hora
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
display(resultado)
depois = spark.table(f"{SCHEMA}.prata_clima_hora").count()
print(f"prata_clima_hora: {antes} linhas antes, {depois} depois")

# COMMAND ----------

# usuários (chave: id_usuario) - o mesmo teste com a tabela inteira como lote
antes = spark.table(f"{SCHEMA}.prata_usuario").count()

resultado = spark.sql(f"""
MERGE INTO {SCHEMA}.prata_usuario AS alvo
USING (SELECT * FROM {SCHEMA}.prata_usuario) AS novo
ON alvo.id_usuario = novo.id_usuario
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
display(resultado)
depois = spark.table(f"{SCHEMA}.prata_usuario").count()
print(f"prata_usuario: {antes} linhas antes, {depois} depois")

# COMMAND ----------

# MAGIC %md ## 2. Integridade da Ouro

# COMMAND ----------

# MAGIC %sql
# MAGIC -- chave de cada dimensão é única (linhas = chaves distintas)
# MAGIC SELECT 'dim_ponto' AS dimensao, COUNT(*) AS linhas, COUNT(DISTINCT sk_ponto) AS chaves_distintas FROM workspace.buslight.dim_ponto
# MAGIC UNION ALL SELECT 'dim_linha', COUNT(*), COUNT(DISTINCT sk_linha) FROM workspace.buslight.dim_linha
# MAGIC UNION ALL SELECT 'dim_tempo', COUNT(*), COUNT(DISTINCT sk_tempo) FROM workspace.buslight.dim_tempo
# MAGIC UNION ALL SELECT 'dim_clima', COUNT(*), COUNT(DISTINCT sk_clima) FROM workspace.buslight.dim_clima
# MAGIC UNION ALL SELECT 'dim_usuario', COUNT(*), COUNT(DISTINCT sk_usuario) FROM workspace.buslight.dim_usuario

# COMMAND ----------

# MAGIC %sql
# MAGIC -- linhas do fato sem dimensão correspondente (deve ser 0 em todas)
# MAGIC SELECT
# MAGIC   SUM(CASE WHEN p.sk_ponto IS NULL THEN 1 ELSE 0 END) AS sem_ponto,
# MAGIC   SUM(CASE WHEN l.sk_linha IS NULL THEN 1 ELSE 0 END) AS sem_linha,
# MAGIC   SUM(CASE WHEN t.sk_tempo IS NULL THEN 1 ELSE 0 END) AS sem_tempo
# MAGIC FROM workspace.buslight.fato_espera_ponto f
# MAGIC LEFT JOIN workspace.buslight.dim_ponto p ON f.sk_ponto = p.sk_ponto
# MAGIC LEFT JOIN workspace.buslight.dim_linha l ON f.sk_linha = l.sk_linha
# MAGIC LEFT JOIN workspace.buslight.dim_tempo t ON f.sk_tempo = t.sk_tempo

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a soma das métricas do fato é igual à da Prata
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM workspace.buslight.prata_previsao_parada) AS previsoes_na_prata,
# MAGIC   (SELECT SUM(qtd_veiculos_previstos) FROM workspace.buslight.fato_espera_ponto) AS veiculos_no_fato,
# MAGIC   (SELECT COUNT(*) FROM workspace.buslight.fato_espera_ponto) AS linhas_do_fato
