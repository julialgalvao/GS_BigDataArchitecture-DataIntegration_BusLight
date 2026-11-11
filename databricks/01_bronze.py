# Databricks notebook source
# BusLight - camada Bronze
# Carrega os arquivos do volume com Auto Loader. Só arquivos novos entram:
# rodar de novo sem arquivo novo não muda a contagem.

from pyspark.sql import functions as F

BASE = "/Volumes/workspace/buslight/chegada"
SCHEMA = "workspace.buslight"

FONTES = {
    "sptrans_posicao": ("json", "bronze_sptrans_posicao"),
    "sptrans_previsao": ("json", "bronze_sptrans_previsao"),
    "clima": ("json", "bronze_clima"),
    "ibge": ("json", "bronze_ibge_municipio"),
    "usuarios": ("csv", "bronze_usuario"),
}

# COMMAND ----------

def contar(tabela):
    if spark.catalog.tableExists(f"{SCHEMA}.{tabela}"):
        return spark.table(f"{SCHEMA}.{tabela}").count()
    return 0


def carregar(fonte, formato, tabela):
    leitor = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", formato)
        .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
        .option("cloudFiles.inferColumnTypes", "true")
    )
    if formato == "json":
        leitor = leitor.option("multiLine", "true")
    else:
        leitor = leitor.option("header", "true")

    (
        leitor.load(f"{BASE}/{fonte}")
        .withColumn("arquivo_origem", F.col("_metadata.file_path"))
        .withColumn("carregado_em", F.current_timestamp())
        .writeStream
        .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(f"{SCHEMA}.{tabela}")
        .awaitTermination()
    )

# COMMAND ----------

contagens = []
for fonte, (formato, tabela) in FONTES.items():
    antes = contar(tabela)
    carregar(fonte, formato, tabela)
    depois = contar(tabela)
    contagens.append((tabela, antes, depois, depois - antes))

display(spark.createDataFrame(contagens, "tabela string, antes long, depois long, novos long"))

# COMMAND ----------

# arquivos carregados por tabela (prova de que nenhum arquivo entrou duas vezes)
for _, tabela in FONTES.values():
    display(
        spark.table(f"{SCHEMA}.{tabela}")
        .groupBy("arquivo_origem").count()
        .orderBy("arquivo_origem")
    )
