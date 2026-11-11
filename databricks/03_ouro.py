# Databricks notebook source
# BusLight - camada Ouro
# Modelo dimensional (fato_espera_ponto + dimensões), proteção do telefone (LGPD)
# e as visões que o app consome.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.buslight"
MARGEM_MIN = 2  # chegar 2 min antes do ônibus

previsao = spark.table(f"{SCHEMA}.prata_previsao_parada")
clima = spark.table(f"{SCHEMA}.prata_clima_hora")


def salvar(df, tabela):
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SCHEMA}.{tabela}")
    print(f"{tabela}: {spark.table(f'{SCHEMA}.{tabela}').count()} linhas")


def chave_hora(coluna):
    return F.date_format(coluna, "yyyyMMddHH").cast("long")


def faixa_horaria(coluna_hora):
    return (
        F.when(coluna_hora < 6, "madrugada")
        .when(coluna_hora < 12, "manha")
        .when(coluna_hora < 18, "tarde")
        .otherwise("noite")
    )

# COMMAND ----------

# MAGIC %md ## Dimensões

# COMMAND ----------

dim_ponto = (
    previsao.groupBy("codigo_parada")
    .agg(
        F.first("nome_parada", ignorenulls=True).alias("nome"),
        F.first("endereco", ignorenulls=True).alias("endereco"),
        F.first("latitude").alias("latitude"),
        F.first("longitude").alias("longitude"),
        F.coalesce(F.first("regiao", ignorenulls=True), F.lit("Zona Leste")).alias("regiao"),
    )
    .withColumn("sk_ponto", F.col("codigo_parada"))
)
salvar(dim_ponto, "dim_ponto")

dim_linha = (
    previsao.groupBy("codigo_linha")
    .agg(
        F.first("letreiro").alias("letreiro"),
        F.first("sentido").alias("sentido"),
        F.first("terminal_principal").alias("terminal_principal"),
        F.first("terminal_secundario").alias("terminal_secundario"),
    )
    .withColumn("sk_linha", F.col("codigo_linha"))
)
salvar(dim_linha, "dim_linha")

dim_tempo = (
    previsao.select(F.date_trunc("hour", "data_hora_coleta").alias("hora_cheia")).distinct()
    .select(
        chave_hora("hora_cheia").alias("sk_tempo"),
        F.to_date("hora_cheia").alias("data"),
        F.hour("hora_cheia").alias("hora"),
        F.date_format("hora_cheia", "EEEE").alias("dia_semana"),
        faixa_horaria(F.hour("hora_cheia")).alias("faixa_horaria"),
    )
)
salvar(dim_tempo, "dim_tempo")

dim_clima = clima.select(
    chave_hora("data_hora").alias("sk_clima"),
    "temperatura_c",
    "precipitacao_mm",
    F.when(F.col("precipitacao_mm") == 0, "sem chuva")
    .when(F.col("precipitacao_mm") < 2.5, "fraca")
    .when(F.col("precipitacao_mm") < 7.6, "moderada")
    .otherwise("forte")
    .alias("faixa_chuva"),
)
salvar(dim_clima, "dim_clima")

# COMMAND ----------

# MAGIC %md ## Usuários com o telefone protegido (LGPD)
# MAGIC O telefone aberto fica só na Prata. Na Ouro existe apenas o hash (uso interno) e a versão mascarada (exibição no app).

# COMMAND ----------

dim_usuario = spark.table(f"{SCHEMA}.prata_usuario").select(
    F.col("id_usuario").alias("sk_usuario"),
    F.sha2("telefone", 256).alias("telefone_hash"),
    F.concat(F.lit("("), F.substring("telefone", 1, 2), F.lit(") *****-"), F.substring("telefone", -4, 4)).alias("telefone_mascarado"),
    F.col("codigo_parada_favorita").alias("sk_ponto_favorito"),
    "faixa_horaria_preferida",
    "cadastrado_em",
)
salvar(dim_usuario, "dim_usuario")

# COMMAND ----------

# MAGIC %md ## Fato: espera no ponto
# MAGIC Uma linha por ponto + linha + coleta. A espera é a do próximo ônibus; o atraso compara com a espera típica daquele ponto, linha e faixa horária.

# COMMAND ----------

fato = (
    previsao.groupBy("codigo_parada", "codigo_linha", "data_hora_coleta")
    .agg(F.min("espera_min").alias("espera_min"), F.count("*").alias("qtd_veiculos_previstos"))
    .withColumn("sk_tempo", chave_hora("data_hora_coleta"))
    .withColumn("sk_clima", chave_hora("data_hora_coleta"))
    .withColumn("faixa", faixa_horaria(F.hour("data_hora_coleta")))
)

tipica = Window.partitionBy("codigo_parada", "codigo_linha", "faixa")
fato = (
    fato.withColumn("atraso_min", F.round(F.col("espera_min") - F.avg("espera_min").over(tipica), 1))
    .select(
        F.col("codigo_parada").alias("sk_ponto"),
        F.col("codigo_linha").alias("sk_linha"),
        "sk_tempo",
        "sk_clima",
        "data_hora_coleta",
        "espera_min",
        "atraso_min",
        "qtd_veiculos_previstos",
    )
)
salvar(fato, "fato_espera_ponto")

# COMMAND ----------

# MAGIC %md ## Visões consumidas pelo MVP

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {SCHEMA}.vw_mvp_mapa_pontos AS
SELECT p.codigo_parada, p.nome, p.endereco, p.latitude, p.longitude, p.regiao,
       t.faixa_horaria,
       ROUND(AVG(f.espera_min), 1) AS espera_media_min,
       COUNT(*)                    AS amostras
FROM {SCHEMA}.fato_espera_ponto f
JOIN {SCHEMA}.dim_ponto p ON f.sk_ponto = p.sk_ponto
JOIN {SCHEMA}.dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SCHEMA}.vw_mvp_ranking_linhas AS
SELECT l.codigo_linha, l.letreiro, l.terminal_principal, l.terminal_secundario,
       ROUND(AVG(f.espera_min), 1)                  AS espera_media_min,
       ROUND(AVG(GREATEST(f.atraso_min, 0)), 1)     AS atraso_medio_min,
       RANK() OVER (ORDER BY AVG(f.espera_min) DESC) AS posicao
FROM {SCHEMA}.fato_espera_ponto f
JOIN {SCHEMA}.dim_linha l ON f.sk_linha = l.sk_linha
GROUP BY ALL
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SCHEMA}.vw_mvp_saia_de_casa AS
SELECT p.codigo_parada, p.nome AS ponto, l.letreiro AS linha, t.faixa_horaria,
       ROUND(AVG(f.espera_min), 1)                        AS espera_sem_aviso_min,
       {MARGEM_MIN}                                       AS espera_com_aviso_min,
       ROUND(GREATEST(AVG(f.espera_min) - {MARGEM_MIN}, 0), 1) AS minutos_evitados
FROM {SCHEMA}.fato_espera_ponto f
JOIN {SCHEMA}.dim_ponto p ON f.sk_ponto = p.sk_ponto
JOIN {SCHEMA}.dim_linha l ON f.sk_linha = l.sk_linha
JOIN {SCHEMA}.dim_tempo t ON f.sk_tempo = t.sk_tempo
GROUP BY ALL
""")

display(spark.table(f"{SCHEMA}.vw_mvp_mapa_pontos").orderBy(F.desc("espera_media_min")).limit(10))
