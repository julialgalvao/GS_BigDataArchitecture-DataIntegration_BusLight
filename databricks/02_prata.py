# Databricks notebook source
# BusLight - camada Prata
# Abre o JSON aninhado, corrige tipos, trata nulos/inválidos, aplica as regras
# de qualidade (com o total barrado por regra) e grava com MERGE pela chave.

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SCHEMA = "workspace.buslight"
qualidade = []

# COMMAND ----------

def merge(df, tabela, chaves):
    destino = f"{SCHEMA}.{tabela}"
    if not spark.catalog.tableExists(destino):
        df.limit(0).write.saveAsTable(destino)

    df.createOrReplaceTempView("lote")
    condicao = " AND ".join(f"alvo.{c} = novo.{c}" for c in chaves)
    spark.sql(f"""
        MERGE INTO {destino} AS alvo
        USING lote AS novo
        ON {condicao}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"{destino}: {spark.table(destino).count()} linhas")


def aplicar_regra(df, numero, descricao, tabela, condicao_invalida):
    barrados = df.filter(condicao_invalida).count()
    qualidade.append((numero, descricao, tabela, barrados))
    return df.filter(~condicao_invalida)

# COMMAND ----------

# MAGIC %md ## Posição dos ônibus

# COMMAND ----------

posicao = (
    spark.table(f"{SCHEMA}.bronze_sptrans_posicao")
    .select(F.to_timestamp("coletado_em").alias("coletado_em"), F.explode("dados.l").alias("linha"))
    .select("coletado_em", "linha", F.explode("linha.vs").alias("v"))
    .select(
        F.col("v.p").cast("string").alias("prefixo_veiculo"),
        F.col("linha.cl").cast("int").alias("codigo_linha"),
        F.col("linha.c").alias("letreiro"),
        F.col("linha.sl").cast("int").alias("sentido"),
        F.col("linha.lt0").alias("terminal_principal"),
        F.col("linha.lt1").alias("terminal_secundario"),
        F.col("v.a").cast("boolean").alias("acessivel"),
        F.to_timestamp("v.ta").alias("data_hora"),
        F.col("v.py").cast("double").alias("latitude"),
        F.col("v.px").cast("double").alias("longitude"),
        "coletado_em",
    )
    .dropna(subset=["prefixo_veiculo", "data_hora", "latitude", "longitude"])
)

# regra 1: coordenada fora da Grande São Paulo
fora_de_sp = ~(F.col("latitude").between(-24.1, -23.2) & F.col("longitude").between(-47.2, -46.1))
posicao = aplicar_regra(posicao, 1, "Coordenada GPS fora da Grande São Paulo", "prata_posicao_onibus", fora_de_sp)

# regra 2: mesmo veículo registrado mais de uma vez no mesmo minuto
janela = Window.partitionBy("prefixo_veiculo", F.date_trunc("minute", "data_hora")).orderBy(F.col("coletado_em").desc())
posicao = posicao.withColumn("ordem", F.row_number().over(janela))
posicao = aplicar_regra(posicao, 2, "Registro duplicado do mesmo veículo no mesmo minuto", "prata_posicao_onibus", F.col("ordem") > 1).drop("ordem")

# regra 3: GPS parado/desatualizado (leitura com mais de 30 min em relação à coleta)
desatualizado = (F.unix_timestamp("coletado_em") - F.unix_timestamp("data_hora")) > 30 * 60
posicao = aplicar_regra(posicao, 3, "Posição GPS desatualizada (mais de 30 min)", "prata_posicao_onibus", desatualizado)

merge(posicao, "prata_posicao_onibus", ["prefixo_veiculo", "data_hora"])

# COMMAND ----------

# MAGIC %md ## Previsão de chegada nos pontos

# COMMAND ----------

def minutos(coluna_hhmm):
    partes = F.split(coluna_hhmm, ":")
    return partes.getItem(0).cast("int") * 60 + partes.getItem(1).cast("int")


previsao = (
    spark.table(f"{SCHEMA}.bronze_sptrans_previsao")
    .select(F.to_timestamp("coletado_em").alias("data_hora_coleta"), F.explode("dados").alias("d"))
    .select(
        "data_hora_coleta",
        F.col("d.parada.termo_busca").alias("regiao"),
        F.col("d.parada.ed").alias("endereco"),
        F.col("d.previsao.hr").alias("hora_referencia"),
        F.col("d.previsao.p").alias("p"),
    )
    .select("*", F.explode("p.l").alias("linha"))
    .select("*", F.explode("linha.vs").alias("v"))
    .select(
        F.col("p.cp").cast("long").alias("codigo_parada"),
        F.col("p.np").alias("nome_parada"),
        "endereco",
        "regiao",
        F.col("p.py").cast("double").alias("latitude"),
        F.col("p.px").cast("double").alias("longitude"),
        F.col("linha.cl").cast("int").alias("codigo_linha"),
        F.col("linha.c").alias("letreiro"),
        F.col("linha.sl").cast("int").alias("sentido"),
        F.col("linha.lt0").alias("terminal_principal"),
        F.col("linha.lt1").alias("terminal_secundario"),
        F.col("v.p").cast("string").alias("prefixo_veiculo"),
        F.col("v.a").cast("boolean").alias("acessivel"),
        F.col("v.t").alias("horario_previsto"),
        "hora_referencia",
        "data_hora_coleta",
    )
    .dropna(subset=["codigo_parada", "codigo_linha", "horario_previsto"])
)

# espera em minutos entre a hora da consulta e a chegada prevista (tratando a virada do dia)
espera = minutos("horario_previsto") - minutos("hora_referencia")
previsao = previsao.withColumn("espera_min", F.when(espera < -720, espera + 1440).otherwise(espera))

# regra 4: previsão no passado ou espera acima de 3 horas
invalida = (F.col("espera_min") < 0) | (F.col("espera_min") > 180)
previsao = aplicar_regra(previsao, 4, "Previsão no passado ou espera acima de 180 min", "prata_previsao_parada", invalida)

merge(previsao, "prata_previsao_parada", ["codigo_parada", "codigo_linha", "prefixo_veiculo", "data_hora_coleta"])

# COMMAND ----------

# MAGIC %md ## Clima por hora

# COMMAND ----------

clima = (
    spark.table(f"{SCHEMA}.bronze_clima")
    .select(F.explode(F.arrays_zip(
        "dados.hourly.time", "dados.hourly.temperature_2m", "dados.hourly.precipitation", "dados.hourly.rain"
    )).alias("h"))
    .select(
        F.to_timestamp("h.time").alias("data_hora"),
        F.col("h.temperature_2m").cast("double").alias("temperatura_c"),
        F.coalesce(F.col("h.precipitation").cast("double"), F.lit(0.0)).alias("precipitacao_mm"),
        F.coalesce(F.col("h.rain").cast("double"), F.lit(0.0)).alias("chuva_mm"),
    )
    .dropna(subset=["data_hora"])
    .dropDuplicates(["data_hora"])
)
merge(clima, "prata_clima_hora", ["data_hora"])

# COMMAND ----------

# MAGIC %md ## Municípios (IBGE)

# COMMAND ----------

bronze_ibge = spark.table(f"{SCHEMA}.bronze_ibge_municipio")

municipios = bronze_ibge.select(F.explode("municipios").alias("m")).select(
    F.col("m.id").cast("int").alias("codigo_municipio"),
    F.col("m.nome").alias("nome_municipio"),
)
populacao = bronze_ibge.select(F.explode("populacao").alias("p")).select(
    F.col("p.D1C").cast("int").alias("codigo_municipio"),
    F.col("p.V").cast("long").alias("populacao"),
    F.col("p.D3N").cast("int").alias("ano_referencia"),
)
municipio = municipios.join(populacao, "codigo_municipio", "left").dropDuplicates(["codigo_municipio"])
merge(municipio, "prata_municipio", ["codigo_municipio"])

# COMMAND ----------

# MAGIC %md ## Usuários (cadastro simulado)

# COMMAND ----------

pontos_validos = spark.table(f"{SCHEMA}.prata_previsao_parada").select("codigo_parada").distinct()

usuario = (
    spark.table(f"{SCHEMA}.bronze_usuario")
    .select(
        F.trim("id_usuario").alias("id_usuario"),
        F.regexp_replace("telefone", r"\D", "").alias("telefone"),
        F.col("codigo_parada_favorita").cast("long").alias("codigo_parada_favorita"),
        F.lower(F.trim("faixa_horaria_preferida")).alias("faixa_horaria_preferida"),
        F.to_date("cadastrado_em").alias("cadastrado_em"),
        F.to_timestamp("gerado_em").alias("gerado_em"),
    )
    .withColumn("ordem", F.row_number().over(Window.partitionBy("id_usuario").orderBy(F.col("gerado_em").desc())))
    .filter("ordem = 1").drop("ordem")
    .join(pontos_validos.withColumn("ponto_existe", F.lit(True)),
          F.col("codigo_parada_favorita") == pontos_validos.codigo_parada, "left")
    .drop("codigo_parada")
)

# regra 5: usuário sem telefone ou com ponto favorito inexistente
sem_contato = (F.coalesce(F.length("telefone"), F.lit(0)) < 10) | F.col("ponto_existe").isNull()
usuario = aplicar_regra(usuario, 5, "Usuário sem telefone ou com ponto favorito inexistente", "prata_usuario", sem_contato).drop("ponto_existe")

merge(usuario, "prata_usuario", ["id_usuario"])

# COMMAND ----------

# MAGIC %md ## Resultado das regras de qualidade

# COMMAND ----------

log_qualidade = (
    spark.createDataFrame(qualidade, "regra int, descricao string, tabela string, registros_barrados long")
    .withColumn("executado_em", F.current_timestamp())
)
log_qualidade.write.mode("append").saveAsTable(f"{SCHEMA}.prata_qualidade_log")
display(log_qualidade)
