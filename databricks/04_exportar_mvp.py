# Databricks notebook source
# BusLight - exportação para o MVP
# Gera um JSON por visão da Ouro no volume. Os arquivos são baixados e
# copiados para app/data/ no repositório de Cloud.
# Da dim_usuario sai só o telefone mascarado.

import json

SCHEMA = "workspace.buslight"
DESTINO = "/Volumes/workspace/buslight/chegada/_mvp"
dbutils.fs.mkdirs(DESTINO)

exportar = {
    "mapa_pontos": f"SELECT * FROM {SCHEMA}.vw_mvp_mapa_pontos",
    "ranking_linhas": f"SELECT * FROM {SCHEMA}.vw_mvp_ranking_linhas ORDER BY posicao",
    "saia_de_casa": f"SELECT * FROM {SCHEMA}.vw_mvp_saia_de_casa",
    "usuarios": f"SELECT sk_usuario, telefone_mascarado, sk_ponto_favorito, faixa_horaria_preferida FROM {SCHEMA}.dim_usuario",
}

for nome, consulta in exportar.items():
    linhas = [r.asDict() for r in spark.sql(consulta).collect()]
    with open(f"{DESTINO}/{nome}.json", "w", encoding="utf-8") as f:
        json.dump(linhas, f, ensure_ascii=False, default=str)
    print(f"{nome}.json: {len(linhas)} registros")

display(dbutils.fs.ls(DESTINO))
