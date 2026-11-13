"""
BusLight - DAG de coleta

Roda 4 vezes por dia (6h, 12h, 18h e 22h) para ter espera de manhã, tarde e noite.
Baixa as fontes em paralelo, confere se os arquivos desta execução chegaram em chegada/
e manda um aviso no final (ou quando alguma tarefa falha).

Variables (Admin -> Variables):
  SPTRANS_TOKEN     obrigatória
  BUSLIGHT_CHEGADA  pasta de pouso (padrão: chegada/ na raiz do repo)
  BUSLIGHT_PONTOS   lista de códigos de parada para substituir os pontos monitorados, ex: [560009166, 260016859]
  BUSLIGHT_WEBHOOK  webhook do Discord/Slack/Teams para os avisos
"""
import json
import logging
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, get_current_context, task

sys.path.insert(0, str(Path(__file__).resolve().parent))
from buslight import coletores, usuarios  # noqa: E402

log = logging.getLogger(__name__)

PASTA_PADRAO = str(Path(__file__).resolve().parent.parent / "chegada")


def pasta_chegada():
    return Variable.get("BUSLIGHT_CHEGADA", default=PASTA_PADRAO)


def data_coleta():
    # data no fuso de SP: o ds do Airflow vem em UTC (a coleta das 22h viraria o dia seguinte)
    # e a execução manual não tem data lógica, então usa a hora do disparo
    contexto = get_current_context()
    momento = contexto.get("logical_date") or contexto["dag_run"].run_after
    return pendulum.instance(momento).in_timezone("America/Sao_Paulo").to_date_string()


def lista_variable(nome):
    return json.loads(Variable.get(nome, default="[]"))


def avisar(mensagem):
    log.warning(mensagem)
    webhook = Variable.get("BUSLIGHT_WEBHOOK", default="")
    if webhook:
        try:
            requests.post(webhook, json={"content": mensagem, "text": mensagem}, timeout=10)
        except requests.RequestException as erro:
            log.error("Não foi possível enviar o aviso: %s", erro)


def avisar_falha(context):
    ti = context["task_instance"]
    avisar(f"[BusLight] Falha em {ti.task_id} ({context.get('ds') or 'manual'}): {context.get('exception')}")


default_args = {
    "owner": "buslight",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": avisar_falha,
}

with DAG(
    dag_id="buslight_coleta",
    description="Coleta das fontes do BusLight (4x por dia)",
    schedule="0 6,12,18,22 * * *",
    start_date=pendulum.datetime(2026, 11, 10, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["buslight", "gs-2026"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    @task
    def coletar_posicao():
        return str(coletores.coletar_posicao(Variable.get("SPTRANS_TOKEN"), pasta_chegada(), data_coleta()))

    @task
    def coletar_previsao():
        arquivo = coletores.coletar_previsao(
            Variable.get("SPTRANS_TOKEN"),
            pasta_chegada(),
            data_coleta(),
            pontos=lista_variable("BUSLIGHT_PONTOS"),
        )
        return str(arquivo)

    @task
    def coletar_clima():
        return str(coletores.coletar_clima(pasta_chegada(), data_coleta()))

    @task
    def coletar_ibge():
        return str(coletores.coletar_ibge(pasta_chegada(), data_coleta()))

    @task
    def gerar_usuarios():
        return str(usuarios.gerar_usuarios(pasta_chegada(), data_coleta(), lista_variable("BUSLIGHT_PONTOS")))

    @task
    def conferir_chegada(arquivos):
        return coletores.conferir_chegada(arquivos)

    @task
    def notificar(resumo):
        avisar(f"[BusLight] Coleta de {data_coleta()} concluída: {len(resumo)} arquivos -> {', '.join(resumo.values())}")

    coletas = [coletar_posicao(), coletar_previsao(), coletar_clima(), coletar_ibge(), gerar_usuarios()]
    inicio >> coletas

    notificar(conferir_chegada(coletas))
