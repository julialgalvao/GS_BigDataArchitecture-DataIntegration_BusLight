"""
BusLight - DAG de coleta diária

Baixa as fontes em paralelo, confere se todos os arquivos chegaram em chegada/
e manda um aviso no final (ou quando alguma tarefa falha).

Variables (Admin -> Variables):
  SPTRANS_TOKEN     obrigatória
  BUSLIGHT_CHEGADA  pasta de pouso (padrão: chegada/ na raiz do repo)
  BUSLIGHT_PONTOS   lista de códigos de parada, ex: [340015333, 260015039]
  BUSLIGHT_TERMOS   bairros para buscar pontos, ex: ["Guaianases", "Itaquera"]
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
    # execução manual no Airflow 3 não tem data lógica (ds vazio), então usa a data do disparo
    contexto = get_current_context()
    if contexto.get("ds"):
        return contexto["ds"]
    return pendulum.instance(contexto["dag_run"].run_after).in_timezone("America/Sao_Paulo").to_date_string()


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
    description="Coleta diária das fontes do BusLight",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 10, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["buslight", "gs-2026"],
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    @task
    def coletar_posicao():
        ds = data_coleta()
        return str(coletores.coletar_posicao(Variable.get("SPTRANS_TOKEN"), pasta_chegada(), ds))

    @task
    def coletar_previsao():
        ds = data_coleta()
        arquivo = coletores.coletar_previsao(
            Variable.get("SPTRANS_TOKEN"),
            pasta_chegada(),
            ds,
            pontos=lista_variable("BUSLIGHT_PONTOS"),
            termos=lista_variable("BUSLIGHT_TERMOS") or None,
        )
        return str(arquivo)

    @task
    def coletar_clima():
        ds = data_coleta()
        return str(coletores.coletar_clima(pasta_chegada(), ds))

    @task
    def coletar_ibge():
        ds = data_coleta()
        return str(coletores.coletar_ibge(pasta_chegada(), ds))

    @task
    def gerar_usuarios():
        ds = data_coleta()
        return str(usuarios.gerar_usuarios(pasta_chegada(), ds, lista_variable("BUSLIGHT_PONTOS")))

    @task
    def conferir_chegada():
        ds = data_coleta()
        return coletores.conferir_chegada(pasta_chegada(), ds)

    @task
    def notificar(resumo):
        ds = data_coleta()
        total = sum(len(arquivos) for arquivos in resumo.values())
        avisar(f"[BusLight] Coleta de {ds} concluída: {total} arquivos de {len(resumo)} fontes.")

    coletas = [coletar_posicao(), coletar_previsao(), coletar_clima(), coletar_ibge(), gerar_usuarios()]
    conferencia = conferir_chegada()

    inicio >> coletas >> conferencia
    notificar(conferencia)
