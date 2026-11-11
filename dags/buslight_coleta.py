"""DAG de coleta do BusLight.

inicio -> [coletar_posicao, coletar_previsao, coletar_clima, coletar_ibge, gerar_usuarios]
       -> conferir_chegada -> notificar

Configuração (Admin -> Variables), nunca no código:
- SPTRANS_TOKEN     token da API Olho Vivo (obrigatória)
- BUSLIGHT_CHEGADA  caminho da pasta de pouso (padrão: <repo>/chegada)
- BUSLIGHT_PONTOS   JSON com códigos de parada, ex.: [340015333, 260015039] (opcional)
- BUSLIGHT_TERMOS   JSON com bairros para descobrir pontos (opcional)
- BUSLIGHT_WEBHOOK  URL de webhook (Discord/Slack/Teams) para avisos (opcional)
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import timedelta
from pathlib import Path

# Garante que o pacote buslight/ (ao lado deste arquivo) seja encontrado
sys.path.insert(0, str(Path(__file__).resolve().parent))

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, task

from buslight import coletores, usuarios

log = logging.getLogger(__name__)

PASTA_PADRAO = str(Path(__file__).resolve().parent.parent / "chegada")


def _chegada() -> str:
    return Variable.get("BUSLIGHT_CHEGADA", default=PASTA_PADRAO)


def _lista(nome: str) -> list:
    return json.loads(Variable.get(nome, default="[]"))


def _avisar(mensagem: str) -> None:
    """Registra no log e, se houver webhook configurado, envia o aviso."""
    log.warning(mensagem)
    webhook = Variable.get("BUSLIGHT_WEBHOOK", default="")
    if webhook:
        try:
            requests.post(webhook, json={"content": mensagem, "text": mensagem}, timeout=10)
        except requests.RequestException as erro:
            log.error("Falha ao enviar aviso pelo webhook: %s", erro)


def avisar_falha(context) -> None:
    ti = context["task_instance"]
    _avisar(f"[BusLight] Falha na tarefa {ti.task_id} do DAG {ti.dag_id} ({context['ds']}): {context.get('exception')}")


default_args = {
    "owner": "buslight",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": avisar_falha,
}

with DAG(
    dag_id="buslight_coleta",
    description="Coleta diária das fontes do BusLight para a pasta de pouso chegada/",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 10, tz="America/Sao_Paulo"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["buslight", "gs-2026", "coleta"],
) as dag:
    inicio = EmptyOperator(task_id="inicio")

    @task
    def coletar_posicao(ds=None) -> str:
        return str(coletores.coletar_posicao(Variable.get("SPTRANS_TOKEN"), _chegada(), ds))

    @task
    def coletar_previsao(ds=None) -> str:
        return str(
            coletores.coletar_previsao(
                Variable.get("SPTRANS_TOKEN"),
                _chegada(),
                ds,
                pontos=_lista("BUSLIGHT_PONTOS"),
                termos=_lista("BUSLIGHT_TERMOS") or None,
            )
        )

    @task
    def coletar_clima(ds=None) -> str:
        return str(coletores.coletar_clima(_chegada(), ds))

    @task
    def coletar_ibge(ds=None) -> str:
        return str(coletores.coletar_ibge(_chegada(), ds))

    @task
    def gerar_usuarios(ds=None) -> str:
        return str(usuarios.gerar_usuarios(_chegada(), ds, _lista("BUSLIGHT_PONTOS")))

    @task
    def conferir_chegada(ds=None) -> dict:
        return coletores.conferir_chegada(_chegada(), ds)

    @task
    def notificar(resumo: dict, ds=None) -> None:
        total = sum(len(arquivos) for arquivos in resumo.values())
        _avisar(f"[BusLight] Coleta de {ds} concluída: {total} arquivo(s) em {len(resumo)} fonte(s).")

    coletas = [coletar_posicao(), coletar_previsao(), coletar_clima(), coletar_ibge(), gerar_usuarios()]
    conferencia = conferir_chegada()
    inicio >> coletas >> conferencia
    notificar(conferencia)
