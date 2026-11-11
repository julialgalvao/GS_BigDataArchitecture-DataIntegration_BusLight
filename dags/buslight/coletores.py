"""Coletores do BusLight.

Funções puras (sem dependência do Airflow) que baixam cada fonte e gravam
um arquivo por coleta na pasta de pouso ``chegada/<fonte>/``.
O DAG ``buslight_coleta`` apenas chama estas funções; assim elas podem ser
testadas fora do Airflow.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

FUSO = ZoneInfo("America/Sao_Paulo")
TIMEOUT = 30

SPTRANS_BASE = "https://api.olhovivo.sptrans.com.br/v2.1"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes/3515/municipios"
IBGE_POPULACAO_URL = "https://apisidra.ibge.gov.br/values/t/6579/n6/{codigos}/v/9324/p/last"

# Centro aproximado do território do piloto (zona leste de São Paulo)
LATITUDE_PILOTO = -23.545
LONGITUDE_PILOTO = -46.415

# Fontes esperadas na pasta de pouso (usado pela conferência)
FONTES = ("sptrans_posicao", "sptrans_previsao", "clima", "ibge", "usuarios")


# ---------------------------------------------------------------- utilidades
def agora() -> datetime:
    return datetime.now(FUSO)


def caminho_arquivo(chegada: str | Path, fonte: str, data: str, extensao: str = "json") -> Path:
    """Monta ``chegada/<fonte>/<fonte>_<AAAA-MM-DD>_<HHMM>.<ext>``.

    A data no nome segue a data lógica da execução; o horário diferencia
    coletas feitas no mesmo dia (ex.: manhã e noite).
    """
    destino = Path(chegada) / fonte / f"{fonte}_{data}_{agora():%H%M}.{extensao}"
    destino.parent.mkdir(parents=True, exist_ok=True)
    return destino


def salvar_json(destino: Path, conteudo: dict | list) -> Path:
    destino.write_text(json.dumps(conteudo, ensure_ascii=False), encoding="utf-8")
    log.info("Gravado %s (%d bytes)", destino, destino.stat().st_size)
    return destino


# ------------------------------------------------------------------- SPTrans
def sessao_sptrans(token: str) -> requests.Session:
    """Autentica na API Olho Vivo. A API devolve ``true``/``false`` e usa cookie."""
    if not token:
        raise ValueError("Variable SPTRANS_TOKEN não configurada.")
    sessao = requests.Session()
    # A API recusa POST sem Content-Length (HTTP 411); data="" garante o cabeçalho.
    resposta = sessao.post(f"{SPTRANS_BASE}/Login/Autenticar", params={"token": token}, data="", timeout=TIMEOUT)
    resposta.raise_for_status()
    if resposta.text.strip().lower() != "true":
        raise PermissionError("SPTrans recusou o token (resposta 'false'). Confira se a chave já foi ativada.")
    return sessao


def coletar_posicao(token: str, chegada: str | Path, data: str) -> Path:
    """Posição GPS de todos os ônibus em circulação (endpoint /Posicao)."""
    sessao = sessao_sptrans(token)
    resposta = sessao.get(f"{SPTRANS_BASE}/Posicao", timeout=TIMEOUT)
    resposta.raise_for_status()
    dados = resposta.json()
    conteudo = {"coletado_em": agora().isoformat(), "fonte": "sptrans/Posicao", "dados": dados}
    return salvar_json(caminho_arquivo(chegada, "sptrans_posicao", data), conteudo)


def buscar_pontos(sessao: requests.Session, termos: list[str], limite_por_termo: int) -> list[dict]:
    """Descobre pontos de parada por termo de busca (ex.: nome do bairro)."""
    pontos: dict[int, dict] = {}
    for termo in termos:
        resposta = sessao.get(f"{SPTRANS_BASE}/Parada/Buscar", params={"termosBusca": termo}, timeout=TIMEOUT)
        resposta.raise_for_status()
        for parada in resposta.json()[:limite_por_termo]:
            pontos[parada["cp"]] = {**parada, "termo_busca": termo}
    return list(pontos.values())


def coletar_previsao(
    token: str,
    chegada: str | Path,
    data: str,
    pontos: list[int] | None = None,
    termos: list[str] | None = None,
    limite_por_termo: int = 8,
) -> Path:
    """Previsão de chegada das linhas nos pontos monitorados (/Previsao/Parada).

    Usa a lista fixa ``pontos`` (Variable BUSLIGHT_PONTOS) se existir; senão
    descobre os pontos pelos ``termos`` de busca (bairros do piloto).
    """
    sessao = sessao_sptrans(token)
    if pontos:
        cadastro = [{"cp": codigo} for codigo in pontos]
    else:
        cadastro = buscar_pontos(sessao, termos or ["Guaianases", "Itaquera", "Tiradentes"], limite_por_termo)

    registros, falhas = [], []
    for ponto in cadastro:
        try:
            resposta = sessao.get(f"{SPTRANS_BASE}/Previsao/Parada", params={"codigoParada": ponto["cp"]}, timeout=TIMEOUT)
            resposta.raise_for_status()
            registros.append({"parada": ponto, "previsao": resposta.json()})
        except requests.RequestException as erro:  # um ponto com falha não derruba a coleta
            log.warning("Falha na previsão do ponto %s: %s", ponto["cp"], erro)
            falhas.append(ponto["cp"])

    if not registros:
        raise RuntimeError("Nenhuma previsão coletada.")
    conteudo = {
        "coletado_em": agora().isoformat(),
        "fonte": "sptrans/Previsao/Parada",
        "pontos_com_falha": falhas,
        "dados": registros,
    }
    return salvar_json(caminho_arquivo(chegada, "sptrans_previsao", data), conteudo)


# ---------------------------------------------------------------- Open-Meteo
def coletar_clima(chegada: str | Path, data: str) -> Path:
    """Chuva e temperatura por hora no território do piloto, para a data da execução."""
    params = {
        "latitude": LATITUDE_PILOTO,
        "longitude": LONGITUDE_PILOTO,
        "hourly": "temperature_2m,precipitation,rain",
        "start_date": data,
        "end_date": data,
        "timezone": "America/Sao_Paulo",
    }
    resposta = requests.get(OPEN_METEO_URL, params=params, timeout=TIMEOUT)
    resposta.raise_for_status()
    conteudo = {"coletado_em": agora().isoformat(), "fonte": "open-meteo/forecast", "dados": resposta.json()}
    return salvar_json(caminho_arquivo(chegada, "clima", data), conteudo)


# ---------------------------------------------------------------------- IBGE
def coletar_ibge(chegada: str | Path, data: str) -> Path:
    """Municípios da Região Metropolitana de SP e população estimada (SIDRA 6579)."""
    municipios = requests.get(IBGE_MUNICIPIOS_URL, timeout=TIMEOUT)
    municipios.raise_for_status()
    lista = municipios.json()
    codigos = ",".join(str(m["id"]) for m in lista)
    populacao = requests.get(IBGE_POPULACAO_URL.format(codigos=codigos), timeout=TIMEOUT)
    populacao.raise_for_status()
    conteudo = {
        "coletado_em": agora().isoformat(),
        "fonte": "ibge/localidades+sidra-6579",
        "municipios": lista,
        "populacao": populacao.json()[1:],  # a 1ª linha do SIDRA é o cabeçalho
    }
    return salvar_json(caminho_arquivo(chegada, "ibge", data), conteudo)


# ---------------------------------------------------------------- conferência
def conferir_chegada(chegada: str | Path, data: str, fontes: tuple[str, ...] = FONTES) -> dict[str, list[str]]:
    """Confere se cada fonte tem ao menos um arquivo não vazio da data. Falha se faltar algum."""
    encontrados, faltando = {}, []
    for fonte in fontes:
        arquivos = [a for a in (Path(chegada) / fonte).glob(f"{fonte}_{data}_*") if a.stat().st_size > 0]
        if arquivos:
            encontrados[fonte] = sorted(a.name for a in arquivos)
        else:
            faltando.append(fonte)
    if faltando:
        raise FileNotFoundError(f"Arquivos ausentes ou vazios em {data}: {', '.join(faltando)}")
    log.info("Conferência OK: %s", encontrados)
    return encontrados
