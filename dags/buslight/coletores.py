"""
Coleta das fontes do BusLight (SPTrans, Open-Meteo e IBGE).
Cada função baixa uma fonte e salva um arquivo em chegada/<fonte>/ com a data no nome.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

log = logging.getLogger(__name__)

FUSO = ZoneInfo("America/Sao_Paulo")
TIMEOUT = 30

SPTRANS_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
IBGE_MUNICIPIOS_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/mesorregioes/3515/municipios"
IBGE_POPULACAO_URL = "https://apisidra.ibge.gov.br/values/t/6579/n6/{codigos}/v/9324/p/last"

# zona leste de SP (Guaianases)
LATITUDE = -23.545
LONGITUDE = -46.415

FONTES = ("sptrans_posicao", "sptrans_previsao", "clima", "ibge", "usuarios")


def agora():
    return datetime.now(FUSO)


def caminho_arquivo(chegada, fonte, data, extensao="json"):
    destino = Path(chegada) / fonte / f"{fonte}_{data}_{agora():%H%M}.{extensao}"
    destino.parent.mkdir(parents=True, exist_ok=True)
    return destino


def salvar_json(destino, conteudo):
    destino.write_text(json.dumps(conteudo, ensure_ascii=False), encoding="utf-8")
    log.info("Arquivo salvo: %s (%d bytes)", destino, destino.stat().st_size)
    return destino


def login_sptrans(token):
    if not token:
        raise ValueError("Variable SPTRANS_TOKEN não configurada")

    sessao = requests.Session()
    # a API retorna erro 411 se o POST for sem corpo
    resp = sessao.post(f"{SPTRANS_URL}/Login/Autenticar", params={"token": token}, data="", timeout=TIMEOUT)
    resp.raise_for_status()
    if resp.text.strip().lower() != "true":
        raise PermissionError("Token recusado pela SPTrans")
    return sessao


def coletar_posicao(token, chegada, data):
    sessao = login_sptrans(token)
    resp = sessao.get(f"{SPTRANS_URL}/Posicao", timeout=TIMEOUT)
    resp.raise_for_status()

    conteudo = {"coletado_em": agora().isoformat(), "fonte": "sptrans/Posicao", "dados": resp.json()}
    return salvar_json(caminho_arquivo(chegada, "sptrans_posicao", data), conteudo)


def buscar_pontos(sessao, termos, limite):
    pontos = {}
    for termo in termos:
        resp = sessao.get(f"{SPTRANS_URL}/Parada/Buscar", params={"termosBusca": termo}, timeout=TIMEOUT)
        resp.raise_for_status()
        for parada in resp.json()[:limite]:
            pontos[parada["cp"]] = {**parada, "termo_busca": termo}
    return list(pontos.values())


def coletar_previsao(token, chegada, data, pontos=None, termos=None, limite=8):
    sessao = login_sptrans(token)

    # sem lista fixa de pontos, busca pelos bairros do piloto
    if pontos:
        paradas = [{"cp": cp} for cp in pontos]
    else:
        paradas = buscar_pontos(sessao, termos or ["Guaianases", "Itaquera", "Tiradentes"], limite)

    registros, falhas = [], []
    for parada in paradas:
        try:
            resp = sessao.get(f"{SPTRANS_URL}/Previsao/Parada", params={"codigoParada": parada["cp"]}, timeout=TIMEOUT)
            resp.raise_for_status()
            registros.append({"parada": parada, "previsao": resp.json()})
        except requests.RequestException as erro:
            log.warning("Falha no ponto %s: %s", parada["cp"], erro)
            falhas.append(parada["cp"])

    if not registros:
        raise RuntimeError("Nenhuma previsão coletada")

    conteudo = {
        "coletado_em": agora().isoformat(),
        "fonte": "sptrans/Previsao/Parada",
        "pontos_com_falha": falhas,
        "dados": registros,
    }
    return salvar_json(caminho_arquivo(chegada, "sptrans_previsao", data), conteudo)


def coletar_clima(chegada, data):
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "hourly": "temperature_2m,precipitation,rain",
        "start_date": data,
        "end_date": data,
        "timezone": "America/Sao_Paulo",
    }
    resp = requests.get(OPEN_METEO_URL, params=params, timeout=TIMEOUT)
    resp.raise_for_status()

    conteudo = {"coletado_em": agora().isoformat(), "fonte": "open-meteo/forecast", "dados": resp.json()}
    return salvar_json(caminho_arquivo(chegada, "clima", data), conteudo)


def coletar_ibge(chegada, data):
    resp = requests.get(IBGE_MUNICIPIOS_URL, timeout=TIMEOUT)
    resp.raise_for_status()
    municipios = resp.json()

    codigos = ",".join(str(m["id"]) for m in municipios)
    resp = requests.get(IBGE_POPULACAO_URL.format(codigos=codigos), timeout=TIMEOUT)
    resp.raise_for_status()

    conteudo = {
        "coletado_em": agora().isoformat(),
        "fonte": "ibge/localidades+sidra-6579",
        "municipios": municipios,
        "populacao": resp.json()[1:],  # primeira linha do SIDRA é o cabeçalho
    }
    return salvar_json(caminho_arquivo(chegada, "ibge", data), conteudo)


def conferir_chegada(chegada, data, fontes=FONTES):
    encontrados, faltando = {}, []
    for fonte in fontes:
        arquivos = [a for a in (Path(chegada) / fonte).glob(f"{fonte}_{data}_*") if a.stat().st_size > 0]
        if arquivos:
            encontrados[fonte] = sorted(a.name for a in arquivos)
        else:
            faltando.append(fonte)

    if faltando:
        raise FileNotFoundError(f"Faltando arquivos de {data}: {', '.join(faltando)}")
    log.info("Todos os arquivos chegaram: %s", encontrados)
    return encontrados
