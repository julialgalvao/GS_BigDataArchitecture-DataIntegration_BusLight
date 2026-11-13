"""
Testes dos coletores e do gerador de usuários (rodam sem internet e sem Airflow).
Uso: pytest tests/
"""
import csv
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "dags"))
from buslight import coletores, usuarios  # noqa: E402


def test_nome_do_arquivo_tem_fonte_data_e_hora(tmp_path):
    destino = coletores.caminho_arquivo(tmp_path, "clima", "2026-11-12")
    assert destino.parent == tmp_path / "clima"
    assert destino.name.startswith("clima_2026-11-12_")
    assert destino.suffix == ".json"


def test_conferencia_passa_com_todos_os_arquivos(tmp_path):
    arquivos = []
    for fonte in ["clima", "ibge"]:
        arquivo = coletores.caminho_arquivo(tmp_path, fonte, "2026-11-12")
        arquivo.write_text("{}")
        arquivos.append(str(arquivo))
    assert set(coletores.conferir_chegada(arquivos)) == {"clima", "ibge"}


def test_conferencia_falha_com_arquivo_vazio(tmp_path):
    vazio = coletores.caminho_arquivo(tmp_path, "clima", "2026-11-12")
    vazio.write_text("")
    with pytest.raises(FileNotFoundError):
        coletores.conferir_chegada([str(vazio)])


def test_conferencia_falha_com_arquivo_ausente(tmp_path):
    with pytest.raises(FileNotFoundError):
        coletores.conferir_chegada([str(tmp_path / "nao_existe.json")])


def test_login_sem_token_falha():
    with pytest.raises(ValueError):
        coletores.login_sptrans("")


def test_pontos_monitorados():
    pontos = coletores.pontos_monitorados()
    assert len(pontos) == 144
    assert len({p["codigo_parada"] for p in pontos}) == 144
    assert {p["codigo_corredor"] for p in pontos} == {7, 8}


def test_usuarios_simulados(tmp_path):
    arquivo = usuarios.gerar_usuarios(tmp_path, "2026-11-12")
    linhas = list(csv.DictReader(arquivo.open(encoding="utf-8")))
    assert len(linhas) == 220
    assert len({linha["id_usuario"] for linha in linhas}) == 220

    # defeitos propositais para as regras de qualidade da Prata
    assert any(linha["telefone"] == "" for linha in linhas)
    assert any(linha["codigo_parada_favorita"] == "0" for linha in linhas)

    # os 200 usuários fixos são iguais em lotes de dias diferentes (MERGE)
    outro = usuarios.gerar_usuarios(tmp_path, "2026-11-13")
    outras = list(csv.DictReader(outro.open(encoding="utf-8")))
    fixos = lambda ls: [(x["id_usuario"], x["telefone"]) for x in ls[:200]]  # noqa: E731
    assert fixos(linhas) == fixos(outras)
