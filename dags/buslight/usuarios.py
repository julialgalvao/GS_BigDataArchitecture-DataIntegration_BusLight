"""Gerador do cadastro de usuários SIMULADO do BusLight.

Nenhum dado pessoal real é usado. O cadastro existe para demonstrar a
proteção de dados pessoais (hash e máscara) exigida pela LGPD na camada Ouro.

Regras de geração (documentadas para o relatório):
- 200 usuários base com semente fixa (42): aparecem em todos os lotes,
  com o mesmo id, para provar que o MERGE da Prata não duplica.
- 20 usuários novos por lote, com semente derivada da data da execução.
- ~5% dos registros sem telefone e ~5% com ponto favorito inexistente
  (código 0), de propósito, para alimentar as regras de qualidade da Prata.
"""
from __future__ import annotations

import csv
import random
from datetime import date
from pathlib import Path

from faker import Faker

from buslight.coletores import agora, caminho_arquivo

USUARIOS_BASE = 200
USUARIOS_NOVOS_POR_LOTE = 20
TAXA_SEM_TELEFONE = 0.05
TAXA_PONTO_INEXISTENTE = 0.05
CAMPOS = ["id_usuario", "telefone", "codigo_parada_favorita", "faixa_horaria_preferida", "cadastrado_em"]
FAIXAS = ["madrugada", "manha", "tarde", "noite"]


def _gerar(fake: Faker, rng: random.Random, id_usuario: str, pontos: list[int]) -> dict:
    telefone = "" if rng.random() < TAXA_SEM_TELEFONE else f"11 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"
    if rng.random() < TAXA_PONTO_INEXISTENTE or not pontos:
        ponto = 0
    else:
        ponto = rng.choice(pontos)
    return {
        "id_usuario": id_usuario,
        "telefone": telefone,
        "codigo_parada_favorita": ponto,
        "faixa_horaria_preferida": rng.choices(FAIXAS, weights=[2, 3, 1, 4])[0],
        "cadastrado_em": fake.date_between(start_date="-180d", end_date="today").isoformat(),
    }


def gerar_usuarios(chegada: str | Path, data: str, pontos: list[int]) -> Path:
    """Gera o lote do dia em ``chegada/usuarios/usuarios_<data>_<HHMM>.csv``."""
    fake = Faker("pt_BR")
    Faker.seed(42)
    rng_base = random.Random(42)
    linhas = [_gerar(fake, rng_base, f"U{i:04d}", pontos) for i in range(1, USUARIOS_BASE + 1)]

    semente_dia = date.fromisoformat(data).toordinal()
    rng_dia = random.Random(semente_dia)
    linhas += [
        _gerar(fake, rng_dia, f"U{semente_dia}{i:02d}", pontos) for i in range(1, USUARIOS_NOVOS_POR_LOTE + 1)
    ]

    destino = caminho_arquivo(chegada, "usuarios", data, extensao="csv")
    with destino.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(arquivo, fieldnames=CAMPOS + ["gerado_em"])
        escritor.writeheader()
        for linha in linhas:
            escritor.writerow({**linha, "gerado_em": agora().isoformat()})
    return destino
