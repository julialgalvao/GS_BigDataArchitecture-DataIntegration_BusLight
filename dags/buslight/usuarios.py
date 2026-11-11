"""
Cadastro de usuários SIMULADO do BusLight (nenhum dado real).
Serve para mostrar a proteção do telefone (hash e máscara) na camada Ouro.

- 200 usuários fixos (semente 42) que se repetem em todo lote -> testa o MERGE
- 20 usuários novos por lote (semente pela data)
- ~5% sem telefone e ~5% com ponto inexistente (0) -> testa as regras de qualidade
"""
import csv
import random
from datetime import date

from faker import Faker

from buslight.coletores import agora, caminho_arquivo

CAMPOS = ["id_usuario", "telefone", "codigo_parada_favorita", "faixa_horaria_preferida", "cadastrado_em", "gerado_em"]
FAIXAS = ["madrugada", "manha", "tarde", "noite"]


def novo_usuario(fake, rng, id_usuario, pontos):
    telefone = "" if rng.random() < 0.05 else f"11 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"
    ponto = 0 if rng.random() < 0.05 or not pontos else rng.choice(pontos)

    return {
        "id_usuario": id_usuario,
        "telefone": telefone,
        "codigo_parada_favorita": ponto,
        "faixa_horaria_preferida": rng.choices(FAIXAS, weights=[2, 3, 1, 4])[0],
        "cadastrado_em": fake.date_between(start_date="-180d", end_date="today").isoformat(),
        "gerado_em": agora().isoformat(),
    }


def gerar_usuarios(chegada, data, pontos):
    fake = Faker("pt_BR")
    Faker.seed(42)

    rng = random.Random(42)
    usuarios = [novo_usuario(fake, rng, f"U{i:04d}", pontos) for i in range(1, 201)]

    dia = date.fromisoformat(data).toordinal()
    rng_dia = random.Random(dia)
    usuarios += [novo_usuario(fake, rng_dia, f"U{dia}{i:02d}", pontos) for i in range(1, 21)]

    destino = caminho_arquivo(chegada, "usuarios", data, extensao="csv")
    with destino.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CAMPOS)
        writer.writeheader()
        writer.writerows(usuarios)
    return destino
