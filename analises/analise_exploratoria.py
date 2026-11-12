"""
BusLight - análise exploratória dos dados coletados

Lê os arquivos da pasta chegada/ (os mesmos que sobem para o Databricks) e gera os
gráficos em docs/graficos/ e um resumo.json com os números usados no relatório.
Serve para conferir os resultados da Ouro e para mostrar o que os dados dizem.

Uso: python analises/analise_exploratoria.py [pasta_chegada]
"""
import json
import statistics
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

RAIZ = Path(__file__).resolve().parent.parent
CHEGADA = Path(sys.argv[1]) if len(sys.argv) > 1 else RAIZ / "chegada"
SAIDA = RAIZ / "docs" / "graficos"
SAIDA.mkdir(parents=True, exist_ok=True)

AZUL, LARANJA, VERDE, CINZA = "#1F3A5F", "#E8833A", "#3A9D5D", "#9AA5B1"
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})


def faixa(hora):
    if hora < 6:
        return "madrugada"
    if hora < 12:
        return "manhã"
    if hora < 18:
        return "tarde"
    return "noite"


def minutos(hhmm):
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def espera(previsto, referencia):
    e = minutos(previsto) - minutos(referencia)
    return e + 1440 if e < -720 else e


def salvar(fig, nome):
    fig.tight_layout()
    fig.savefig(SAIDA / nome, dpi=180)
    plt.close(fig)


# ------------------------------------------------------------ leitura
previsoes = []      # uma linha por ponto + linha + coleta (espera do próximo ônibus)
veiculos_prev = []  # cada veículo previsto (para acessibilidade)
for arquivo in sorted((CHEGADA / "sptrans_previsao").glob("*.json")):
    conteudo = json.loads(arquivo.read_text(encoding="utf-8"))
    coletado = datetime.fromisoformat(conteudo["coletado_em"])
    for registro in conteudo["dados"]:
        parada = registro["parada"]
        p = registro["previsao"].get("p") or {}
        hr = registro["previsao"].get("hr")
        for linha in p.get("l") or []:
            esperas = [espera(v["t"], hr) for v in linha["vs"]]
            esperas = [e for e in esperas if 0 <= e <= 180]
            if not esperas:
                continue
            previsoes.append({
                "coleta": arquivo.stem,
                "data": coletado.date().isoformat(),
                "hora": coletado.hour,
                "faixa": faixa(coletado.hour),
                "ponto": parada.get("nome") or str(parada["codigo_parada"]),
                "codigo_parada": parada["codigo_parada"],
                "corredor": parada.get("corredor", "-"),
                "linha": linha["c"],
                "espera": min(esperas),
                "veiculos": len(esperas),
            })
            for v in linha["vs"]:
                veiculos_prev.append(bool(v.get("a")))

frota = []          # ônibus em circulação por coleta
acessiveis_frota = 0
total_frota = 0
for arquivo in sorted((CHEGADA / "sptrans_posicao").glob("*.json")):
    conteudo = json.loads(arquivo.read_text(encoding="utf-8"))
    vs = [v for linha in conteudo["dados"]["l"] for v in linha["vs"]]
    coletado = datetime.fromisoformat(conteudo["coletado_em"])
    frota.append((coletado, len(vs)))
    acessiveis_frota += sum(1 for v in vs if v.get("a"))
    total_frota += len(vs)

chuva_por_hora = {}
for arquivo in sorted((CHEGADA / "clima").glob("*.json")):
    h = json.loads(arquivo.read_text(encoding="utf-8"))["dados"]["hourly"]
    for t, mm in zip(h["time"], h["precipitation"]):
        chuva_por_hora[t[:13]] = mm or 0.0

if not previsoes:
    sys.exit("Nenhuma previsão encontrada em " + str(CHEGADA))

resumo = {
    "coletas": sorted({p["coleta"] for p in previsoes}),
    "dias": sorted({p["data"] for p in previsoes}),
    "pontos": len({p["codigo_parada"] for p in previsoes}),
    "linhas": len({p["linha"] for p in previsoes}),
    "observacoes": len(previsoes),
}
todas = [p["espera"] for p in previsoes]
resumo["espera_media"] = round(statistics.mean(todas), 1)
resumo["espera_mediana"] = statistics.median(todas)
resumo["espera_p90"] = sorted(todas)[int(len(todas) * 0.9)]
resumo["pct_acima_20min"] = round(100 * sum(e > 20 for e in todas) / len(todas), 1)
resumo["pct_acima_30min"] = round(100 * sum(e > 30 for e in todas) / len(todas), 1)

# ------------------------------------------------------------ G1 distribuição da espera
fig, ax = plt.subplots(figsize=(8, 4))
ax.hist(todas, bins=range(0, max(todas) + 5, 5), color=AZUL, edgecolor="white")
ax.axvline(resumo["espera_mediana"], color=LARANJA, linestyle="--", label=f"mediana: {resumo['espera_mediana']} min")
ax.axvline(20, color=CINZA, linestyle=":", label="20 min")
ax.set_xlabel("Espera pelo próximo ônibus da linha (min)")
ax.set_ylabel("Ponto × linha × coleta")
ax.set_title("Distribuição da espera nos pontos monitorados")
ax.legend()
salvar(fig, "g1_distribuicao_espera.png")

# ------------------------------------------------------------ G2 espera por faixa horária
por_faixa = defaultdict(list)
for p in previsoes:
    por_faixa[p["faixa"]].append(p["espera"])
ordem = [f for f in ["madrugada", "manhã", "tarde", "noite"] if f in por_faixa]
fig, ax = plt.subplots(figsize=(7, 4))
ax.boxplot([por_faixa[f] for f in ordem], tick_labels=ordem, showfliers=False,
           patch_artist=True, boxprops=dict(facecolor="#D9E2F3", color=AZUL), medianprops=dict(color=LARANJA, lw=2))
for i, f in enumerate(ordem, start=1):
    ax.text(i, statistics.median(por_faixa[f]) + 1, f"{statistics.mean(por_faixa[f]):.1f} min (média)", ha="center", fontsize=8.5)
ax.set_ylabel("Espera (min)")
ax.set_title("Espera por faixa horária da coleta")
salvar(fig, "g2_espera_por_faixa.png")
resumo["espera_media_por_faixa"] = {f: round(statistics.mean(por_faixa[f]), 1) for f in ordem}

# ------------------------------------------------------------ G3 top 10 pontos com maior espera
por_ponto = defaultdict(list)
for p in previsoes:
    por_ponto[p["ponto"]].append(p["espera"])
top = sorted(((statistics.mean(v), k) for k, v in por_ponto.items() if len(v) >= 3), reverse=True)[:10]
fig, ax = plt.subplots(figsize=(8, 4.5))
ax.barh([k for _, k in top][::-1], [m for m, _ in top][::-1], color=LARANJA)
for i, (m, _) in enumerate(top[::-1]):
    ax.text(m + 0.3, i, f"{m:.1f}", va="center", fontsize=8.5)
ax.set_xlabel("Espera média (min)")
ax.set_title("10 pontos com maior espera média")
salvar(fig, "g3_top10_pontos.png")
resumo["top_pontos"] = [{"ponto": k, "espera_media": round(m, 1)} for m, k in top]

# ------------------------------------------------------------ G4 espera por corredor
por_corredor = defaultdict(list)
for p in previsoes:
    por_corredor[p["corredor"].replace("Corredor ", "")].append(p["espera"])
fig, ax = plt.subplots(figsize=(7, 3.6))
nomes = list(por_corredor)
medias = [statistics.mean(por_corredor[n]) for n in nomes]
ax.bar(nomes, medias, color=[AZUL, VERDE][: len(nomes)])
for i, m in enumerate(medias):
    ax.text(i, m + 0.3, f"{m:.1f} min", ha="center")
ax.set_ylabel("Espera média (min)")
ax.set_title("Espera média por corredor")
salvar(fig, "g4_espera_por_corredor.png")
resumo["espera_media_por_corredor"] = {n: round(m, 1) for n, m in zip(nomes, medias)}

# ------------------------------------------------------------ G5 frota em circulação por coleta
fig, ax = plt.subplots(figsize=(8, 3.6))
ax.plot([c.strftime("%d/%m %Hh%M") for c, _ in frota], [n for _, n in frota], marker="o", color=AZUL)
ax.set_ylabel("Ônibus em circulação")
ax.set_title("Ônibus em circulação na cidade em cada coleta")
plt.setp(ax.get_xticklabels(), rotation=30, ha="right")
salvar(fig, "g5_frota_por_coleta.png")
resumo["frota_por_coleta"] = [{"coleta": c.isoformat(), "onibus": n} for c, n in frota]

# ------------------------------------------------------------ G6 acessibilidade
pct_frota = 100 * acessiveis_frota / total_frota if total_frota else 0
pct_prev = 100 * sum(veiculos_prev) / len(veiculos_prev) if veiculos_prev else 0
fig, ax = plt.subplots(figsize=(6.5, 3.4))
ax.barh(["Frota em circulação", "Ônibus previstos\nnos pontos monitorados"], [pct_frota, pct_prev], color=VERDE)
for i, v in enumerate([pct_frota, pct_prev]):
    ax.text(v + 1, i, f"{v:.1f}%", va="center")
ax.set_xlim(0, 110)
ax.set_xlabel("% de veículos acessíveis (rampa/elevador)")
ax.set_title("Acessibilidade da frota")
salvar(fig, "g6_acessibilidade.png")
resumo["pct_frota_acessivel"] = round(pct_frota, 1)
resumo["pct_previstos_acessiveis"] = round(pct_prev, 1)

# ------------------------------------------------------------ G7 chuva × espera (só se houver chuva)
com_chuva, sem_chuva = [], []
for p in previsoes:
    mm = chuva_por_hora.get(f"{p['data']}T{p['hora']:02d}", 0.0)
    (com_chuva if mm > 0 else sem_chuva).append(p["espera"])
resumo["coletas_com_chuva"] = len(com_chuva)
if com_chuva and sem_chuva:
    fig, ax = plt.subplots(figsize=(6, 3.6))
    vals = [statistics.mean(sem_chuva), statistics.mean(com_chuva)]
    ax.bar(["Sem chuva", "Com chuva"], vals, color=[CINZA, AZUL])
    for i, v in enumerate(vals):
        ax.text(i, v + 0.3, f"{v:.1f} min", ha="center")
    ax.set_ylabel("Espera média (min)")
    ax.set_title("Espera com e sem chuva")
    salvar(fig, "g7_chuva_espera.png")
    resumo["espera_sem_chuva"] = round(vals[0], 1)
    resumo["espera_com_chuva"] = round(vals[1], 1)

# ------------------------------------------------------------ impacto estimado
MARGEM = 2
evitado = [max(e - MARGEM, 0) for e in todas]
resumo["minutos_evitados_por_viagem"] = round(statistics.mean(evitado), 1)
resumo["horas_evitadas_mes_por_usuario"] = round(statistics.mean(evitado) * 22 / 60, 1)

(SAIDA / "resumo.json").write_text(json.dumps(resumo, ensure_ascii=False, indent=1), encoding="utf-8")
print(json.dumps(resumo, ensure_ascii=False, indent=1))
