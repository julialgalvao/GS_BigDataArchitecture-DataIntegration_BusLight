# Global Solutions - Big Data Architecture & Data Integration - BusLight

> *Luz para você ir a qualquer destino.*
> Global Solution 2026.2 · CitizenTech · FIAP · Engenharia de Software · 4º ano (4ES)

Componente **Dados** do BusLight: o pipeline que coleta dados reais de ônibus, clima e população com **Apache Airflow**, trata em camadas **Bronze, Prata e Ouro** no **Databricks** e entrega ao app o que o cidadão vê no ponto de ônibus.

---

## 👥 Integrantes

| Nome completo | RM |
|---|---|
| Aline Fernandes Zeppelini | RM97966 |
| Camilly Breitbach Ishida | RM551474 |
| Julia Leite Galvão | RM550201 |

---

## 📑 Sumário

1. [O produto](#-o-produto)
2. [Ficha Técnica do MVP](#-ficha-técnica-do-mvp)
3. [As seis perguntas do briefing](#-as-seis-perguntas-do-briefing)
4. [ODS conectados](#-ods-conectados)
5. [Perguntas que o MVP responde](#-perguntas-que-o-mvp-responde-ao-cidadão)
6. [Arquitetura](#️-arquitetura)
7. [Fontes de dados](#-fontes-de-dados)
8. [Coleta no Airflow](#️-coleta-no-airflow)
9. [Camadas no Databricks](#-camadas-no-databricks)
10. [Consultas SQL](#-consultas-sql-da-ouro)
11. [Contrato com o MVP](#-contrato-com-o-mvp)
12. [LGPD](#-lgpd)
13. [Atendimento aos requisitos](#-atendimento-aos-requisitos-da-disciplina)
14. [Estrutura do repositório](#-estrutura-do-repositório)
15. [Como reproduzir](#️-como-reproduzir)
16. [Links](#-links)

---

## 🚏 O produto

**BusLight** é um ponto de ônibus inteligente, previsível e seguro.

Na periferia de São Paulo, o trabalhador que depende do ônibus espera muito tempo no ponto, muitas vezes à noite e em lugar escuro, sem saber se o ônibus já passou ou se vai demorar. O BusLight usa a posição real dos ônibus para mostrar quando ele realmente chega e avisar a hora certa de sair de casa, reduzindo o tempo em que a pessoa fica exposta no ponto.

**Território do piloto:** pontos de ônibus da zona leste de São Paulo (região de Itaquera, Guaianases e Cidade Tiradentes), onde as viagens são longas e a espera noturna é maior.

---

## 🧩 Ficha Técnica do MVP

| Campo | Conteúdo |
|---|---|
| Nome do produto | BusLight |
| Cidadão e território | Trabalhador da periferia da zona leste de São Paulo que depende de ônibus, principalmente quem sai cedo ou volta tarde; com atenção a mulheres, idosos e pessoas com deficiência |
| Problema, em uma frase | O passageiro espera tempo demais no ponto, sem saber quando o ônibus chega, muitas vezes à noite e em lugar inseguro |
| Proposta de valor | Saber quando o ônibus chega e a hora certa de sair de casa, para esperar o mínimo possível no ponto |
| Arquitetura: componentes | App web + API (Cloud) · Pipeline de dados (este repositório) · Assistente de IA generativa · Visão computacional no ponto · Robô de alertas (RPA) · Totem físico com sensores (Maker) |
| Fontes de dados | SPTrans Olho Vivo, Open-Meteo, IBGE e cadastro de usuários simulado |
| Modelos de IA utilizados | LLM local com RAG e agente (assistente do passageiro) · YOLO customizado (lotação do ponto) |
| URL do MVP e repositório | MVP: [preencher] · Repositório: [preencher] |
| Indicadores de impacto | Minutos de espera evitados, pontos críticos identificados, usuários noturnos atendidos |
| ODS conectados | 11, 10 e 9 |

---

## 🧭 As seis perguntas do briefing

| Pergunta | Resposta |
|---|---|
| **Quem é o cidadão beneficiado?** | O trabalhador da periferia de São Paulo que depende do ônibus, principalmente em horário noturno |
| **Qual problema será resolvido?** | A espera longa, imprevisível e insegura no ponto de ônibus |
| **Como a solução funcionará?** | Coleta a posição real dos ônibus e a previsão de chegada nos pontos, cruza com o clima, calcula espera e atraso por ponto, linha e horário, e mostra no app quando o ônibus chega e a hora certa de sair de casa |
| **Quais tecnologias serão empregadas?** | Neste componente: Apache Airflow 3, Databricks Free Edition (Auto Loader, Delta Lake, Spark SQL), Python e APIs públicas |
| **Quem participará do ecossistema?** | Passageiros (usam de graça), Prefeitura e SPTrans (painel de gestão de linhas e pontos críticos), empresas de mídia em ponto de ônibus e empresas com funcionários de turno noturno |
| **Como o impacto poderá ser medido e ampliado?** | Medido pelos minutos de espera evitados e pelos pontos críticos identificados (consulta de impacto na Ouro); ampliado levando o mesmo pipeline a outras regiões e cidades com dados abertos de transporte |

---

## 🌍 ODS conectados

| ODS | Meta | Como o BusLight contribui |
|---|---|---|
| **11 – Cidades e comunidades sustentáveis** | 11.2 – transporte seguro, acessível e sustentável, com atenção a mulheres, crianças, pessoas com deficiência e idosos | Torna o transporte público mais previsível e reduz o tempo de exposição no ponto |
| **10 – Redução das desigualdades** | 10.2 – inclusão social e econômica de todos | Foca na periferia, onde a espera é maior e o acesso a informação de qualidade é menor |
| **9 – Indústria, inovação e infraestrutura** | 9.1 – infraestrutura de qualidade, confiável e acessível | Transforma dados abertos de transporte em infraestrutura de informação para o cidadão e para a gestão pública |

---

## ❓ Perguntas que o MVP responde ao cidadão

| # | Pergunta | Consulta SQL |
|---|---|---|
| 1 | Quanto tempo, em média, se espera em cada ponto à noite? | `01_espera_noturna_por_ponto.sql` |
| 2 | Quais linhas mais atrasam? | `02_ranking_linhas_atraso.sql` |
| 3 | Quanto a chuva aumenta a espera? | `03_efeito_chuva_espera.sql` |
| 4 | Quais pontos são críticos (maior espera noturna)? | `04_pontos_criticos.sql` |
| 5 | **Impacto:** quantos minutos de exposição no ponto o usuário economiza com o aviso "saia de casa às..."? | `05_impacto_minutos_evitados.sql` |

---

## 🏗️ Arquitetura

```
  FONTES                 AIRFLOW 3 (local)                 DATABRICKS FREE EDITION                     MVP
┌───────────────┐   ┌──────────────────────────┐   ┌──────────────────────────────────────────┐   ┌──────────┐
│ SPTrans       │   │ DAG buslight_coleta      │   │  Volume /Volumes/workspace/buslight/     │   │ BusLight │
│  /Posicao     │──▶│  @daily                  │   │         chegada/  (upload em lotes)      │   │   app    │
│  /Previsao    │──▶│  ┌─ coletar_posicao  ─┐  │   │               │                          │   │          │
│ Open-Meteo    │──▶│  ├─ coletar_previsao ─┤  │   │   Auto Loader ▼                          │   │  Mapa    │
│ IBGE          │──▶│  ├─ coletar_clima    ─┼─▶│──▶│  BRONZE  (bruto, incremental)            │   │  Linhas  │
│ Cadastro sim. │──▶│  ├─ coletar_ibge     ─┤  │   │               │ tipos, nulos, MERGE,     │   │  Saia de │
└───────────────┘   │  └─ gerar_usuarios   ─┘  │   │               ▼ regras de qualidade      │──▶│  casa às │
                    │   conferir_chegada       │   │  PRATA   (tratado e confiável)           │   │  Meus    │
                    │   notificar              │   │               │ modelo dimensional       │   │  alertas │
                    └──────────────────────────┘   │               ▼ + hash/máscara (LGPD)    │   └──────────┘
                       pasta local chegada/        │  OURO    (fato + dimensões + visões MVP) │
                                                   └──────────────────────────────────────────┘
```

> **Por que o upload é manual?** O Databricks Free Edition não se conecta ao Airflow local. Os arquivos da pasta `chegada/` sobem ao Volume pela interface do Databricks, em **lotes de dias diferentes**, e o Auto Loader garante que cada arquivo seja carregado uma única vez.

---

## 📊 Fontes de dados

| Fonte | Origem | O que fornece | Formato | Frequência |
|---|---|---|---|---|
| SPTrans Olho Vivo – `/Posicao` | API pública externa (token gratuito) | Posição GPS de todos os ônibus em circulação | JSON | Diária (amostras ao longo do dia) |
| SPTrans Olho Vivo – `/Previsao/Parada` | API pública externa (token gratuito) | Previsão de chegada das linhas nos pontos monitorados | JSON | Diária (amostras ao longo do dia) |
| Open-Meteo | API aberta externa, sem chave | Chuva e temperatura por hora em São Paulo | JSON | Diária |
| IBGE – Localidades e SIDRA | API aberta externa | Municípios e população estimada | JSON | Estática (1 coleta) |
| Cadastro de usuários | Dado **simulado**, gerado por script com semente fixa | Usuário, telefone fictício e ponto favorito | CSV | Por lote |

> **Sobre o dado simulado:** o cadastro é gerado pelo próprio DAG (`gerar_usuarios`) com a biblioteca Faker e semente fixa, para ser reproduzível. Nenhum dado pessoal real é usado. Ele existe para demonstrar a proteção de dados pessoais exigida pela LGPD na camada Ouro.

Uma amostra de cada fonte está em [`amostras/`](amostras/).

---

## ⚙️ Coleta no Airflow

**DAG:** [`dags/buslight_coleta.py`](dags/buslight_coleta.py) · **Agendamento:** `@daily` · **Airflow:** 3.4.0 · **Coletores:** [`dags/buslight/`](dags/buslight/)

```
                     ┌─ coletar_posicao  ─┐
                     ├─ coletar_previsao ─┤
inicio ──────────────┼─ coletar_clima    ─┼──▶ conferir_chegada ──▶ notificar
                     ├─ coletar_ibge     ─┤
                     └─ gerar_usuarios   ─┘
```

| Tarefa | O que faz |
|---|---|
| `inicio` | Marca o início da execução |
| `coletar_posicao` | Autentica na SPTrans e salva a posição de todos os ônibus |
| `coletar_previsao` | Salva a previsão de chegada para cada ponto monitorado (lista fixa ou descoberta pelos bairros do piloto); um ponto com falha é registrado sem derrubar a coleta |
| `coletar_clima` | Salva chuva e temperatura por hora do dia da execução |
| `coletar_ibge` | Salva os municípios da Região Metropolitana de SP e a população estimada |
| `gerar_usuarios` | Gera o lote do cadastro simulado |
| `conferir_chegada` | Verifica se todas as fontes têm arquivo do dia e não vazio; falha se faltar algum |
| `notificar` | Envia o resumo da execução; em caso de falha, o `on_failure_callback` dispara o aviso |

**Resiliência:** cada tarefa tem 2 novas tentativas automáticas (intervalo de 2 minutos) antes de falhar.

**Padrão dos arquivos na pasta de pouso** (um arquivo por coleta, com a data no nome e o horário para diferenciar coletas do mesmo dia):

```
chegada/
├── sptrans_posicao/   sptrans_posicao_2026-11-12_0630.json
├── sptrans_previsao/  sptrans_previsao_2026-11-12_0630.json
├── clima/             clima_2026-11-12_0630.json
├── ibge/              ibge_2026-11-12_0630.json
└── usuarios/          usuarios_2026-11-12_0630.csv
```

**Configuração no Airflow (Admin → Variables, nunca no código):**

| Variable | Obrigatória | Conteúdo |
|---|---|---|
| `SPTRANS_TOKEN` | Sim | Token da API Olho Vivo |
| `BUSLIGHT_CHEGADA` | Não | Caminho da pasta de pouso (padrão: `chegada/` na raiz do repositório) |
| `BUSLIGHT_PONTOS` | Não | Lista JSON dos códigos de parada monitorados, ex.: `[340015333, 260015039]` |
| `BUSLIGHT_TERMOS` | Não | Lista JSON de bairros usados para descobrir pontos quando não há lista fixa (padrão: `["Guaianases", "Itaquera", "Tiradentes"]`) |
| `BUSLIGHT_WEBHOOK` | Não | URL de webhook (Discord, Slack ou Teams) para receber os avisos |

---

## 🥉🥈🥇 Camadas no Databricks

Todas as tabelas ficam no schema **`workspace.buslight`**. Notebooks em [`databricks/`](databricks/).

### Bronze — dado bruto, incremental e sem duplicação

| Fonte | Tabela Bronze | Carga |
|---|---|---|
| SPTrans – posição | `bronze_sptrans_posicao` | Auto Loader (`cloudFiles`, JSON) |
| SPTrans – previsão | `bronze_sptrans_previsao` | Auto Loader (`cloudFiles`, JSON) |
| Open-Meteo | `bronze_clima` | Auto Loader (`cloudFiles`, JSON) |
| IBGE | `bronze_ibge_municipio` | Auto Loader (`cloudFiles`, JSON) |
| Cadastro simulado | `bronze_usuario` | Auto Loader (`cloudFiles`, CSV) |

- Cada tabela guarda `arquivo_origem` e `carregado_em` para auditoria.
- `checkpointLocation` e `schemaLocation` próprios por fonte em `chegada/_controle/`.
- `trigger(availableNow=True)`: processa só o que é novo e para.
- **Prova de idempotência:** contagem após o 1º lote, após o 2º lote (só arquivos novos entram) e após rodar de novo sem arquivo novo (contagem não muda).

### Prata — dado tratado e confiável

| Tabela Prata | Tratamento | Chave do `MERGE` |
|---|---|---|
| `prata_posicao_onibus` | Explode o JSON aninhado (linhas → veículos), tipos de data/hora e coordenadas | `prefixo_veiculo` + `data_hora` |
| `prata_previsao_parada` | Explode pontos → linhas → veículos, calcula `espera_min` | `codigo_parada` + `codigo_linha` + `data_hora_coleta` |
| `prata_clima_hora` | `arrays_zip` + `explode` das listas paralelas do Open-Meteo | `data_hora` |
| `prata_municipio` | Junta municípios com população | `codigo_municipio` |
| `prata_usuario` | Padroniza telefone, valida campos obrigatórios | `id_usuario` |

**Regras de qualidade** (cada uma com o total de registros barrados, gravado em `prata_qualidade_log`):

| # | Regra | Tabela |
|---|---|---|
| 1 | Coordenada GPS fora da Grande São Paulo | `prata_posicao_onibus` |
| 2 | Registro duplicado do mesmo veículo no mesmo minuto | `prata_posicao_onibus` |
| 3 | Velocidade impossível entre duas leituras (acima de 120 km/h) | `prata_posicao_onibus` |
| 4 | Previsão de chegada no passado ou com espera acima de 180 minutos | `prata_previsao_parada` |
| 5 | Usuário sem telefone ou com ponto favorito inexistente | `prata_usuario` |

### Ouro — modelo dimensional pronto para o MVP

```
                 ┌─────────────┐
                 │  dim_tempo  │
                 └──────┬──────┘
┌─────────────┐  ┌──────┴────────────┐  ┌─────────────┐
│  dim_ponto  ├──┤ fato_espera_ponto ├──┤  dim_linha  │
└─────────────┘  └──────┬────────────┘  └─────────────┘
                 ┌──────┴──────┐        ┌─────────────┐
                 │  dim_clima  │        │ dim_usuario │──▶ ponto favorito (dim_ponto)
                 └─────────────┘        └─────────────┘
```

| Tabela | Tipo | Colunas principais |
|---|---|---|
| `fato_espera_ponto` | Fato | `sk_ponto`, `sk_linha`, `sk_tempo`, `sk_clima`, `espera_min`, `atraso_min`, `qtd_veiculos_previstos` |
| `dim_ponto` | Dimensão | `sk_ponto`, `codigo_parada`, `nome`, `endereco`, `latitude`, `longitude`, `regiao` |
| `dim_linha` | Dimensão | `sk_linha`, `codigo_linha`, `letreiro`, `sentido`, `terminal_principal`, `terminal_secundario` |
| `dim_tempo` | Dimensão | `sk_tempo`, `data`, `hora`, `dia_semana`, `faixa_horaria` (madrugada, manhã, tarde, noite) |
| `dim_clima` | Dimensão | `sk_clima`, `faixa_chuva` (sem chuva, fraca, moderada, forte), `temperatura_c` |
| `dim_usuario` | Dimensão | `sk_usuario`, `telefone_hash` (SHA-256), `telefone_mascarado`, `sk_ponto_favorito` |

**Atraso:** `atraso_min` = espera observada − espera típica daquele ponto, linha e faixa horária.

---

## 🔎 Consultas SQL da Ouro

Arquivos em [`databricks/sql/`](databricks/sql/). Resultados e interpretação no relatório PDF.

| # | Consulta | Pergunta | Tipo |
|---|---|---|---|
| 1 | `01_espera_noturna_por_ponto.sql` | Espera média por ponto à noite | Cidadão |
| 2 | `02_ranking_linhas_atraso.sql` | Linhas com mais atraso | Gestão |
| 3 | `03_efeito_chuva_espera.sql` | Espera com chuva × sem chuva | Análise |
| 4 | `04_pontos_criticos.sql` | Pontos com maior espera noturna | Gestão |
| 5 | `05_impacto_minutos_evitados.sql` | Minutos de exposição evitados por usuário com o "saia de casa às..." | **Impacto** |

---

## 🔌 Contrato com o MVP

A Ouro entrega ao app três visões prontas e uma dimensão de usuários protegida. O app (repositório de Cloud) consome uma exportação em JSON dessas visões.

| O MVP consome | Tabela/visão da Ouro | Tela do app que exibe |
|---|---|---|
| Espera média por ponto e faixa horária | `vw_mvp_mapa_pontos` | **Mapa de pontos** – cor de cada ponto pela espera |
| Linhas com mais atraso | `vw_mvp_ranking_linhas` | **Linhas** – ranking de atraso |
| Melhor horário para sair de casa | `vw_mvp_saia_de_casa` | **Saia de casa às...** – horário recomendado |
| Usuário e ponto favorito | `dim_usuario` (só `telefone_mascarado`, ex.: `(11) *****-1234`) | **Meus alertas** – nunca exibe o telefone completo |

---

## 🔒 LGPD

| Princípio | Como é aplicado |
|---|---|
| Credenciais fora do código | Token da SPTrans em **Airflow Variables**; nada de chave, senha ou token no repositório |
| Minimização | O único dado pessoal é o telefone para alerta; nome e localização em tempo real **não são coletados** |
| Proteção na Ouro | `dim_usuario` guarda o telefone em **hash SHA-256** (uso interno) e **mascarado** (exibição); o telefone aberto não sai da Prata |
| Dados fictícios | Cadastro 100% simulado; nenhum dado pessoal real no repositório nem nas amostras |
| Transparência | O app informa o que é coletado e por quê |

---

## ✅ Atendimento aos requisitos da disciplina

| Req. | Requisito | Onde está |
|---|---|---|
| 01 | Mínimo de 2 fontes, ao menos 1 API pública | 4 fontes externas (SPTrans ×2, Open-Meteo, IBGE) + cadastro simulado |
| 02 | DAG agendado salvando um arquivo por coleta com data no nome | `dags/buslight_coleta.py` (`@daily`, `chegada/<fonte>/<fonte>_<data>`) |
| 03 | 2+ tarefas em paralelo, 1 tarefa de conferência, chaves em Variables | 5 coletas em paralelo, `conferir_chegada`, `SPTRANS_TOKEN` em Variable |
| 04 | Arquivos enviados ao Volume em 2+ lotes de dias diferentes | Lote 1 e lote 2 em `/Volumes/workspace/buslight/chegada` |
| 05 | Bronze com Auto Loader, incremental e sem duplicar | `databricks/01_bronze.py` + contagens antes/depois |
| 06 | Prata com tipos, nulos, `MERGE` e 3+ regras de qualidade com contagem | `databricks/02_prata.py` (5 regras, `prata_qualidade_log`) |
| 07 | Ouro com 1 fato, 2+ dimensões e 5 consultas SQL (1 de impacto) | `databricks/03_ouro.py` + `databricks/sql/` |
| 08 | Dados pessoais protegidos na Ouro e uso pelo MVP demonstrado | `dim_usuario` com hash/máscara + [Contrato com o MVP](#-contrato-com-o-mvp) |
| 09 | README com integrantes, arquitetura e passo a passo | Este arquivo |

---

## 📁 Estrutura do repositório

```
├── dags/
│   ├── buslight_coleta.py          # DAG de coleta
│   └── buslight/
│       ├── coletores.py            # SPTrans, Open-Meteo, IBGE e conferência
│       └── usuarios.py             # gerador do cadastro simulado
├── databricks/
│   ├── 00_setup.sql                # schema, volume e pastas
│   ├── 01_bronze.py                # Auto Loader
│   ├── 02_prata.py                 # tratamento, MERGE e qualidade
│   ├── 03_ouro.py                  # modelo dimensional, LGPD e visões do MVP
│   └── sql/                        # as 5 consultas
├── amostras/                       # um pouco de cada fonte
├── docs/                           # diagramas e prints
├── requirements.txt
├── .gitignore                      # chegada/, .venv/, .airflow/, .env
└── README.md
```

---

## ▶️ Como reproduzir

### Pré-requisitos
- Python 3.12
- Conta gratuita no [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
- Token gratuito da [API Olho Vivo da SPTrans](https://www.sptrans.com.br/desenvolvedores/)

### 1. Airflow (coleta)
```bash
git clone https://github.com/julialgalvao/GS_BigDataArchitecture-DataIntegration_BusLight.git
cd GS_BigDataArchitecture-DataIntegration_BusLight
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt
export AIRFLOW_HOME=$(pwd)/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow standalone
```
1. Acesse `http://localhost:8080` com o usuário e a senha exibidos no terminal.
2. Em **Admin → Variables**, crie ao menos `SPTRANS_TOKEN` (as demais são opcionais, ver [Coleta no Airflow](#️-coleta-no-airflow)).
3. Ative o DAG `buslight_coleta`, clique em **Trigger** e acompanhe nas visões **Graph** e **Grid**.
4. Os arquivos aparecem em `chegada/`.

### 2. Databricks (lakehouse)
1. Importe os notebooks da pasta `databricks/` no workspace.
2. Rode `00_setup.sql` para criar o schema `workspace.buslight` e o volume `chegada`.
3. **Lote 1:** envie os arquivos do 1º dia de coleta ao volume pela interface e rode `01_bronze`.
4. **Lote 2:** envie os arquivos do 2º dia e rode `01_bronze` de novo (só os novos entram).
5. Rode `02_prata` e `03_ouro`.
6. Execute as consultas de `databricks/sql/`.

---

## 🔗 Links

| Item | Link |
|---|---|
| MVP (URL pública) | [preencher] |
| Repositório de Cloud (app que consome a Ouro) | [preencher] |
| Relatório PDF | [preencher] |
