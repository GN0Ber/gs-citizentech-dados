# GS 2026.2 CitizenTech · Componente Dados (Big Data Architecture & Data Integration)

**Produto (nome provisório: LuzViva):** inspeção e manutenção de iluminação pública. Um robô
mede a luz sob cada poste à noite, o app e a visão computacional recebem relatos com foto,
e o pipeline cruza tudo com o cadastro real da ILUME para dizer ao gestor quais luminárias
estão apagadas, onde e em que ordem atender.

**Perguntas que o MVP responde ao cidadão e ao gestor**
1. Como está a iluminação da minha região agora?
2. Quais luminárias a equipe de campo deve atender primeiro?
3. Quantas falhas o sistema achou antes de alguém reclamar?
4. Quanto tempo a cidade leva hoje para resolver poste e fiação pelo SP156?
5. Quando a visão diz "apagada", o robô confirma?
6. Depois que o chamado é aberto, quanto tempo leva o reparo, e o robô confirma a luz de volta?

## Integrantes

| Nome | RM |
| --- | --- |
| Caíque Walter Silva | RM550693 |
| Guilherme Nobre Bernardo | RM98604 |
| Guilherme Monteiro Espim | RM99499 |
| João Paulo Fonseca Zamperlini | RM99279 |
| Matheus José de Lima Costa | RM551157 |

## Arquitetura

```
GeoSampa WFS (cadastro ILUME) ─┐
SP156 (CKAN, postes/fiação) ───┼─> Airflow 3: gs_coleta_iluminacao (@daily)
Open-Meteo (sol, chuva) ───────┤     inicio >> [luminarias, sp156, clima] em paralelo
Robô LDR / app + visão / RPA ──┘     luminarias >> [medicoes_robo, ocorrencias_app]
                                     medicoes_robo >> chamados_rpa
                                     tudo >> conferir_chegada  (+ alerta de falha)
        │  chegada/<fonte>/<fonte>_AAAA-MM-DD.<ext>
        ▼  upload em lotes pela interface (Free Edition não conecta ao Airflow)
Databricks Volume workspace.gs.chegada
        ▼  01_bronze (Auto Loader, checkpoint: só arquivo novo entra)
bronze_*  ──02_prata (tipos, regras, quarentena, MERGE)──>  prata_*
        ▼  03_ouro
dim_luminaria · dim_regiao · dim_tempo · fato_inspecao · fato_ocorrencia · fato_chamado
        ▼  views vw_mvp_* ──05_export_mvp──> JSON para a API do MVP
```

## Como reproduzir

1. **Airflow 3 com interface, via Docker (recomendado no Windows)**
   Com o Docker Desktop aberto, na pasta do repositório: `docker compose up`. Abra
   http://localhost:8080, ative `gs_coleta_iluminacao` e acompanhe nas telas Graph e Grid.
   Os arquivos caem em `chegada/`. Alternativa sem Docker (Linux/WSL ou Mac):
   ```bash
   python -m venv .venv && source .venv/bin/activate
   pip install -r requirements.txt
   export AIRFLOW_HOME=~/airflow AIRFLOW__CORE__DAGS_FOLDER=$PWD/dags AIRFLOW__CORE__LOAD_EXAMPLES=False
   airflow standalone            # abre em http://localhost:8080
   airflow variables set gs_chegada "$PWD/chegada"
   airflow variables set gs_webhook_url "<webhook do Discord/Teams>"   # opcional; segredo, fora do repo
   ```
   Ative `gs_coleta_iluminacao`. Com `catchup=True` e início em 01/11/2026, o Airflow coleta
   todos os dias desde então, o que já dá lotes de dias diferentes.
   Para testar uma fonte sem Airflow: `cd dags && python -m coletores.cli iluminacao luminarias 2026-11-04`.
2. **Databricks Free Edition**: abra este repositório como **Git folder** (os notebooks
   importam `databricks/gs_lib`). Rode `00_setup`, suba o primeiro lote de `chegada/` no
   Volume `workspace.gs.chegada`, rode `01_bronze`, `02_prata` (informe o sal no widget),
   `03_ouro`, `04_consultas` e `05_export_mvp`. Suba o segundo lote e rode de novo a partir
   do `01_bronze` para mostrar a carga incremental.
3. **Testes locais**: `pytest` (coletores em 1 s; Prata e Ouro com PySpark local em ~2 min).

## Mapa dos requisitos

| Req. | Onde está |
|---|---|
| 01 fontes | `dags/coletores/temas.py`, `docs/fontes.md` |
| 02 DAG agendado | `dags/gs_coleta.py` (`@daily`, `chegada/<fonte>/<fonte>_<data>`) |
| 03 paralelismo, verificação, Variables | 3 coletas em paralelo, `conferir_chegada`, `gs_chegada`/`gs_webhook_url` |
| 04 dois lotes | upload de `chegada/` em dias diferentes |
| 05 Bronze incremental | `01_bronze.py` + `gs.controle_cargas` |
| 06 Prata | `02_prata.py`, `gs_lib/iluminacao.py` + `gs.controle_qualidade` |
| 07 Ouro e 5 SQL | `03_ouro.sql`, `04_consultas.sql` (6 consultas; Q3 e Q6 de impacto) |
| 08 LGPD e MVP | `cidadao_hash`, coordenada generalizada, `vw_mvp_*`, `docs/contrato_mvp.md` |
| 09 README | este arquivo |

## Repositório

```
dags/        DAG e coletores (luminárias, SP156, clima, simulados)
databricks/  notebooks 00 a 05 e a biblioteca gs_lib testada
amostras/    um pouco de cada fonte (luminárias e SP156 reais, recortados)
docs/        fontes, contrato com o MVP, dados simulados, roteiro do relatório
tests/       testes dos coletores, da Prata e da Ouro
```

Sem chaves, senhas ou dados pessoais reais. O sal da pseudonimização é digitado no widget
do notebook 02 e não fica salvo em lugar nenhum do repositório.
