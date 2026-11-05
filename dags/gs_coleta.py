"""DAGs de coleta da GS CitizenTech (Airflow 3).

Gera o DAG gs_coleta_iluminacao a partir de coletores/temas.py:

    inicio >> [luminarias, sp156, clima]          (em paralelo)
    luminarias >> [medicoes_robo, ocorrencias_app] (ancoradas no cadastro real)
    medicoes_robo >> chamados_rpa                  (o RPA abre chamado a partir da medição)
    todas >> conferir_chegada

Requisitos atendidos (slide 35 do enunciado):
  02  agendado (@daily), grava em chegada/<fonte>/<fonte>_<data>.<ext>
  03  >= 2 tarefas em paralelo, 1 tarefa que confere a chegada,
      chaves e caminhos em Variables (nunca no código), notificação de falha
"""
from __future__ import annotations

import json
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Variable

from coletores.comum import CHEGADA_PADRAO
from coletores.temas import DEPENDENCIAS, TEMAS

log = logging.getLogger(__name__)


def _chegada() -> Path:
    return Path(Variable.get("gs_chegada", default=str(CHEGADA_PADRAO)))


def _data_coleta(context) -> str:
    """Data lógica da execução.

    No Airflow 3 uma execução disparada à mão pode vir sem logical_date (e sem `ds`).
    Nesse caso coletamos o dia anterior ao disparo, que já está fechado nas fontes.
    """
    ds = context.get("ds")
    if ds:
        return ds
    run_after = context["dag_run"].run_after
    return (run_after.date() - timedelta(days=1)).isoformat()


def notificar_falha(context) -> None:
    """Callback de falha: log estruturado + webhook opcional (Discord/Slack/Teams).

    O endereço do webhook fica na Variable gs_webhook_url (é segredo: nunca no repo).
    """
    ti = context["task_instance"]
    msg = {
        "dag": ti.dag_id, "tarefa": ti.task_id, "data": _data_coleta(context),
        # Airflow 3 nem sempre preenche "exception" no contexto do callback
        "erro": str(context.get("exception") or context.get("error")
                    or getattr(ti, "state", "falhou"))[:500],
    }
    log.error("FALHA NA COLETA %s", json.dumps(msg, ensure_ascii=False))
    url = Variable.get("gs_webhook_url", default=None)
    if url:
        texto = f":rotating_light: GS coleta falhou: {msg['dag']}.{msg['tarefa']} ({msg['data']}): {msg['erro']}"
        try:
            requests.post(url, json={"content": texto, "text": texto}, timeout=10)
        except requests.RequestException:
            log.exception("Não foi possível enviar o webhook de falha")


def _rodar_coletor(nome_tema: str, fonte: str, **context) -> str:
    ds = _data_coleta(context)
    coletor = TEMAS[nome_tema]["fontes"][fonte]
    caminho = coletor(ds=ds, chegada=_chegada())
    tamanho = caminho.stat().st_size
    log.info("%s gravado em %s (%d bytes)", fonte, caminho, tamanho)
    return str(caminho)  # vai para o XCom e aparece na Grid


def conferir_chegada(nome_tema: str, **context) -> dict:
    """Confere se cada fonte gravou o arquivo do dia e se ele não está vazio."""
    ds = _data_coleta(context)
    chegada = _chegada()
    relatorio, faltando = {}, []
    for fonte in TEMAS[nome_tema]["fontes"]:
        achados = sorted((chegada / fonte).glob(f"{fonte}_{ds}.*"))
        achados = [p for p in achados if not p.name.endswith(".tmp")]
        if achados and achados[0].stat().st_size > 0:
            relatorio[fonte] = {"arquivo": achados[0].name, "bytes": achados[0].stat().st_size}
        else:
            faltando.append(fonte)
    log.info("Chegada %s: %s", ds, json.dumps(relatorio, ensure_ascii=False))
    if faltando:
        raise FileNotFoundError(f"Arquivos de {ds} ausentes ou vazios: {faltando}")
    return relatorio


for nome_tema, cfg in TEMAS.items():
    with DAG(
        dag_id=f"gs_coleta_{nome_tema}",
        description=cfg["descricao"],
        schedule="@daily",
        start_date=pendulum.datetime(2026, 11, 1, tz="America/Sao_Paulo"),
        catchup=True,            # o backfill gera lotes de dias diferentes (requisito 04)
        max_active_runs=1,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=2),
            "on_failure_callback": notificar_falha,
        },
        tags=["gs", "citizentech", nome_tema],
    ) as dag:
        inicio = EmptyOperator(task_id="inicio")
        coletas = {
            fonte: PythonOperator(
                task_id=f"coletar_{fonte}",
                python_callable=_rodar_coletor,
                op_kwargs={"nome_tema": nome_tema, "fonte": fonte},
            )
            for fonte in cfg["fontes"]
        }
        conferir = PythonOperator(
            task_id="conferir_chegada",
            python_callable=conferir_chegada,
            op_kwargs={"nome_tema": nome_tema},
            trigger_rule="all_done",  # roda mesmo se uma coleta falhar, para dizer qual faltou
            retries=0,
        )
        for fonte, tarefa in coletas.items():
            pai = DEPENDENCIAS.get(fonte)
            (coletas[pai] if pai else inicio) >> tarefa
            tarefa >> conferir
    globals()[dag.dag_id] = dag
