"""Utilidades compartilhadas pelos coletores.

Nada aqui depende do Airflow, para que os coletores possam ser testados com pytest
e executados à mão (python -m coletores.cli) sem subir o scheduler.
"""
from __future__ import annotations

import json
import os
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

# Pasta de pouso. Pode ser trocada pela variável de ambiente GS_CHEGADA
# (o DAG também aceita a Airflow Variable gs_chegada, que tem prioridade).
CHEGADA_PADRAO = Path(os.environ.get("GS_CHEGADA", Path.home() / "gs_chegada"))

USER_AGENT = "gs-citizentech-fiap/1.0 (projeto academico)"


def sessao_http() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = USER_AGENT
    return s


def get_com_retry(url: str, *, params: dict | None = None, tentativas: int = 4,
                  timeout: int = 60, sessao: requests.Session | None = None,
                  stream: bool = False) -> requests.Response:
    """GET com backoff exponencial.

    O portal da ANEEL tem um WAF que derruba conexões de forma intermitente; sem
    retry com espera razoável, uma oscilação de 15 s derruba a coleta do dia.
    """
    s = sessao or sessao_http()
    ultimo_erro: Exception | None = None
    for i in range(tentativas):
        try:
            r = s.get(url, params=params, timeout=timeout, stream=stream)
            if r.status_code >= 500 or r.status_code == 429:
                raise requests.HTTPError(f"HTTP {r.status_code} em {url}", response=r)
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            resp = getattr(e, "response", None)
            if resp is not None and 400 <= resp.status_code < 500 and resp.status_code != 429:
                raise  # erro do cliente: tentar de novo não resolve
            ultimo_erro = e
            if i < tentativas - 1:
                time.sleep(min(2 ** i * 2, 20))
    raise RuntimeError(f"Falha após {tentativas} tentativas: {url}") from ultimo_erro


def destino(chegada: Path, fonte: str, ds: str, ext: str) -> Path:
    """chegada/<fonte>/<fonte>_<AAAA-MM-DD>.<ext> (um arquivo por coleta, data no nome)."""
    pasta = Path(chegada) / fonte
    pasta.mkdir(parents=True, exist_ok=True)
    return pasta / f"{fonte}_{ds}.{ext}"


def gravar_json(caminho: Path, conteudo: Any, metadados: dict) -> Path:
    """Grava JSON em uma linha (JSON Lines com 1 registro) para o Auto Loader ler sem multiLine."""
    envelope = {"_coleta": metadados, "dados": conteudo}
    tmp = caminho.with_suffix(caminho.suffix + ".tmp")
    tmp.write_text(json.dumps(envelope, ensure_ascii=False), encoding="utf-8")
    tmp.replace(caminho)  # escrita atômica: o arquivo nunca aparece pela metade
    return caminho


def metadados_coleta(fonte: str, ds: str, url: str, **extra: Any) -> dict:
    return {"fonte": fonte, "data_coleta": ds, "url": url,
            "coletado_em": datetime.now().isoformat(timespec="seconds"), **extra}


def ds_valida(ds: str) -> str:
    date.fromisoformat(ds)  # levanta ValueError se vier lixo
    return ds
