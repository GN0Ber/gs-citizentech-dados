"""Clima diário e horário do Open-Meteo (sem chave).

Por que o endpoint /forecast e não o /archive do exemplo do professor: o arquivo
histórico (ERA5) chega com ~5 dias de atraso, então a coleta de "ontem" volta com
nulos. O /forecast aceita start_date/end_date nos últimos ~90 dias e devolve o dado
observado/analisado do dia. Para backfill mais antigo que isso, use ARCHIVE_URL.
"""
from __future__ import annotations

from pathlib import Path

from .comum import destino, get_com_retry, gravar_json, metadados_coleta

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Para iluminação importa saber quando escurece e se o céu está fechado:
# a medição do robô só vale à noite, e chuva/nuvem mudam a leitura do LDR.
HORARIO = "cloud_cover,precipitation,temperature_2m"
DIARIO = ("sunrise,sunset,daylight_duration,precipitation_sum,"
          "temperature_2m_max,temperature_2m_min")


def coletar(ds: str, chegada: Path, pontos: list[dict], fonte: str = "clima",
            url: str = FORECAST_URL) -> Path:
    respostas = []
    for p in pontos:
        r = get_com_retry(url, params={
            "latitude": p["lat"], "longitude": p["lon"],
            "start_date": ds, "end_date": ds,
            "hourly": HORARIO, "daily": DIARIO,
            "timezone": "America/Sao_Paulo",
        }, timeout=30)
        corpo = r.json()
        corpo["ponto"] = p["nome"]  # o Open-Meteo não devolve o nome do lugar
        respostas.append(corpo)
    return gravar_json(destino(chegada, fonte, ds, "json"), respostas,
                       metadados_coleta(fonte, ds, url, pontos=len(pontos)))
