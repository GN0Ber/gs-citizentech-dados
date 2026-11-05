"""Cadastro de pontos de iluminação pública (ILUME) via WFS do GeoSampa.

Camada geoportal:iluminacao_publica: ~663 mil pontos na cidade (placa, tipo de
lâmpada, endereço, coordenada). Para o MVP, coletamos só a área-piloto (uma caixa
por subprefeitura do piloto), paginando de 10 mil em 10 mil.
"""
from __future__ import annotations

from pathlib import Path

from .comum import destino, get_com_retry, gravar_json, metadados_coleta

WFS_URL = "https://wfs.geosampa.prefeitura.sp.gov.br/geoserver/geoportal/wfs"
CAMADA = "geoportal:iluminacao_publica"


def _pagina(bbox: tuple, inicio: int, tamanho: int) -> dict:
    lon0, lat0, lon1, lat1 = bbox
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": CAMADA, "outputFormat": "application/json", "srsName": "EPSG:4326",
        "count": tamanho, "startIndex": inicio,
        "sortBy": "cd_identificador_iluminacao_publica",  # paginação estável exige ordem
        # CQL com EPSG:4326 recebe lon,lat (o contrário do BBOX do WFS 2.0)
        "CQL_FILTER": f"BBOX(ge_ponto,{lon0},{lat0},{lon1},{lat1},'EPSG:4326')",
    }
    return get_com_retry(WFS_URL, params=params, timeout=180).json()


def coletar(ds: str, chegada: Path, areas: dict[str, tuple], fonte: str = "luminarias",
            tamanho_pagina: int = 10_000) -> Path:
    pontos = []
    por_area = {}
    for area, bbox in areas.items():
        inicio, total = 0, None
        while total is None or inicio < total:
            corpo = _pagina(bbox, inicio, tamanho_pagina)
            total = corpo.get("numberMatched") or corpo.get("totalFeatures") or 0
            feats = corpo.get("features", [])
            if not feats:
                break
            for f in feats:
                lon, lat = (f.get("geometry") or {}).get("coordinates") or (None, None)
                pontos.append({**f["properties"], "lon": lon, "lat": lat, "area_piloto": area})
            inicio += len(feats)
        por_area[area] = inicio
    if not pontos:
        raise ValueError("WFS do GeoSampa não devolveu pontos para a área-piloto")
    return gravar_json(destino(chegada, fonte, ds, "json"), pontos,
                       metadados_coleta(fonte, ds, WFS_URL, camada=CAMADA,
                                        pontos_por_area=por_area, registros=len(pontos)))
