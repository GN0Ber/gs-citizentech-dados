"""Fontes do componente Dados (tema: iluminação pública e manutenção).

Cada fonte vira uma tarefa no DAG gs_coleta_iluminacao. Fontes listadas em
DEPENDENCIAS rodam depois daquela de que dependem; as demais rodam em paralelo.
Tudo aqui foi conferido contra os portais em 05/11/2026 (ver docs/fontes.md).
"""
from __future__ import annotations

from functools import partial

from . import ckan, geosampa, open_meteo, simulado

SP_DADOS = "https://dados.prefeitura.sp.gov.br"

AREAS_PILOTO = simulado.AREAS_PILOTO

# Ponto central de cada área para o clima (nascer/pôr do sol, nuvens, chuva)
PONTOS_CLIMA = [
    {"nome": a, "lat": round((b[1] + b[3]) / 2, 4), "lon": round((b[0] + b[2]) / 2, 4)}
    for a, b in AREAS_PILOTO.items()
]

# Serviços do SP156 ligados à rede de iluminação. Atenção: o pedido de "lâmpada
# apagada" NÃO aparece nos dados abertos do SP156 (1 linha em 463 mil no 2º TRI 2026),
# porque segue pelo canal da ILUME. Isso é parte do problema que o produto resolve.
SERVICOS_SP156 = ["poste em mau estado", "fiação solta"]

TEMAS = {
    "iluminacao": {
        "descricao": "Histórico e análise das condições de iluminação pública",
        "fontes": {
            # Externa real: cadastro ILUME de pontos de iluminação (GeoSampa WFS)
            "luminarias": partial(geosampa.coletar, areas=AREAS_PILOTO, fonte="luminarias"),
            # Externa real: pedidos do SP156 sobre postes e fiação (trimestral)
            "sp156": partial(
                ckan.coletar, fonte="sp156", base_url=SP_DADOS, dataset="dados-do-sp156",
                padrao_recurso=r"^\s*Dados do SP156", palavras=SERVICOS_SP156,
                colunas_filtro=["Serviço"],
            ),
            # Externa real: nascer/pôr do sol, nebulosidade e chuva por área-piloto
            "clima": partial(open_meteo.coletar, pontos=PONTOS_CLIMA, fonte="clima"),
            # Própria: medições LDR do robô de inspeção (Maker Lab)
            "medicoes_robo": partial(simulado.coletar, fonte="medicoes_robo"),
            # Própria: evidências do app classificadas pela visão (Computer Vision)
            "ocorrencias_app": partial(simulado.coletar, fonte="ocorrencias_app"),
            # Própria: chamados abertos e fechados pelo robô de RPA (front do Caíque)
            "chamados_rpa": partial(simulado.coletar, fonte="chamados_rpa"),
        },
    },
}

# As fontes próprias se ancoram no cadastro real, então esperam a coleta dele
DEPENDENCIAS = {"medicoes_robo": "luminarias", "ocorrencias_app": "luminarias",
                # o RPA abre chamado a partir da medição da noite
                "chamados_rpa": "medicoes_robo"}
