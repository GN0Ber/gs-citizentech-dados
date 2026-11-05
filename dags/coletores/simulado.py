"""Dados próprios do produto, simulados até o robô e a visão estarem integrados.

O briefing aceita dados simulados "com geração justificada e documentada"; a
justificativa e os parâmetros estão em docs/dados_simulados.md. Quando o robô do
Maker Lab e o modelo de visão estiverem enviando dados de verdade, eles gravam no
mesmo formato em chegada/medicoes_robo e chegada/ocorrencias_app, e nada muda
daí para frente no pipeline.

Regras da geração:
* Ancorada no cadastro REAL da ILUME: o robô "passa" por luminárias que existem
  (o arquivo de luminárias mais recente na pasta de chegada), com ruído de GPS.
* Determinística por data: a mesma `ds` gera o mesmo arquivo (seed = data).
* Estado de cada luminária é estável entre os dias (uma lâmpada queimada continua
  queimada): ~6% apagadas e ~10% com iluminância insuficiente, sorteadas pela placa.
* Sem dado pessoal real: nomes fictícios e e-mails no domínio reservado exemplo.com.
* ~8% de sujeira proposital para as regras de qualidade da Prata barrarem algo real.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

from .comum import destino, gravar_json, metadados_coleta

NOMES = ["Ana", "Bruno", "Carla", "Diego", "Elisa", "Fábio", "Gabi", "Hugo", "Iara",
         "João", "Karen", "Lucas", "Marta", "Nilo", "Olga", "Paulo", "Rita", "Saulo"]
SOBRENOMES = ["Silva", "Souza", "Oliveira", "Santos", "Lima", "Pereira", "Costa", "Ramos"]

# Áreas-piloto: caixas (lon_min, lat_min, lon_max, lat_max), nomes iguais ao SP156
AREAS_PILOTO = {
    "SE": (-46.650, -23.565, -46.620, -23.535),
    "PINHEIROS": (-46.710, -23.590, -46.670, -23.555),
    "CAMPO LIMPO": (-46.790, -23.660, -46.740, -23.610),
}

CATEGORIAS = ["luminaria_apagada", "luminaria_apagada", "luminaria_piscando",
              "luminaria_acesa_de_dia", "poste_danificado", "fiacao_exposta"]


def _rng(ds: str, fonte: str) -> random.Random:
    return random.Random(int(hashlib.sha256(f"{fonte}:{ds}".encode()).hexdigest()[:12], 16))


def _sorteio_estavel(chave: str) -> float:
    return int(hashlib.md5(chave.encode()).hexdigest()[:8], 16) / 0xFFFFFFFF


def estado_luminaria(placa: str) -> str:
    """Estado "verdadeiro" (oculto) da luminária, igual em todos os dias."""
    s = _sorteio_estavel("estado:" + placa)
    return "apagada" if s < 0.06 else "insuficiente" if s < 0.16 else "ok"


def inventario(chegada: Path, ds: str) -> list[dict]:
    """Luminárias do arquivo de cadastro mais recente com data <= ds (ou [] se não houver)."""
    pasta = Path(chegada) / "luminarias"
    arquivos = sorted(p for p in pasta.glob("luminarias_*.json") if p.stem[-10:] <= ds)
    if not arquivos:
        return []
    dados = json.loads(arquivos[-1].read_text(encoding="utf-8"))["dados"]
    vistos, saida = set(), []
    for d in dados:  # o cadastro tem pontos repetidos; para simular basta um por placa
        placa = d.get("cd_placa")
        if placa and placa not in vistos and d.get("lat") is not None:
            vistos.add(placa)
            saida.append({"placa": placa, "lat": d["lat"], "lon": d["lon"],
                          "area": d.get("area_piloto"), "tipo": d.get("dc_tipo_iluminacao_publica")})
    return saida


ZONA_TESTE = 400  # luminárias por área-piloto onde o robô faz ronda e o app é divulgado


def zona_teste(lums: list[dict], n: int = ZONA_TESTE) -> list[dict]:
    """As n luminárias mais próximas do centro de cada área-piloto.

    Um piloto real começa por alguns quarteirões, não pelos 16 mil pontos da subprefeitura:
    o robô consegue voltar aos mesmos postes (e confirmar reparos) e as fotos dos cidadãos
    caem onde o robô também mede (o que permite medir a precisão da visão).
    """
    zona = []
    for area, (lon0, lat0, lon1, lat1) in AREAS_PILOTO.items():
        cy, cx = (lat0 + lat1) / 2, (lon0 + lon1) / 2
        da_area = [l for l in lums if l["area"] == area]
        da_area.sort(key=lambda l: ((l["lat"] - cy) ** 2 + (l["lon"] - cx) ** 2, l["placa"]))
        zona.extend(da_area[:n])
    return zona


def _ruido_gps(rng: random.Random, lat: float, lon: float, metros: float = 6.0):
    dlat = rng.gauss(0, metros) / 111_320
    dlon = rng.gauss(0, metros) / (111_320 * math.cos(math.radians(lat)))
    return round(lat + dlat, 7), round(lon + dlon, 7)


def _ponto_aleatorio(rng: random.Random, area: str):
    lon0, lat0, lon1, lat1 = AREAS_PILOTO[area]
    return round(rng.uniform(lat0, lat1), 7), round(rng.uniform(lon0, lon1), 7), None, None


def _lux(rng: random.Random, estado: str, tipo: str | None) -> float:
    if estado == "apagada":
        return round(rng.uniform(0.1, 1.0), 2)
    if estado == "insuficiente":
        return round(rng.uniform(1.5, 4.5), 2)
    base = (15, 40) if (tipo or "").upper() == "LED" else (8, 25)
    return round(rng.uniform(*base), 2)


def medicoes_robo(ds: str, lums: list[dict], n_min: int = 150, n_max: int = 260,
                  reparadas: set[str] | None = None) -> list[dict]:
    """Rota noturna do robô: um dia por área-piloto, em rodízio.

    `reparadas` = placas com chamado concluído antes desta noite: o robô passa a
    medir luz normal ali, o que permite ao pipeline confirmar o reparo em campo.
    """
    reparadas = reparadas or set()
    rng = _rng(ds, "medicoes_robo")
    dia = date.fromisoformat(ds)
    areas = list(AREAS_PILOTO)
    area = areas[dia.toordinal() % len(areas)]
    # rota em ordem geográfica dentro da zona de teste da área
    candidatos = sorted((l for l in zona_teste(lums) if l["area"] == area),
                        key=lambda l: (round(l["lat"], 3), l["lon"]))
    n = rng.randint(n_min, n_max)
    if candidatos:
        ini = rng.randrange(max(1, len(candidatos) - n))
        rota = candidatos[ini:ini + n]
    else:  # primeiro dia, sem cadastro ainda: pontos soltos (a Prata barra por distância)
        rota = [dict(zip(("lat", "lon", "placa", "tipo"), _ponto_aleatorio(rng, area)))
                for _ in range(n)]
    inicio = datetime.combine(dia, datetime.min.time()) + timedelta(hours=19)
    regs = []
    for i, lum in enumerate(rota):
        estado = estado_luminaria(lum["placa"]) if lum.get("placa") else "ok"
        if lum.get("placa") in reparadas:
            estado = "ok"
        lat, lon = _ruido_gps(rng, lum["lat"], lum["lon"])
        reg = {
            "id_medicao": f"M{ds.replace('-', '')}{i:05d}",
            "robo_id": "ROBO-01",
            "data_hora": (inicio + timedelta(seconds=40 * i)).isoformat(),
            "lat": lat, "lon": lon,
            "lux": _lux(rng, estado, lum.get("tipo")),
            "sensor": "LDR-GL5528",
            "bateria_pct": max(5, 100 - i // 3),
            "area_piloto": area,
            # o robô lê a etiqueta com a placa do ponto; ~5% das vezes não consegue ler
            "placa": lum.get("placa") if rng.random() > 0.05 else None,
        }
        s = rng.random()
        if s < 0.02:
            reg["lux"] = -1.0                                         # leitura impossível
        elif s < 0.04:
            reg["lat"] = reg["lon"] = None                            # GPS sem fix
        elif s < 0.06:
            reg["data_hora"] = (inicio - timedelta(hours=7)).isoformat()  # medição de dia
        regs.append(reg)
        if rng.random() < 0.03:
            regs.append(dict(reg))                                    # reenvio duplicado
    return regs


def _pessoa(rng: random.Random, i: int) -> dict:
    nome = f"{rng.choice(NOMES)} {rng.choice(SOBRENOMES)}"
    usuario = nome.lower().replace(" ", ".").encode("ascii", "ignore").decode()
    return {"nome": nome, "email": f"{usuario}{i}@exemplo.com",
            "telefone": f"+55 11 9{rng.randint(1000, 9999)}-{rng.randint(1000, 9999)}"}


def ocorrencias_app(ds: str, lums: list[dict], n_min: int = 8, n_max: int = 18,
                    reparadas: set[str] | None = None) -> list[dict]:
    """Evidências do app: foto do cidadão ou da câmera, já classificada pela visão."""
    rng = _rng(ds, "ocorrencias_app")
    dia = date.fromisoformat(ds)
    lums = zona_teste(lums)  # o app é divulgado na mesma zona onde o robô faz ronda
    reparadas = reparadas or set()
    com_falha = [l for l in lums
                 if estado_luminaria(l["placa"]) != "ok" and l["placa"] not in reparadas]
    regs = []
    for i in range(rng.randint(n_min, n_max)):
        # 70% das fotos são de luminárias que de fato falham; 30% são alarmes falsos
        fonte = com_falha if (com_falha and rng.random() < 0.7) else lums
        if fonte:
            lum = rng.choice(fonte)
            lat, lon = _ruido_gps(rng, lum["lat"], lum["lon"], 10)
            area = lum["area"]
        else:
            area = rng.choice(list(AREAS_PILOTO))
            lat, lon, _, _ = _ponto_aleatorio(rng, area)
        # Um modelo de visão razoável: quando a falha é real, acerta a classe e tem confiança
        # mais alta; alarme falso (luminária ok, sombra, reflexo) sai com confiança mais baixa.
        # Assim a consulta Q5 mostra a precisão subindo com a confiança, como num modelo real.
        falha_real = (bool(fonte) and estado_luminaria(lum["placa"]) != "ok"
                      and lum["placa"] not in reparadas)
        if falha_real:
            categoria = rng.choice(["luminaria_apagada"] * 3 + ["luminaria_piscando"])
            confianca = round(rng.uniform(0.65, 0.99), 3)
        else:
            categoria = rng.choice(CATEGORIAS)
            confianca = round(rng.uniform(0.45, 0.85), 3)
        origem = rng.choice(["app_foto_cidadao", "app_foto_cidadao", "camera_visao"])
        quando = datetime.combine(dia, datetime.min.time()) + timedelta(
            minutes=rng.randint(18 * 60, 24 * 60 - 1))
        reg = {
            "id_ocorrencia": f"O{ds.replace('-', '')}{i:04d}",
            "data_hora": quando.isoformat(),
            "categoria": categoria,
            "origem": origem,
            "confianca_visao": confianca,
            "modelo_visao": "yolo-luminaria-v1",
            "foto_id": hashlib.sha1(f"{ds}{i}".encode()).hexdigest()[:16],
            "lat": lat, "lon": lon, "area_piloto": area,
            # a placa ILUME pintada no poste, lida na foto; o cidadão nem sempre enquadra
            "placa": (lum["placa"] if fonte and rng.random() < 0.85 else None),
            "cidadao": _pessoa(rng, i) if origem == "app_foto_cidadao" else None,
        }
        s = rng.random()
        if s < 0.02:
            reg["categoria"] = None                                   # categoria obrigatória
        elif s < 0.04:
            reg["lat"], reg["lon"] = -22.9, -43.2                     # fora de SP (é o Rio)
        elif s < 0.06:
            reg["confianca_visao"] = 1.7                              # confiança inválida
        regs.append(reg)
        if rng.random() < 0.03:
            regs.append(dict(reg))
    return regs


# ---------------------------------------------------------------- chamados do RPA

EQUIPES = {"SE": "EQUIPE-CENTRO", "PINHEIROS": "EQUIPE-OESTE", "CAMPO LIMPO": "EQUIPE-SUL"}


def _chamados_anteriores(chegada: Path, ds: str) -> dict[str, dict]:
    """Último estado conhecido de cada chamado, lendo os arquivos de dias anteriores."""
    estado: dict[str, dict] = {}
    for arq in sorted((Path(chegada) / "chamados_rpa").glob("chamados_rpa_*.json")):
        if arq.stem[-10:] >= ds:
            continue
        for c in json.loads(arq.read_text(encoding="utf-8"))["dados"]:
            estado[c["id_chamado"]] = c
    return estado


def reparadas_ate(chegada: Path, ds: str) -> set[str]:
    return {c["placa"] for c in _chamados_anteriores(chegada, ds).values()
            if c.get("status") == "concluido"}


def chamados_rpa(ds: str, chegada: Path, corte_prioridade: int = 30) -> list[dict]:
    """O que o robô de RPA grava por dia: chamados abertos hoje + os que mudaram hoje.

    Abre chamado para cada luminária medida hoje abaixo de 5 lux (prioridade >= corte,
    mesma regra da vw_mvp_fila_manutencao), um por placa enquanto estiver aberto.
    O prazo de reparo é sorteado por placa (1 a 10 dias) e o fechamento sai no dia
    em que vence. ~3% dos chamados são cancelados (endereço não localizado).
    """
    dia = date.fromisoformat(ds)
    anteriores = _chamados_anteriores(chegada, ds)
    abertos_por_placa = {c["placa"]: c for c in anteriores.values()
                         if c["status"] in ("aberto", "em_atendimento")}
    saida: list[dict] = []

    # 1. andamento dos chamados que já existiam
    for c in abertos_por_placa.values():
        prazo = 1 + int(_sorteio_estavel("prazo:" + c["id_chamado"]) * 10)
        aberto = date.fromisoformat(c["aberto_em"][:10])
        novo = dict(c)
        if _sorteio_estavel("cancela:" + c["id_chamado"]) < 0.03:
            novo.update(status="cancelado", atualizado_em=f"{ds}T09:00:00",
                        motivo="endereço não localizado pela equipe")
        elif dia >= aberto + timedelta(days=prazo):
            novo.update(status="concluido", atualizado_em=f"{ds}T16:30:00",
                        concluido_em=f"{ds}T16:30:00")
        elif c["status"] == "aberto" and dia >= aberto + timedelta(days=1):
            novo.update(status="em_atendimento", atualizado_em=f"{ds}T08:00:00")
        else:
            continue
        saida.append(novo)

    # 2. novos chamados a partir das medições desta noite
    arq = Path(chegada) / "medicoes_robo" / f"medicoes_robo_{ds}.json"
    if arq.exists():
        vistos = set(abertos_por_placa)
        lums = {l["placa"]: l for l in inventario(chegada, ds)}
        for m in json.loads(arq.read_text(encoding="utf-8"))["dados"]:
            if m.get("lat") is None or m.get("lux") is None or not (0 <= m["lux"] < 5):
                continue
            # a placa vem do casamento com o cadastro, como a fila faz
            perto = min(lums.values(), default=None,
                        key=lambda l: (l["lat"] - m["lat"]) ** 2 + (l["lon"] - m["lon"]) ** 2)
            if not perto or perto["placa"] in vistos:
                continue
            prioridade = 60 if m["lux"] < 1 else 30
            if prioridade < corte_prioridade:
                continue
            vistos.add(perto["placa"])
            saida.append({
                "id_chamado": f"CH-{ds.replace('-', '')}-{perto['placa']}",
                "placa": perto["placa"],
                "area_piloto": m["area_piloto"],
                "prioridade": prioridade,
                "situacao_detectada": "apagada" if m["lux"] < 1 else "insuficiente",
                "status": "aberto",
                "aberto_em": f"{ds}T07:00:00",
                "atualizado_em": f"{ds}T07:00:00",
                "concluido_em": None,
                "equipe": EQUIPES.get(m["area_piloto"], "EQUIPE-GERAL"),
                "origem": "rpa",
            })
    return saida


GERADORES = {"medicoes_robo": medicoes_robo, "ocorrencias_app": ocorrencias_app}


def coletar(ds: str, chegada: Path, fonte: str) -> Path:
    lums = inventario(chegada, ds)
    if fonte == "chamados_rpa":
        dados = chamados_rpa(ds, chegada)
    elif fonte == "medicoes_robo":
        dados = medicoes_robo(ds, lums, reparadas=reparadas_ate(chegada, ds))
    elif fonte == "ocorrencias_app":
        dados = ocorrencias_app(ds, lums, reparadas=reparadas_ate(chegada, ds))
    else:
        dados = GERADORES[fonte](ds, lums)
    return gravar_json(destino(chegada, fonte, ds, "json"), dados,
                       metadados_coleta(fonte, ds, "simulado://" + fonte, registros=len(dados),
                                        simulado=True, luminarias_base=len(lums)))
