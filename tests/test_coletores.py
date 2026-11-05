"""Testes dos coletores sem rede (requests é simulado com monkeypatch)."""
from __future__ import annotations

import json
from datetime import date, timedelta
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))

from coletores import ckan, comum, open_meteo, simulado  # noqa: E402


class RespostaFalsa:
    def __init__(self, corpo=None, conteudo=b"", status=200):
        self._corpo, self._conteudo, self.status_code = corpo, conteudo, status

    def json(self):
        return self._corpo

    def raise_for_status(self):
        if self.status_code >= 400:
            raise comum.requests.HTTPError(response=self)

    def iter_content(self, _):
        yield self._conteudo


# ---------- simulado ----------

LUMS = [{"placa": f"IP{i:07d}", "lat": -23.55 + i * 1e-4, "lon": -46.63, "area": a, "tipo": "LED"}
        for i, a in enumerate(["SE", "PINHEIROS", "CAMPO LIMPO"] * 200)]


def test_simulado_e_deterministico_por_data():
    assert simulado.medicoes_robo("2026-11-03", LUMS) == simulado.medicoes_robo("2026-11-03", LUMS)
    assert simulado.medicoes_robo("2026-11-03", LUMS) != simulado.medicoes_robo("2026-11-04", LUMS)


def test_estado_da_luminaria_e_estavel_e_proporcional():
    estados = [simulado.estado_luminaria(f"IP{i:07d}") for i in range(20000)]
    apagadas = estados.count("apagada") / len(estados)
    assert 0.05 < apagadas < 0.07
    assert simulado.estado_luminaria("IP0723371") == simulado.estado_luminaria("IP0723371")


def test_medicoes_tem_sujeira_e_rodam_de_noite():
    regs = [r for d in range(1, 10) for r in simulado.medicoes_robo(f"2026-11-{d:02d}", LUMS)]
    assert any(r["lux"] < 0 for r in regs)
    assert any(r["lat"] is None for r in regs)
    assert any(r["data_hora"][11:13] == "12" for r in regs), "medição diurna para a regra barrar"
    ids = [r["id_medicao"] for r in regs]
    assert len(ids) > len(set(ids)), "precisa haver reenvios duplicados"


def test_ocorrencias_sem_dado_pessoal_real():
    regs = [r for d in range(1, 41)
            for r in simulado.ocorrencias_app((date(2026, 11, 1) + timedelta(days=d)).isoformat(), LUMS)]
    pessoas = [r["cidadao"] for r in regs if r["cidadao"]]
    assert pessoas and all(p["email"].endswith("@exemplo.com") for p in pessoas)
    assert any(r["categoria"] is None for r in regs)


def test_inventario_le_o_cadastro_mais_recente_ate_a_data(tmp_path):
    pasta = tmp_path / "luminarias"; pasta.mkdir()
    for ds, placa in (("2026-11-01", "A"), ("2026-11-03", "B"), ("2026-11-09", "C")):
        (pasta / f"luminarias_{ds}.json").write_text(json.dumps({"dados": [
            {"cd_placa": placa, "lat": -23.5, "lon": -46.6, "area_piloto": "SE"}]}))
    assert [l["placa"] for l in simulado.inventario(tmp_path, "2026-11-05")] == ["B"]
    assert simulado.inventario(tmp_path / "vazio", "2026-11-05") == []


def test_coletar_simulado_grava_envelope(tmp_path):
    p = simulado.coletar("2026-11-04", tmp_path, "ocorrencias_app")
    assert p == tmp_path / "ocorrencias_app" / "ocorrencias_app_2026-11-04.json"
    envelope = json.loads(p.read_text(encoding="utf-8"))
    assert envelope["_coleta"]["simulado"] is True
    assert len(envelope["dados"]) == envelope["_coleta"]["registros"]


# ---------- geosampa ----------

def test_geosampa_pagina_ate_o_total(tmp_path, monkeypatch):
    from coletores import geosampa
    paginas = []

    def get_falso(url, params=None, timeout=None):
        paginas.append(params["startIndex"])
        ini = params["startIndex"]
        feats = [{"geometry": {"coordinates": [-46.6, -23.5]},
                  "properties": {"cd_identificador_iluminacao_publica": ini + k, "cd_placa": f"P{ini+k}"}}
                 for k in range(min(2, 5 - ini))]
        return RespostaFalsa({"numberMatched": 5, "features": feats})

    monkeypatch.setattr(geosampa, "get_com_retry", get_falso)
    p = geosampa.coletar("2026-11-04", tmp_path, {"SE": (-46.65, -23.56, -46.62, -23.53)}, tamanho_pagina=2)
    dados = json.loads(p.read_text())["dados"]
    assert paginas == [0, 2, 4] and len(dados) == 5
    assert dados[0]["lat"] == -23.5 and dados[0]["area_piloto"] == "SE"


# ---------- open-meteo ----------

def test_open_meteo_um_arquivo_com_todos_os_pontos(tmp_path, monkeypatch):
    chamadas = []

    def get_falso(url, params=None, timeout=None):
        chamadas.append(params)
        return RespostaFalsa({"hourly": {"time": ["2026-11-04T00:00"], "precipitation": [0.4]}})

    monkeypatch.setattr(open_meteo, "get_com_retry", get_falso)
    pontos = [{"nome": "Sé", "lat": -23.5, "lon": -46.6}, {"nome": "Penha", "lat": -23.5, "lon": -46.5}]
    p = open_meteo.coletar("2026-11-04", tmp_path, pontos)
    corpo = json.loads(p.read_text(encoding="utf-8"))
    assert [d["ponto"] for d in corpo["dados"]] == ["Sé", "Penha"]
    assert chamadas[0]["start_date"] == chamadas[0]["end_date"] == "2026-11-04"


# ---------- ckan ----------

RECURSOS = [
    {"id": "a", "name": "Glossário SP156", "format": "PDF", "url": "x.pdf", "last_modified": "2026-01-01"},
    {"id": "b", "name": "Dados SP156 1º semestre 2026", "format": "CSV", "url": "b.csv",
     "last_modified": "2026-08-01T10:00:00"},
    {"id": "c", "name": "Dados SP156 2º semestre 2025", "format": "CSV", "url": "c.csv",
     "last_modified": "2026-02-01T10:00:00"},
]


def test_escolher_recurso_pega_o_csv_mais_recente():
    assert ckan.escolher_recurso(RECURSOS, r"semestre")["id"] == "b"


def test_escolher_recurso_sem_match_explica_o_que_existe():
    with pytest.raises(LookupError, match="Glossário"):
        ckan.escolher_recurso(RECURSOS, r"nao-existe")


def test_filtrar_csv_latin1_ponto_e_virgula_e_sem_acento(tmp_path):
    origem = tmp_path / "bruto.csv"
    origem.write_bytes(
        "Protocolo;Assunto;Serviço\n"
        "1;Iluminação;Troca de lâmpada\n"
        "2;Árvore;Poda\n"
        "3;Pavimentação;TAPA-BURACO\n".encode("latin-1"))
    saida = tmp_path / "saida.csv"
    lidas, gravadas = ckan.filtrar_csv(origem, saida, ["ilumina", "tapa-buraco"])
    assert (lidas, gravadas) == (3, 2)
    texto = saida.read_text(encoding="utf-8")
    assert "Iluminação" in texto and "Poda" not in texto and texto.startswith("Protocolo,")


def test_coletar_ckan_usa_cache_e_grava_meta(tmp_path, monkeypatch):
    csv_bytes = "id;assunto\n1;iluminação\n2;poda\n".encode("latin-1")
    downloads = []

    def get_falso(url, params=None, timeout=None, stream=False):
        if url.endswith("package_show"):
            return RespostaFalsa({"success": True, "result": {"resources": RECURSOS}})
        downloads.append(url)
        return RespostaFalsa(conteudo=csv_bytes)

    monkeypatch.setattr(ckan, "get_com_retry", get_falso)
    kw = dict(fonte="sp156", base_url="https://x", dataset="d", padrao_recurso="semestre",
              palavras=["ilumina"])
    p1 = ckan.coletar("2026-11-03", tmp_path, **kw)
    p2 = ckan.coletar("2026-11-04", tmp_path, **kw)
    assert downloads == ["b.csv"], "o segundo dia deve sair do cache"
    assert p1.name == "sp156_2026-11-03.csv" and p2.exists()
    meta = json.loads((tmp_path / "sp156" / "_meta" / "sp156_2026-11-04.json").read_text())
    assert meta["linhas_gravadas"] == 1


def test_filtro_que_nao_acha_nada_falha_alto(tmp_path, monkeypatch):
    monkeypatch.setattr(ckan, "get_com_retry", lambda url, **k: RespostaFalsa(
        {"success": True, "result": {"resources": RECURSOS}}) if url.endswith("package_show")
        else RespostaFalsa(conteudo=b"id;assunto\n1;poda\n"))
    with pytest.raises(ValueError, match="não encontrou"):
        ckan.coletar("2026-11-04", tmp_path, fonte="sp156", base_url="https://x", dataset="d",
                     padrao_recurso="semestre", palavras=["ilumina"])


# ---------- retry ----------

def test_retry_desiste_rapido_em_404(monkeypatch):
    class S:
        def get(self, *a, **k):
            return RespostaFalsa(status=404)
    with pytest.raises(comum.requests.HTTPError):
        comum.get_com_retry("https://x", sessao=S(), tentativas=3)


def test_retry_insiste_em_503(monkeypatch):
    monkeypatch.setattr(comum.time, "sleep", lambda s: None)
    respostas = iter([RespostaFalsa(status=503), RespostaFalsa(status=503), RespostaFalsa({"ok": 1})])

    class S:
        def get(self, *a, **k):
            return next(respostas)
    assert comum.get_com_retry("https://x", sessao=S(), tentativas=3).json() == {"ok": 1}


def test_ciclo_de_chamados_abre_atende_conclui_e_robo_confirma(tmp_path):
    (tmp_path / "luminarias").mkdir()
    (tmp_path / "luminarias" / "luminarias_2026-11-01.json").write_text(json.dumps({"dados": [
        {"cd_placa": l["placa"], "lat": l["lat"], "lon": l["lon"], "area_piloto": l["area"],
         "dc_tipo_iluminacao_publica": "LED"} for l in LUMS]}))
    status = {}
    for d in range(3, 15):
        ds = f"2026-11-{d:02d}"
        simulado.coletar(ds, tmp_path, "medicoes_robo")
        p = simulado.coletar(ds, tmp_path, "chamados_rpa")
        for c in json.loads(p.read_text())["dados"]:
            assert c["status"] in ("aberto", "em_atendimento", "concluido", "cancelado")
            status[c["id_chamado"]] = c
    abertos_por_placa = {}
    for c in status.values():
        if c["status"] in ("aberto", "em_atendimento"):
            abertos_por_placa.setdefault(c["placa"], []).append(c["id_chamado"])
    assert all(len(v) == 1 for v in abertos_por_placa.values()), "um chamado aberto por placa"
    concluidas = {c["placa"] for c in status.values() if c["status"] == "concluido"}
    assert concluidas
    # depois do reparo o robô mede luz normal na placa
    assert simulado.reparadas_ate(tmp_path, "2026-11-15") == concluidas
