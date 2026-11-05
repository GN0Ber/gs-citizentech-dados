"""Testa as transformações da Prata com PySpark local (sem Delta).

Usa amostras em amostras/ (recortes dos arquivos reais coletados) e emula a Bronze
como o Auto Loader faz: JSON com todos os campos como texto e CSV como texto.
Rodar: pytest tests/test_prata_spark.py  (precisa de pyspark e Java)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, functions as F  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "databricks"))
AMOSTRAS = RAIZ / "amostras"

from gs_lib import iluminacao as il  # noqa: E402
from gs_lib.comum import aplicar_regras, nome_coluna  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("gs-testes")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "America/Sao_Paulo").getOrCreate())
    yield s
    s.stop()


def bronze_json(spark, fonte):
    return (spark.read.option("primitivesAsString", "true").json(str(AMOSTRAS / fonte))
            .withColumn("arquivo_origem", F.input_file_name())
            .withColumn("carregado_em", F.current_timestamp()))


def bronze_csv(spark, fonte):
    df = spark.read.option("header", "true").csv(str(AMOSTRAS / fonte))
    df = df.select([F.col(f"`{c}`").alias(nome_coluna(c)) for c in df.columns])
    return (df.withColumn("arquivo_origem", F.input_file_name())
              .withColumn("carregado_em", F.current_timestamp()))


@pytest.fixture(scope="module")
def luminarias(spark):
    df = il.luminarias_para_prata(bronze_json(spark, "luminarias"))
    ok, _, _ = aplicar_regras(df, il.regras_luminarias())
    return ok.cache()


def test_nome_coluna():
    assert nome_coluna("Data de Finalização") == "data_de_finalizacao"
    assert nome_coluna("Serviço") == "servico"


def test_luminarias_dedup_por_id_entre_lotes(spark, luminarias):
    bruto = bronze_json(spark, "luminarias").select(F.explode("dados")).count()
    ids = luminarias.select("id_luminaria").distinct().count()
    assert ids == luminarias.count(), "uma linha por id_luminaria"
    assert bruto > ids, "a amostra tem dois lotes com os mesmos pontos"


def test_medicoes_casam_com_luminaria_e_regras_barram_sujeira(spark, luminarias):
    df = il.medicoes_para_prata(bronze_json(spark, "medicoes_robo"), luminarias)
    ok, barrado, cont = aplicar_regras(df, il.regras_medicoes())
    regras = {n: b for n, _, _, b in cont}
    assert regras["lux_valido"] > 0 and regras["medicao_noturna"] > 0 and regras["coordenada_em_sp"] > 0
    # sem a sujeira, quase toda medição cai a até 30 m de uma luminária real
    casadas = ok.count() / max(1, df.filter(F.col("lux") >= 0).count())
    assert casadas > 0.8, casadas
    assert ok.filter(F.col("distancia_luminaria_m") > il.RAIO_CASAMENTO_M).count() == 0
    sit = {r["situacao"] for r in ok.select("situacao").distinct().collect()}
    assert {"ok", "apagada", "insuficiente"} <= sit
    assert barrado.count() == df.count() - ok.count()
    assert df.select("id_medicao").distinct().count() == df.count(), "duplicados removidos"


def test_ocorrencias_lgpd_e_regras(spark, luminarias):
    df = il.ocorrencias_para_prata(bronze_json(spark, "ocorrencias_app"), luminarias, "sal-teste")
    assert "cidadao" not in df.columns and "nome" not in df.columns
    hashes = [r[0] for r in df.select("cidadao_hash").where("cidadao_hash is not null").limit(5).collect()]
    assert hashes and all(len(h) == 64 for h in hashes)
    ok, _, cont = aplicar_regras(df, il.regras_ocorrencias())
    assert dict((n, b) for n, _, _, b in cont)["categoria_obrigatoria"] > 0
    assert ok.where("id_luminaria is not null").count() > 0


def test_sp156_formatos_de_data_misturados(spark):
    df = il.sp156_para_prata(bronze_csv(spark, "sp156"))
    formatos = {r[0] for r in df.select("formato_data").distinct().collect()}
    assert formatos == {"dd/MM/yyyy", "MM/dd/yyyy"}, formatos
    # todas as aberturas caem nos trimestres publicados (abr-set/2026), nenhuma trocada
    fora = df.where("data_abertura < '2026-04-01' or data_abertura > '2026-09-30'").count()
    assert fora == 0
    assert df.where("data_abertura is null").count() == 0
    assert df.select("id_pedido").distinct().count() == df.count()
    ok, _, _ = aplicar_regras(df, il.regras_sp156())
    assert ok.where("dias_atendimento < 0").count() == 0


def test_clima_explode_dias(spark):
    df = il.clima_para_prata(bronze_json(spark, "clima"))
    linhas = df.collect()
    assert len(linhas) == 3  # 3 áreas x 1 dia
    assert all(10 < r["horas_escuridao"] < 14 for r in linhas)
    ok, _, _ = aplicar_regras(df, il.regras_clima())
    assert ok.count() == 3


def test_medicao_sem_gps_casa_pela_placa(spark, luminarias):
    placa = luminarias.select("placa").first()[0]
    linha = ('{"_coleta": {"fonte": "medicoes_robo", "data_coleta": "2026-11-10"}, "dados": ['
             '{"id_medicao": "MX1", "robo_id": "ROBO-01", "data_hora": "2026-11-10T20:00:00", '
             '"lat": null, "lon": null, "lux": 0.3, "situacao_robo": "apagada", '
             '"area_piloto": "SE", "placa": "' + placa + '"}]}')
    bronze = (spark.read.option("primitivesAsString", "true").json(spark.sparkContext.parallelize([linha]))
              .withColumn("arquivo_origem", F.lit("teste")).withColumn("carregado_em", F.current_timestamp()))
    df = il.medicoes_para_prata(bronze, luminarias)
    r = df.first()
    assert r["id_luminaria"] is not None and r["lat"] is not None and r["situacao"] == "apagada"
    ok, _, _ = aplicar_regras(df, il.regras_medicoes())
    assert ok.count() == 1


def test_chamados_ultimo_estado_e_regras(spark, luminarias):
    df = il.chamados_para_prata(bronze_json(spark, "chamados_rpa"), luminarias)
    assert df.select("id_chamado").distinct().count() == df.count()
    ok, _, cont = aplicar_regras(df, il.regras_chamados())
    assert dict((n, b) for n, _, _, b in cont)["status_conhecido"] == 0
    assert ok.where("status = 'concluido' and concluido_em is null").count() == 0
