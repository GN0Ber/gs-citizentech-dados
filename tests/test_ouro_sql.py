"""As 5 consultas e o modelo da Ouro rodam e devolvem dados sobre as amostras."""
from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

import sys  # noqa: E402
sys.path.insert(0, str(Path(__file__).parent))
import ouro_local  # noqa: E402


@pytest.fixture(scope="module")
def resultado():
    spark = (SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.session.timeZone", "America/Sao_Paulo").getOrCreate())
    raiz = Path(__file__).resolve().parents[1]
    return spark, ouro_local.rodar(spark, raiz / "amostras")


def test_cinco_consultas_retornam_linhas(resultado):
    _, (_, consultas) = resultado
    assert len(consultas) == 6
    for i, df in enumerate(consultas, 1):
        assert df.count() > 0, f"Q{i} veio vazia"


def test_estrela_e_lgpd(resultado):
    spark, _ = resultado
    fato = spark.table("fato_ocorrencia")
    assert not {"logradouro", "numero", "email", "nome", "telefone"} & set(fato.columns)
    assert fato.where("lat_aprox <> round(lat_aprox, 3)").count() == 0
    for v in ("vw_mvp_mapa_luminarias", "vw_mvp_fila_manutencao", "vw_mvp_painel_subprefeitura"):
        assert "cidadao_hash" not in spark.table(v).columns
    orfas = spark.sql("""SELECT count(*) FROM fato_inspecao f LEFT ANTI JOIN dim_luminaria d
                         ON d.id_luminaria = f.id_luminaria""").first()[0]
    assert orfas == 0, "toda inspeção aponta para uma luminária do cadastro"


def test_fila_de_manutencao_priorizada(resultado):
    spark, _ = resultado
    fila = spark.table("vw_mvp_fila_manutencao")
    assert fila.where("prioridade < 0 or prioridade > 100").count() == 0
    assert fila.where("situacao = 'apagada'").count() > 0


def test_ciclo_dos_chamados(resultado):
    spark, _ = resultado
    fato = spark.table("fato_chamado")
    ids = fato.select("id_chamado").distinct().count()
    assert ids == fato.count(), "um registro por chamado, no último estado"
    assert fato.where("status = 'concluido'").count() > 0
    assert fato.where("status = 'concluido' and horas_ate_conclusao < 0").count() == 0
    # o robô volta à área a cada 3 noites: parte dos reparos é confirmada em campo
    assert fato.where("reparo_confirmado_pelo_robo").count() > 0
    fila = spark.table("vw_mvp_fila_manutencao")
    assert {"chamado_aberto", "status_chamado"} <= set(fila.columns)
    painel = spark.table("vw_mvp_painel_subprefeitura")
    assert "mediana_horas_ate_reparo" in painel.columns
