"""Roda 03_ouro.sql e 04_consultas.sql no Spark local, sobre a Prata montada a partir
de uma pasta de chegada. Usado pelos testes e para ensaiar os números do relatório.

    python tests/ouro_local.py amostras   # imprime as 5 consultas
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "databricks"))
from gs_lib import iluminacao as il  # noqa: E402
from gs_lib.comum import aplicar_regras, nome_coluna  # noqa: E402


def _json(spark, pasta):
    return (spark.read.option("primitivesAsString", "true").json(str(pasta))
            .withColumn("arquivo_origem", F.input_file_name())
            .withColumn("carregado_em", F.current_timestamp()))


def _csv(spark, pasta):
    df = spark.read.option("header", "true").csv(str(pasta))
    df = df.select([F.col(f"`{c}`").alias(nome_coluna(c)) for c in df.columns])
    return df.withColumn("arquivo_origem", F.input_file_name()).withColumn("carregado_em", F.current_timestamp())


def montar_prata(spark, chegada: Path) -> dict:
    barrados = {}
    def ok(nome, df, regras):
        v, _, cont = aplicar_regras(df, regras)
        v.cache().createOrReplaceTempView(f"prata_{nome}")
        barrados[nome] = {n: b for n, _, _, b in cont}
        return v
    lums = ok("luminarias", il.luminarias_para_prata(_json(spark, chegada / "luminarias")), il.regras_luminarias())
    ok("medicoes", il.medicoes_para_prata(_json(spark, chegada / "medicoes_robo"), lums), il.regras_medicoes())
    ok("ocorrencias", il.ocorrencias_para_prata(_json(spark, chegada / "ocorrencias_app"), lums, "sal-local-teste"),
       il.regras_ocorrencias())
    ok("sp156", il.sp156_para_prata(_csv(spark, chegada / "sp156")), il.regras_sp156())
    ok("clima", il.clima_para_prata(_json(spark, chegada / "clima")), il.regras_clima())
    ok("chamados", il.chamados_para_prata(_json(spark, chegada / "chamados_rpa"), lums), il.regras_chamados())
    return barrados


def comandos(arquivo: Path) -> list[str]:
    texto = "\n".join(l for l in arquivo.read_text(encoding="utf-8").splitlines()
                      if not l.lstrip().startswith("-- MAGIC") and "COMMAND ----------" not in l)
    texto = texto.replace("workspace.gs.", "")
    texto = re.sub(r"CREATE OR REPLACE (TABLE|VIEW)", "CREATE OR REPLACE TEMP VIEW", texto)
    return [c.strip() for c in re.split(r";\s*\n", texto) if re.sub(r"--.*", "", c).strip()]


def rodar(spark, chegada: Path):
    barrados = montar_prata(spark, chegada)
    for c in comandos(RAIZ / "databricks" / "03_ouro.sql"):
        spark.sql(c)
    resultados = [spark.sql(c) for c in comandos(RAIZ / "databricks" / "04_consultas.sql")]
    return barrados, resultados


if __name__ == "__main__":
    spark = (SparkSession.builder.master("local[4]").config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "America/Sao_Paulo").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    barrados, res = rodar(spark, Path(sys.argv[1]))
    print("BARRADOS POR REGRA:", barrados)
    for t in ("dim_luminaria", "dim_regiao", "dim_tempo", "fato_inspecao", "fato_ocorrencia", "fato_chamado"):
        print(t, spark.table(t).count())
    for i, df in enumerate(res, 1):
        print(f"\n== Q{i}")
        df.show(25, truncate=40)
