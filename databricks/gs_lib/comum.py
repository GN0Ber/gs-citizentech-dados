"""Funções puras de transformação (só PySpark, sem Delta), testadas localmente em tests/.

Os notebooks importam daqui para que a lógica testada seja a mesma que roda no
Databricks. Requer o repositório aberto como Git folder no workspace.
"""
from __future__ import annotations

import re
import unicodedata

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def nome_coluna(nome: str) -> str:
    """'Data de Finalização' -> 'data_de_finalizacao' (Delta não aceita espaço no nome)."""
    s = unicodedata.normalize("NFKD", nome)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^0-9a-z_]+", "_", s).strip("_")
    return s or "coluna"


def sanitizar_colunas(df: DataFrame) -> DataFrame:
    return df.select([F.col(f"`{c}`").alias(nome_coluna(c)) for c in df.columns])


def decimal_br(col: str | Column) -> Column:
    """'1.234,56' / ',00' / '472,42' -> double. Vazio vira nulo."""
    c = F.col(col) if isinstance(col, str) else col
    limpo = F.regexp_replace(F.regexp_replace(F.trim(c), r"\.", ""), ",", ".")
    return F.when(F.trim(c) == "", None).otherwise(limpo.cast("double"))


def aplicar_regras(df: DataFrame, regras: list[tuple[str, str, Column]]):
    """Avalia cada regra (nome, descrição, condição de VALIDADE) sobre a entrada inteira.

    Retorna (df_valido, df_barrado, contagens). Uma linha é barrada se falhar em
    qualquer regra; condição nula conta como falha (dado ausente não passa).
    `contagens` = [(nome, descricao, avaliadas, barradas)] para controle_qualidade.
    """
    marcado = df
    for i, (_, _, cond) in enumerate(regras):
        marcado = marcado.withColumn(f"__ok{i}", F.coalesce(cond, F.lit(False)))
    marcado = marcado.cache()
    total = marcado.count()
    contagens = []
    for i, (nome, desc, _) in enumerate(regras):
        barradas = marcado.filter(~F.col(f"__ok{i}")).count()
        contagens.append((nome, desc, total, barradas))

    flags = [F.col(f"__ok{i}") for i in range(len(regras))]
    tudo_ok = flags[0]
    for f in flags[1:]:
        tudo_ok = tudo_ok & f
    primeira_falha = F.coalesce(*[F.when(~F.col(f"__ok{i}"), F.lit(n))
                                  for i, (n, _, _) in enumerate(regras)])
    colunas = [c for c in df.columns]
    valido = marcado.filter(tudo_ok).select(colunas)
    barrado = (marcado.filter(~tudo_ok)
               .select(primeira_falha.alias("regra"),
                       F.to_json(F.struct(*[F.col(f"`{c}`") for c in colunas])).alias("registro")))
    return valido, barrado, contagens


def deduplicar(df: DataFrame, chaves: list[str], ordem: list[Column]) -> DataFrame:
    """Mantém uma linha por chave, a primeira segundo `ordem` (ex.: arquivo mais novo)."""
    w = Window.partitionBy(*chaves).orderBy(*ordem)
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")


def chave_hash(*cols: str) -> Column:
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("∅"))
                                      for c in cols]), 256)


# Caixa do município de São Paulo (folgada) para a regra de coordenada válida
SP_LAT = (-24.01, -23.35)
SP_LON = (-46.83, -46.36)


def dentro_de_sp(lat: str = "lat", lon: str = "lon") -> Column:
    return (F.col(lat).between(*SP_LAT) & F.col(lon).between(*SP_LON))
