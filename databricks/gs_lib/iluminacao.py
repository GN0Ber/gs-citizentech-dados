"""Transformações Bronze -> Prata do tema iluminação pública.

Fontes: cadastro ILUME (GeoSampa), SP156 (postes e fiação), clima (Open-Meteo),
medições LDR do robô e ocorrências do app classificadas pela visão.
Cada função recebe o DataFrame da Bronze e devolve o DataFrame candidato à Prata;
as regras de qualidade ficam ao lado de cada função (REGRAS_*).
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .comum import chave_hash, deduplicar, dentro_de_sp

# Limiares de iluminância medida pelo robô sob a luminária (lux).
# Referência: ABNT NBR 5101 (iluminação pública) exige iluminância média de alguns
# lux nas vias de pedestres, conforme a classe da via. Valores de trabalho do MVP,
# a calibrar com o LDR real do Maker Lab.
LUX_APAGADA = 1.0
LUX_MINIMO = 5.0
RAIO_CASAMENTO_M = 30.0


# ---------------------------------------------------------------- luminárias

def luminarias_para_prata(bronze: DataFrame) -> DataFrame:
    l = bronze.select(F.col("_coleta.data_coleta").alias("data_coleta"), "arquivo_origem",
                      "carregado_em", F.explode("dados").alias("l"))
    df = l.select(
        F.col("l.cd_identificador_iluminacao_publica").cast("long").alias("id_luminaria"),
        F.trim("l.cd_placa").alias("placa"),
        F.upper(F.trim("l.dc_tipo_iluminacao_publica")).alias("tipo_lampada"),
        F.upper(F.trim("l.tx_endereco")).alias("endereco"),
        F.nullif(F.trim("l.tx_unidade"), F.lit("")).alias("unidade_ilume"),
        F.col("l.lat").cast("double").alias("lat"),
        F.col("l.lon").cast("double").alias("lon"),
        F.col("l.area_piloto").alias("subprefeitura"),
        F.to_date(F.regexp_replace("l.dt_carga", "Z$", "")).alias("data_carga_ilume"),
        F.col("l.sg_fonte_original").alias("fonte_original"),
        F.to_date("data_coleta").alias("data_coleta"),
        "arquivo_origem", "carregado_em",
    )
    return deduplicar(df, ["id_luminaria"], [F.col("carregado_em").desc()])


def regras_luminarias() -> list:
    """(nome, descrição, condição de validade). Função porque Column exige Spark ativo."""
    return [
    ("id_presente", "Ponto sem identificador ILUME não vira dimensão", F.col("id_luminaria").isNotNull()),
    ("coordenada_em_sp", "Coordenada presente e dentro do município", dentro_de_sp()),
    ("placa_presente", "Sem placa o chamado não chega à equipe de campo",
     F.col("placa").isNotNull() & (F.col("placa") != "")),
]


# ---------------------------------------------------------------- casamento geográfico

def _celula(col_lat: str, col_lon: str, tam: float = 0.0005):
    return (F.floor(F.col(col_lat) / tam).cast("long"), F.floor(F.col(col_lon) / tam).cast("long"))


def distancia_m(lat1, lon1, lat2, lon2) -> Column:
    """Haversine em metros."""
    r = 6_371_000.0
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.sin(dlon / 2) ** 2)
    return 2 * r * F.asin(F.sqrt(a))


def casar_luminaria(pontos: DataFrame, luminarias: DataFrame, chave: str,
                    raio_m: float = RAIO_CASAMENTO_M) -> DataFrame:
    """Liga cada ponto (medição/ocorrência) à luminária cadastrada mais próxima.

    Grade de ~55 m: cada ponto procura nas 9 células ao redor, então qualquer
    luminária a até ~55 m é candidata; depois fica a mais próxima dentro do raio.
    Pontos sem luminária no raio saem com id_luminaria nulo.
    """
    cy, cx = _celula("lat", "lon")
    vizinhos = F.array(*[F.struct(F.lit(dy).alias("dy"), F.lit(dx).alias("dx"))
                         for dy in (-1, 0, 1) for dx in (-1, 0, 1)])
    p = (pontos.withColumn("__cy", cy).withColumn("__cx", cx)
               .withColumn("__v", F.explode(vizinhos))
               .withColumn("__ky", F.col("__cy") + F.col("__v.dy"))
               .withColumn("__kx", F.col("__cx") + F.col("__v.dx")))
    ly, lx = _celula("l_lat", "l_lon")
    l = (luminarias.select(F.col("id_luminaria").alias("l_id"), F.col("lat").alias("l_lat"),
                           F.col("lon").alias("l_lon"))
                   .withColumn("__ky", ly).withColumn("__kx", lx))
    cand = (p.join(l, ["__ky", "__kx"], "left")
             .withColumn("__d", distancia_m(F.col("lat"), F.col("lon"), F.col("l_lat"), F.col("l_lon"))))
    w = Window.partitionBy(chave).orderBy(F.col("__d").asc_nulls_last(), F.col("l_id"))
    melhor = cand.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1")
    dentro = F.col("__d") <= raio_m
    return (melhor
            .withColumn("id_luminaria", F.when(dentro, F.col("l_id")))
            .withColumn("distancia_luminaria_m", F.when(dentro, F.round("__d", 1)))
            .drop(*[c for c in melhor.columns if c.startswith("__") or c.startswith("l_")]))


# ---------------------------------------------------------------- medições do robô

def medicoes_para_prata(bronze: DataFrame, luminarias_prata: DataFrame) -> DataFrame:
    m = bronze.select("arquivo_origem", "carregado_em", F.explode("dados").alias("m"))
    df = m.select(
        F.col("m.id_medicao").alias("id_medicao"),
        F.col("m.robo_id").alias("robo_id"),
        F.to_timestamp("m.data_hora").alias("data_hora"),
        F.col("m.lat").cast("double").alias("lat"),
        F.col("m.lon").cast("double").alias("lon"),
        F.col("m.lux").cast("double").alias("lux"),
        F.col("m.sensor").alias("sensor"),
        F.col("m.bateria_pct").cast("int").alias("bateria_pct"),
        F.col("m.area_piloto").alias("subprefeitura"),
        "arquivo_origem", "carregado_em",
    )
    df = deduplicar(df, ["id_medicao"], [F.col("carregado_em").desc()])
    df = casar_luminaria(df, luminarias_prata, "id_medicao")
    return df.withColumn(
        "situacao",
        F.when(F.col("lux") < LUX_APAGADA, "apagada")
         .when(F.col("lux") < LUX_MINIMO, "insuficiente")
         .otherwise("ok"))


def regras_medicoes() -> list:
    """(nome, descrição, condição de validade). Função porque Column exige Spark ativo."""
    return [
    ("lux_valido", "Leitura do LDR não pode ser negativa", F.col("lux") >= 0),
    ("coordenada_em_sp", "GPS com fix e dentro do município", dentro_de_sp()),
    ("medicao_noturna", "Iluminância de rua só vale entre 18h e 6h", (F.hour("data_hora") >= 18) | (F.hour("data_hora") < 6)),
    ("perto_de_luminaria", f"A até {RAIO_CASAMENTO_M:.0f} m de um ponto do cadastro ILUME",
     F.col("id_luminaria").isNotNull()),
]


# ---------------------------------------------------------------- ocorrências do app

def ocorrencias_para_prata(bronze: DataFrame, luminarias_prata: DataFrame, sal: str) -> DataFrame:
    o = bronze.select(F.col("_coleta.data_coleta").alias("data_coleta"), "arquivo_origem",
                      "carregado_em", F.explode("dados").alias("o"))
    email = F.lower(F.trim(F.col("o.cidadao.email")))
    df = o.select(
        F.col("o.id_ocorrencia").alias("id_ocorrencia"),
        F.to_timestamp("o.data_hora").alias("data_hora"),
        F.col("o.categoria").alias("categoria"),
        F.col("o.origem").alias("origem"),
        F.col("o.confianca_visao").cast("double").alias("confianca_visao"),
        F.col("o.modelo_visao").alias("modelo_visao"),
        F.col("o.foto_id").alias("foto_id"),
        F.col("o.lat").cast("double").alias("lat"),
        F.col("o.lon").cast("double").alias("lon"),
        F.col("o.area_piloto").alias("subprefeitura"),
        # LGPD: nome e telefone ficam na Bronze; o e-mail vira pseudônimo com sal
        F.when(email.isNotNull(), F.sha2(F.concat(email, F.lit(sal)), 256)).alias("cidadao_hash"),
        F.to_date("data_coleta").alias("data_coleta"),
        "arquivo_origem", "carregado_em",
    )
    df = deduplicar(df, ["id_ocorrencia"], [F.col("carregado_em").desc()])
    return casar_luminaria(df, luminarias_prata, "id_ocorrencia", raio_m=40.0)


def regras_ocorrencias() -> list:
    """(nome, descrição, condição de validade). Função porque Column exige Spark ativo."""
    return [
    ("categoria_obrigatoria", "Ocorrência precisa ter categoria", F.col("categoria").isNotNull()),
    ("coordenada_em_sp", "Coordenada dentro do município", dentro_de_sp()),
    ("confianca_valida", "Confiança da visão entre 0 e 1", F.col("confianca_visao").between(0, 1)),
    ("data_nao_futura", "Data não passa do dia seguinte à coleta",
     F.to_date("data_hora") <= F.date_add("data_coleta", 1)),
]


# ---------------------------------------------------------------- SP156

CHAVE_SP156 = ["data_de_abertura", "meio_de_comunicacao", "servico", "logradouro", "numero",
               "cep", "latitude", "longitude", "prefeitura_operacional", "distrito"]


def _formato_data_por_arquivo(df: DataFrame, coluna: str = "data_de_abertura") -> DataFrame:
    """Achado real: o 2º TRI 2026 publica 01/04/2026 (dia/mês) e o 3º TRI publica
    07/14/2026 (mês/dia). Dentro de um arquivo o padrão é único, então basta ver qual
    posição passa de 12."""
    partes = F.split(F.col(coluna), "/")
    w = Window.partitionBy("arquivo_origem")
    return (df.withColumn("__p2", F.max(partes.getItem(1).cast("int")).over(w))
              .withColumn("formato_data", F.when(F.col("__p2") > 12, F.lit("MM/dd/yyyy"))
                          .otherwise(F.lit("dd/MM/yyyy")))
              .drop("__p2"))


def _data(coluna: str) -> Column:
    c = F.trim(F.col(coluna))
    return (F.when(c == "", None)
             .when(F.col("formato_data") == "MM/dd/yyyy", F.to_date(c, "MM/dd/yyyy"))
             .otherwise(F.to_date(c, "dd/MM/yyyy")))


def sp156_para_prata(bronze: DataFrame) -> DataFrame:
    df = _formato_data_por_arquivo(bronze)
    # O dado aberto não tem protocolo. Chave = hash dos campos que não mudam com o
    # andamento + nº da ocorrência (pedidos idênticos são pedidos distintos). Status e
    # finalização ficam fora da chave, para o MERGE ATUALIZAR o pedido quando fechar.
    w = Window.partitionBy("arquivo_origem", "__h").orderBy(F.lit(1))
    df = (df.withColumn("__h", chave_hash(*CHAVE_SP156))
            .withColumn("__n", F.row_number().over(w))
            .withColumn("id_pedido", F.concat_ws("-", "__h", "__n")))
    s = F.lower("servico")
    df = df.select(
        "id_pedido",
        _data("data_de_abertura").alias("data_abertura"),
        _data("data_de_finalizacao").alias("data_finalizacao"),
        F.upper(F.trim("meio_de_comunicacao")).alias("canal"),
        F.trim("servico").alias("servico"),
        F.when(s.contains("fiação") | s.contains("fiacao"), "fiacao_exposta")
         .otherwise("poste_danificado").alias("categoria"),
        F.trim("status").alias("status"),
        F.trim("orgao").alias("orgao"),
        F.upper(F.trim("logradouro")).alias("logradouro"),
        F.trim("numero").alias("numero"),
        F.lpad(F.regexp_replace("cep", r"\D", ""), 8, "0").alias("cep"),
        F.regexp_replace("latitude", ",", ".").cast("double").alias("lat"),
        F.regexp_replace("longitude", ",", ".").cast("double").alias("lon"),
        F.upper(F.trim("prefeitura_operacional")).alias("subprefeitura"),
        F.upper(F.trim("distrito")).alias("distrito"),
        "formato_data", "arquivo_origem", "carregado_em",
    ).withColumn("dias_atendimento", F.datediff("data_finalizacao", "data_abertura"))
    return deduplicar(df, ["id_pedido"], [F.col("carregado_em").desc(), F.col("arquivo_origem").desc()])


def regras_sp156() -> list:
    """(nome, descrição, condição de validade). Função porque Column exige Spark ativo."""
    return [
    ("data_abertura_valida", "Abertura presente, legível e não futura",
     F.col("data_abertura").isNotNull() & (F.col("data_abertura") <= F.current_date())),
    ("coordenada_em_sp", "Latitude/longitude dentro do município", dentro_de_sp()),
    ("finalizacao_coerente", "Finalização ausente ou não anterior à abertura",
     F.col("data_finalizacao").isNull() | (F.col("data_finalizacao") >= F.col("data_abertura"))),
]


# ---------------------------------------------------------------- clima

def clima_para_prata(bronze: DataFrame) -> DataFrame:
    p = bronze.select("arquivo_origem", "carregado_em", F.explode("dados").alias("p"))
    d = p.select("arquivo_origem", "carregado_em", F.col("p.ponto").alias("ponto"),
                 F.col("p.daily").alias("daily"))
    d = d.select("*", F.posexplode("daily.time").alias("i", "dia"))
    em = lambda c: F.element_at(F.col(f"daily.{c}"), F.col("i") + 1)
    df = d.select(
        F.upper("ponto").alias("subprefeitura"),
        F.to_date("dia").alias("data"),
        F.to_timestamp(em("sunrise")).alias("nascer_sol"),
        F.to_timestamp(em("sunset")).alias("por_sol"),
        (em("daylight_duration").cast("double") / 3600).alias("horas_luz"),
        em("precipitation_sum").cast("double").alias("chuva_mm"),
        em("temperature_2m_max").cast("double").alias("temp_max"),
        em("temperature_2m_min").cast("double").alias("temp_min"),
        "arquivo_origem", "carregado_em",
    ).withColumn("horas_escuridao", F.round(24 - F.col("horas_luz"), 2))
    return deduplicar(df, ["subprefeitura", "data"], [F.col("carregado_em").desc()])


def regras_clima() -> list:
    """(nome, descrição, condição de validade). Função porque Column exige Spark ativo."""
    return [
    ("data_presente", "Dia informado", F.col("data").isNotNull()),
    ("sol_coerente", "Pôr do sol depois do nascer", F.col("por_sol") > F.col("nascer_sol")),
    ("chuva_nao_negativa", "Chuva >= 0 mm", F.col("chuva_mm") >= 0),
]
