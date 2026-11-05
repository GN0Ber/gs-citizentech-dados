# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze (requisito 05)
# MAGIC Lê do Volume só os arquivos que ainda não foram carregados (Auto Loader com checkpoint)
# MAGIC e grava cada fonte em `workspace.gs.bronze_<fonte>`, sem tratar nada.
# MAGIC
# MAGIC **Prova de idempotência para o relatório** (contagens ficam em `gs.controle_cargas`):
# MAGIC 1. suba o lote 1 e rode → anote as contagens;
# MAGIC 2. suba o lote 2 (outro dia de coleta) e rode → só as linhas novas entram;
# MAGIC 3. rode de novo sem subir nada → `linhas_novas = 0`.

# COMMAND ----------

# fonte -> formato do arquivo na pasta de chegada
FONTES = {"luminarias": "json", "sp156": "csv", "clima": "json",
          "medicoes_robo": "json", "ocorrencias_app": "json"}
BASE = "/Volumes/workspace/gs/chegada"

# COMMAND ----------

import os, sys
sys.path.insert(0, os.getcwd())  # Git folder: permite importar gs_lib ao lado do notebook
from pyspark.sql import functions as F
from gs_lib.comum import nome_coluna
from gs_lib.delta_io import registrar_carga

def carregar_bronze(fonte: str, formato: str) -> dict:
    tabela = f"workspace.gs.bronze_{fonte}"
    antes = spark.table(tabela).count() if spark.catalog.tableExists(tabela) else 0
    leitor = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", formato)
              .option("cloudFiles.schemaLocation", f"{BASE}/_controle/schema_{fonte}")
              .option("pathGlobFilter", f"*.{formato}"))   # ignora _meta/ e .tmp
    if formato == "csv":
        leitor = leitor.option("header", "true").option("encoding", "UTF-8")
    df = leitor.load(f"{BASE}/{fonte}")
    df = df.select([F.col(f"`{c}`").alias(nome_coluna(c) if c != "_coleta" else c)
                    for c in df.columns])                  # Delta não aceita espaço no nome
    consulta = (df.withColumn("arquivo_origem", F.col("_metadata.file_path"))
                  .withColumn("carregado_em", F.current_timestamp())
                  .writeStream
                  .option("checkpointLocation", f"{BASE}/_controle/checkpoint_{fonte}")
                  .option("mergeSchema", "true")
                  .trigger(availableNow=True)
                  .toTable(tabela))
    consulta.awaitTermination()
    depois = spark.table(tabela).count()
    arquivos = spark.table(tabela).select("arquivo_origem").distinct().count()
    registrar_carga(spark, "bronze", f"bronze_{fonte}", antes, depois, arquivos)
    return {"fonte": fonte, "antes": antes, "depois": depois, "novas": depois - antes,
            "arquivos": arquivos}

# COMMAND ----------

resultado = [carregar_bronze(f, fmt) for f, fmt in FONTES.items()]
display(spark.createDataFrame(resultado))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Histórico de cargas: print deste resultado vai no item 07 do relatório
# MAGIC SELECT * FROM workspace.gs.controle_cargas WHERE camada = 'bronze' ORDER BY executado_em DESC
