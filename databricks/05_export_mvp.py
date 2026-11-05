# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Entrega ao MVP (requisito 08)
# MAGIC O Databricks Free Edition não fica de pé 24 h nem deve ter token embutido no app.
# MAGIC Por isso a entrega ao produto é um **export JSON das views `vw_mvp_*`** num Volume;
# MAGIC a API do MVP (componente Produção) baixa esses arquivos e os serve. Formato e campos
# MAGIC em `docs/contrato_mvp.md`. Rodar depois do 03.

# COMMAND ----------

import json
from datetime import datetime, timezone

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gs.mvp COMMENT 'Export das views do MVP'")
DESTINO = "/Volumes/workspace/gs/mvp"
VIEWS = ["vw_mvp_mapa_luminarias", "vw_mvp_fila_manutencao", "vw_mvp_painel_subprefeitura"]

gerado_em = datetime.now(timezone.utc).isoformat(timespec="seconds")
for v in VIEWS:
    linhas = [r.asDict() for r in spark.table(f"workspace.gs.{v}").collect()]
    corpo = {"view": v, "gerado_em": gerado_em, "linhas": len(linhas), "dados": linhas}
    with open(f"{DESTINO}/{v}.json", "w", encoding="utf-8") as f:
        json.dump(corpo, f, ensure_ascii=False, default=str)
    print(v, len(linhas))

# COMMAND ----------

# MAGIC %md
# MAGIC Baixe os três arquivos em **Catalog > workspace > gs > mvp** e entregue ao
# MAGIC repositório do MVP (pasta `data/`), ou publique onde a API ler. Print desta tela
# MAGIC e da tela do MVP exibindo os dados vão no item 05/08 do relatório.
