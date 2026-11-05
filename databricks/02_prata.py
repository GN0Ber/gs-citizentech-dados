# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Prata (requisito 06)
# MAGIC Tipos corretos, nulos e inválidos tratados, MERGE pela chave e regras de qualidade
# MAGIC com o total barrado por regra (`gs.controle_qualidade`). Linhas barradas não somem:
# MAGIC vão para `gs.prata_quarentena` com o motivo.
# MAGIC
# MAGIC | Tabela | Chave do MERGE | Regras |
# MAGIC |---|---|---|
# MAGIC | prata_luminarias | id_luminaria | id, coordenada em SP, placa |
# MAGIC | prata_medicoes | id_medicao | lux >= 0, GPS em SP, noturna, a até 30 m de luminária |
# MAGIC | prata_ocorrencias | id_ocorrencia | categoria, coordenada, confiança 0..1, data não futura |
# MAGIC | prata_sp156 | id_pedido | abertura válida, coordenada, finalização coerente |
# MAGIC | prata_clima | subprefeitura + data | data, sol coerente, chuva >= 0 |
# MAGIC | prata_chamados | id_chamado | status do contrato, prioridade 0..100, conclusão coerente, placa no cadastro |

# COMMAND ----------

# Sal da pseudonimização (LGPD). Nunca fica no código nem no Git.
# Ordem: widget sal_lgpd (se preenchido) → arquivo no Volume → gera um aleatório na 1ª vez
# e guarda no Volume, para o lote 2 usar o MESMO sal (senão os pseudônimos mudam).
import os, sys, secrets
ARQ_SAL = "/Volumes/workspace/gs/chegada/_controle/sal_lgpd.txt"
try:
    dbutils.widgets.text("sal_lgpd", "")
    SAL = dbutils.widgets.get("sal_lgpd").strip()
except Exception:
    SAL = ""
if len(SAL) < 16:
    if os.path.exists(ARQ_SAL):
        SAL = open(ARQ_SAL, encoding="utf-8").read().strip()
    else:
        SAL = secrets.token_hex(24)
        os.makedirs(os.path.dirname(ARQ_SAL), exist_ok=True)
        with open(ARQ_SAL, "w", encoding="utf-8") as f:
            f.write(SAL)
        print("Sal novo gerado e guardado no Volume (fora do Git):", ARQ_SAL)
assert len(SAL) >= 16, "Sal inválido"

sys.path.insert(0, os.getcwd())
from gs_lib import iluminacao as il
from gs_lib.comum import aplicar_regras
from gs_lib.delta_io import merge, registrar_qualidade

def processar(nome, df, regras, chaves):
    ok, barrado, contagens = aplicar_regras(df, regras)
    registrar_qualidade(spark, f"prata_{nome}", contagens, barrado)
    r = merge(spark, ok, f"prata_{nome}", chaves)
    print(nome, r, [(c[0], c[3]) for c in contagens])
    return r

b = lambda f: spark.table(f"workspace.gs.bronze_{f}")

# COMMAND ----------

# Luminárias primeiro: medições e ocorrências se casam com elas
processar("luminarias", il.luminarias_para_prata(b("luminarias")), il.regras_luminarias(), ["id_luminaria"])
lums = spark.table("workspace.gs.prata_luminarias")

processar("medicoes", il.medicoes_para_prata(b("medicoes_robo"), lums), il.regras_medicoes(), ["id_medicao"])
processar("ocorrencias", il.ocorrencias_para_prata(b("ocorrencias_app"), lums, SAL),
          il.regras_ocorrencias(), ["id_ocorrencia"])
processar("sp156", il.sp156_para_prata(b("sp156")), il.regras_sp156(), ["id_pedido"])
processar("clima", il.clima_para_prata(b("clima")), il.regras_clima(), ["subprefeitura", "data"])
processar("chamados", il.chamados_para_prata(b("chamados_rpa"), lums), il.regras_chamados(), ["id_chamado"])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Print para o relatório: total barrado por regra na última execução
# MAGIC SELECT tabela, regra, descricao, linhas_avaliadas, linhas_barradas,
# MAGIC        round(100 * linhas_barradas / linhas_avaliadas, 2) AS pct_barrado
# MAGIC FROM workspace.gs.controle_qualidade
# MAGIC QUALIFY executado_em = max(executado_em) OVER (PARTITION BY tabela)
# MAGIC ORDER BY tabela, regra
