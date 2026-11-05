-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup do lakehouse da GS
-- MAGIC Cria o schema, o Volume de pouso e as tabelas de controle. Rodar uma vez.
-- MAGIC
-- MAGIC Depois, em **Catalog > workspace > gs > chegada**, suba a pasta `chegada/` gerada pelo
-- MAGIC Airflow (cada lote é um upload, de dias de coleta diferentes: requisito 04).
-- MAGIC A estrutura dentro do Volume deve ficar `chegada/<fonte>/<fonte>_<data>.<ext>`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.gs
COMMENT 'GS CitizenTech 2026.2: Bronze, Prata e Ouro do componente Dados';

CREATE VOLUME IF NOT EXISTS workspace.gs.chegada
COMMENT 'Pasta de pouso: arquivos do Airflow enviados em lotes pela interface';

-- COMMAND ----------

-- Evidência do requisito 05: contagem antes/depois de cada carga da Bronze
CREATE TABLE IF NOT EXISTS workspace.gs.controle_cargas (
  camada        STRING,
  tabela        STRING,
  executado_em  TIMESTAMP,
  linhas_antes  BIGINT,
  linhas_depois BIGINT,
  linhas_novas  BIGINT,
  arquivos_total BIGINT
);

-- Evidência do requisito 06: total barrado por regra de qualidade
CREATE TABLE IF NOT EXISTS workspace.gs.controle_qualidade (
  tabela        STRING,
  regra         STRING,
  descricao     STRING,
  executado_em  TIMESTAMP,
  linhas_avaliadas BIGINT,
  linhas_barradas  BIGINT
);

-- Linhas barradas ficam guardadas com o motivo (nada some em silêncio)
CREATE TABLE IF NOT EXISTS workspace.gs.prata_quarentena (
  tabela       STRING,
  regra        STRING,
  registro     STRING,
  barrado_em   TIMESTAMP
);
