-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Ouro: modelo dimensional e contrato com o MVP (requisitos 07 e 08)
-- MAGIC
-- MAGIC Estrela com dois fatos que compartilham as dimensões:
-- MAGIC
-- MAGIC * **fato_inspecao**: uma linha por medição do robô (grão: medição) · métricas: lux, distância
-- MAGIC * **fato_ocorrencia**: uma linha por ocorrência (app/visão) ou pedido do SP156 · métrica: dias de atendimento
-- MAGIC * **fato_chamado**: uma linha por chamado do RPA (grão: chamado) · métricas: horas até a conclusão, reparo confirmado
-- MAGIC * **dim_luminaria** (cadastro ILUME real), **dim_regiao**, **dim_tempo**
-- MAGIC
-- MAGIC **LGPD na Ouro**: nenhum nome, e-mail ou telefone chega aqui; o cidadão é só um
-- MAGIC pseudônimo (`cidadao_hash`, SHA-256 com sal). O endereço do SP156 perde o número e a
-- MAGIC coordenada é generalizada para ~110 m (3 casas decimais). As views `vw_mvp_*` são o
-- MAGIC contrato com o produto e não expõem nem o pseudônimo.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_luminaria AS
SELECT id_luminaria, placa, tipo_lampada, endereco, unidade_ilume,
       lat, lon, subprefeitura, data_carga_ilume
FROM workspace.gs.prata_luminarias;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_regiao AS
WITH todas AS (
  SELECT subprefeitura FROM workspace.gs.prata_luminarias
  UNION SELECT subprefeitura FROM workspace.gs.prata_sp156
  UNION SELECT subprefeitura FROM workspace.gs.prata_ocorrencias
)
SELECT t.subprefeitura,
       l.subprefeitura IS NOT NULL                    AS area_piloto,
       coalesce(l.luminarias_cadastradas, 0)          AS luminarias_cadastradas
FROM todas t
LEFT JOIN (SELECT subprefeitura, count(*) AS luminarias_cadastradas
           FROM workspace.gs.prata_luminarias GROUP BY subprefeitura) l
  ON l.subprefeitura = t.subprefeitura
WHERE t.subprefeitura IS NOT NULL AND t.subprefeitura <> '';

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.dim_tempo AS
WITH limites AS (
  SELECT least(min_m, min_o, min_s) AS ini, greatest(max_m, max_o, max_s) AS fim FROM (
    SELECT (SELECT min(to_date(data_hora)) FROM workspace.gs.prata_medicoes)    AS min_m,
           (SELECT max(to_date(data_hora)) FROM workspace.gs.prata_medicoes)    AS max_m,
           (SELECT min(to_date(data_hora)) FROM workspace.gs.prata_ocorrencias) AS min_o,
           (SELECT max(to_date(data_hora)) FROM workspace.gs.prata_ocorrencias) AS max_o,
           (SELECT min(data_abertura) FROM workspace.gs.prata_sp156)            AS min_s,
           (SELECT max(data_abertura) FROM workspace.gs.prata_sp156)            AS max_s))
SELECT CAST(date_format(d, 'yyyyMMdd') AS INT) AS data_key,
       d AS data, year(d) AS ano, month(d) AS mes, quarter(d) AS trimestre,
       date_format(d, 'EEEE') AS dia_semana, dayofweek(d) IN (1, 7) AS fim_de_semana
FROM limites LATERAL VIEW explode(sequence(ini, fim, interval 1 day)) AS d;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.fato_inspecao AS
SELECT m.id_medicao,
       CAST(date_format(m.data_hora, 'yyyyMMdd') AS INT) AS data_key,
       m.subprefeitura,
       m.id_luminaria,
       m.data_hora,
       hour(m.data_hora)                                  AS hora,
       m.lux,
       m.situacao,
       m.distancia_luminaria_m,
       m.robo_id,
       c.chuva_mm,
       c.horas_escuridao,
       -- leitura feita antes do pôr do sol real é menos confiável (crepúsculo)
       (c.por_sol IS NULL OR m.data_hora >= c.por_sol + INTERVAL 30 MINUTES) AS apos_crepusculo
FROM workspace.gs.prata_medicoes m
LEFT JOIN workspace.gs.prata_clima c
  ON c.subprefeitura = m.subprefeitura AND c.data = to_date(m.data_hora);

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gs.fato_ocorrencia AS
SELECT id_ocorrencia                                       AS id_ocorrencia,
       'app'                                               AS fonte,
       origem,
       categoria,
       CAST(date_format(data_hora, 'yyyyMMdd') AS INT)     AS data_key,
       subprefeitura,
       id_luminaria,
       'nova'                                              AS status,
       CAST(NULL AS INT)                                   AS dias_atendimento,
       confianca_visao,
       cidadao_hash,                                       -- pseudônimo (LGPD)
       round(lat, 3) AS lat_aprox, round(lon, 3) AS lon_aprox
FROM workspace.gs.prata_ocorrencias
UNION ALL
SELECT id_pedido, 'sp156', canal, categoria,
       CAST(date_format(data_abertura, 'yyyyMMdd') AS INT),
       subprefeitura, CAST(NULL AS BIGINT), status, dias_atendimento,
       CAST(NULL AS DOUBLE), CAST(NULL AS STRING),
       round(lat, 3), round(lon, 3)                        -- sem logradouro/número (LGPD)
FROM workspace.gs.prata_sp156;

-- COMMAND ----------

-- Fato do ciclo de manutenção (grão: chamado). Fecha o indicador "tempo até o reparo":
-- da abertura pelo RPA à conclusão informada, e se o robô confirmou a luz de volta depois.
CREATE OR REPLACE TABLE workspace.gs.fato_chamado AS
WITH confirmacao AS (
  SELECT c.id_chamado,
         min(m.data_hora) AS primeira_medicao_ok_apos
  FROM workspace.gs.prata_chamados c
  JOIN workspace.gs.prata_medicoes m
    ON m.id_luminaria = c.id_luminaria AND m.situacao = 'ok' AND m.data_hora > c.concluido_em
  WHERE c.status = 'concluido'
  GROUP BY c.id_chamado)
SELECT c.id_chamado,
       c.placa,
       c.id_luminaria,
       c.subprefeitura,
       CAST(date_format(c.aberto_em, 'yyyyMMdd') AS INT)   AS data_key,
       c.prioridade,
       c.situacao_detectada,
       c.status,
       c.equipe,
       c.aberto_em,
       c.concluido_em,
       c.horas_ate_conclusao,
       k.primeira_medicao_ok_apos,
       k.primeira_medicao_ok_apos IS NOT NULL               AS reparo_confirmado_pelo_robo
FROM workspace.gs.prata_chamados c
LEFT JOIN confirmacao k ON k.id_chamado = c.id_chamado;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contrato com o MVP
-- MAGIC Três views, documentadas em `docs/contrato_mvp.md`. O app, a automação (RPA) e a IA
-- MAGIC consomem só estas views (ou o export JSON delas no notebook 05).

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.gs.vw_mvp_mapa_luminarias AS
WITH ultima AS (
  SELECT id_luminaria, data_hora, lux, situacao,
         row_number() OVER (PARTITION BY id_luminaria ORDER BY data_hora DESC) AS rn
  FROM workspace.gs.fato_inspecao WHERE id_luminaria IS NOT NULL),
ocorr AS (
  SELECT id_luminaria, count(*) AS ocorrencias_app
  FROM workspace.gs.fato_ocorrencia WHERE fonte = 'app' AND id_luminaria IS NOT NULL
  GROUP BY id_luminaria)
SELECT l.id_luminaria, l.placa, l.endereco, l.subprefeitura, l.lat, l.lon,
       u.data_hora AS ultima_medicao, u.lux AS ultimo_lux,
       coalesce(u.situacao, 'sem_medicao') AS situacao,
       coalesce(o.ocorrencias_app, 0)      AS ocorrencias_app
FROM workspace.gs.dim_luminaria l
LEFT JOIN ultima u ON u.id_luminaria = l.id_luminaria AND u.rn = 1
LEFT JOIN ocorr o  ON o.id_luminaria = l.id_luminaria;

-- COMMAND ----------

-- Fila que a automação (RPA) transforma em chamado. Prioridade 0..100:
-- apagada 60 / insuficiente 30 + 10 por ocorrência de cidadão (máx. 30) + 10 se a visão
-- também apontou com confiança >= 0.8 (evidência de duas fontes independentes).
CREATE OR REPLACE VIEW workspace.gs.vw_mvp_fila_manutencao AS
WITH visao AS (
  SELECT id_luminaria, max(confianca_visao) AS max_confianca
  FROM workspace.gs.fato_ocorrencia
  WHERE fonte = 'app' AND categoria IN ('luminaria_apagada', 'luminaria_piscando')
  GROUP BY id_luminaria),
chamado AS (
  SELECT placa, id_chamado, status, aberto_em,
         row_number() OVER (PARTITION BY placa ORDER BY aberto_em DESC) AS rn
  FROM workspace.gs.fato_chamado
  WHERE status IN ('aberto', 'em_atendimento'))
SELECT m.id_luminaria, m.placa, m.endereco, m.subprefeitura, m.lat, m.lon,
       m.situacao, m.ultimo_lux, m.ultima_medicao, m.ocorrencias_app,
       ch.id_chamado AS chamado_aberto, ch.status AS status_chamado,
       coalesce(v.max_confianca, 0) AS confianca_visao,
       least(100, CASE m.situacao WHEN 'apagada' THEN 60 WHEN 'insuficiente' THEN 30 ELSE 0 END
                  + least(30, 10 * m.ocorrencias_app)
                  + CASE WHEN v.max_confianca >= 0.8 THEN 10 ELSE 0 END) AS prioridade
FROM workspace.gs.vw_mvp_mapa_luminarias m
LEFT JOIN visao v ON v.id_luminaria = m.id_luminaria
LEFT JOIN chamado ch ON ch.placa = m.placa AND ch.rn = 1
WHERE m.situacao IN ('apagada', 'insuficiente') OR v.max_confianca >= 0.8;

-- COMMAND ----------

CREATE OR REPLACE VIEW workspace.gs.vw_mvp_painel_subprefeitura AS
SELECT r.subprefeitura,
       r.luminarias_cadastradas,
       count(DISTINCT CASE WHEN m.situacao <> 'sem_medicao' THEN m.id_luminaria END) AS luminarias_inspecionadas,
       count(DISTINCT CASE WHEN m.situacao = 'apagada' THEN m.id_luminaria END)      AS apagadas,
       count(DISTINCT CASE WHEN m.situacao = 'insuficiente' THEN m.id_luminaria END) AS insuficientes,
       round(100 * count(DISTINCT CASE WHEN m.situacao IN ('apagada','insuficiente') THEN m.id_luminaria END)
             / nullif(count(DISTINCT CASE WHEN m.situacao <> 'sem_medicao' THEN m.id_luminaria END), 0), 1)
         AS pct_com_falha,
       max(ch.mediana_horas_reparo)   AS mediana_horas_ate_reparo,
       max(ch.chamados_em_aberto)     AS chamados_em_aberto
FROM workspace.gs.dim_regiao r
LEFT JOIN workspace.gs.vw_mvp_mapa_luminarias m ON m.subprefeitura = r.subprefeitura
LEFT JOIN (SELECT subprefeitura,
                  percentile_approx(horas_ate_conclusao, 0.5)                               AS mediana_horas_reparo,
                  sum(CASE WHEN status IN ('aberto', 'em_atendimento') THEN 1 ELSE 0 END)  AS chamados_em_aberto
           FROM workspace.gs.fato_chamado GROUP BY subprefeitura) ch
  ON ch.subprefeitura = r.subprefeitura
WHERE r.area_piloto
GROUP BY r.subprefeitura, r.luminarias_cadastradas;
