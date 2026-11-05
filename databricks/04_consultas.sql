-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · As consultas da Ouro (requisito 07: 5 obrigatórias, aqui 6)
-- MAGIC Cada uma responde uma pergunta que o MVP mostra ao gestor. Interpretação dos
-- MAGIC resultados vai no relatório (item 08), ao lado do print de cada uma.

-- COMMAND ----------

-- Q1 · Qual a situação da iluminação em cada subprefeitura do piloto agora?
-- (última medição de cada luminária inspecionada)
SELECT subprefeitura, luminarias_cadastradas, luminarias_inspecionadas,
       apagadas, insuficientes, pct_com_falha
FROM workspace.gs.vw_mvp_painel_subprefeitura
ORDER BY pct_com_falha DESC;

-- COMMAND ----------

-- Q2 · Quais luminárias a equipe de campo deve atender primeiro?
SELECT placa, endereco, subprefeitura, situacao, ultimo_lux, ocorrencias_app,
       confianca_visao, prioridade
FROM workspace.gs.vw_mvp_fila_manutencao
ORDER BY prioridade DESC, ultimo_lux ASC
LIMIT 20;

-- COMMAND ----------

-- Q3 · IMPACTO: quantas falhas o sistema achou antes de algum cidadão reclamar,
-- e quantas horas de rua no escuro isso representa por noite?
WITH falhas AS (
  SELECT m.id_luminaria, m.subprefeitura, m.situacao, m.ocorrencias_app
  FROM workspace.gs.vw_mvp_mapa_luminarias m
  WHERE m.situacao IN ('apagada', 'insuficiente')),
escuro AS (
  SELECT subprefeitura, avg(horas_escuridao) AS horas_escuridao_media
  FROM workspace.gs.prata_clima GROUP BY subprefeitura)
SELECT f.subprefeitura,
       count(*)                                              AS falhas_detectadas,
       sum(CASE WHEN f.ocorrencias_app = 0 THEN 1 ELSE 0 END) AS falhas_sem_nenhum_relato,
       round(100 * sum(CASE WHEN f.ocorrencias_app = 0 THEN 1 ELSE 0 END) / count(*), 1)
                                                             AS pct_deteccao_proativa,
       round(sum(CASE WHEN f.situacao = 'apagada' THEN 1 ELSE 0 END)
             * coalesce(max(e.horas_escuridao_media), 11), 0) AS horas_luminaria_no_escuro_por_noite
FROM falhas f
LEFT JOIN escuro e ON e.subprefeitura = f.subprefeitura
GROUP BY f.subprefeitura
ORDER BY falhas_sem_nenhum_relato DESC;

-- COMMAND ----------

-- Q4 · Linha de base REAL: quanto a cidade demora hoje para resolver poste em mau estado
-- e fiação solta pelo SP156, por subprefeitura? (é o tempo que o produto quer reduzir)
SELECT f.subprefeitura,
       f.categoria,
       count(*)                                                     AS pedidos,
       sum(CASE WHEN f.status = 'Finalizada' THEN 1 ELSE 0 END)     AS finalizados,
       round(100 * sum(CASE WHEN f.status IN ('Aberto', 'Em Andamento') THEN 1 ELSE 0 END)
             / count(*), 1)                                         AS pct_em_aberto,
       percentile_approx(f.dias_atendimento, 0.5)                   AS mediana_dias,
       percentile_approx(f.dias_atendimento, 0.9)                   AS p90_dias
FROM workspace.gs.fato_ocorrencia f
WHERE f.fonte = 'sp156'
GROUP BY f.subprefeitura, f.categoria
HAVING count(*) >= 5
ORDER BY mediana_dias DESC
LIMIT 20;

-- COMMAND ----------

-- Q5 · IA responsável: quando a visão diz "luminária apagada", o robô confirma?
-- Cada foto é comparada com a medição LDR da MESMA luminária mais próxima no tempo
-- (até 3 dias antes ou depois): comparar com a última medição puniria a visão por um
-- poste que já foi consertado depois da foto.
WITH alegacoes AS (
  SELECT o.id_ocorrencia, o.id_luminaria,
         to_date(CAST(o.data_key AS STRING), 'yyyyMMdd') AS dia_foto,
         CASE WHEN o.confianca_visao >= 0.9 THEN '0.90+'
              WHEN o.confianca_visao >= 0.8 THEN '0.80-0.89'
              WHEN o.confianca_visao >= 0.6 THEN '0.60-0.79'
              ELSE '<0.60' END AS faixa_confianca
  FROM workspace.gs.fato_ocorrencia o
  WHERE o.fonte = 'app' AND o.categoria = 'luminaria_apagada' AND o.id_luminaria IS NOT NULL),
pares AS (
  SELECT a.faixa_confianca, i.situacao,
         row_number() OVER (PARTITION BY a.id_ocorrencia
                            ORDER BY abs(datediff(to_date(i.data_hora), a.dia_foto)), i.data_hora) AS rn
  FROM alegacoes a
  JOIN workspace.gs.fato_inspecao i
    ON i.id_luminaria = a.id_luminaria
   AND abs(datediff(to_date(i.data_hora), a.dia_foto)) <= 3)
SELECT faixa_confianca,
       count(*)                                                                    AS alegacoes_com_medicao,
       sum(CASE WHEN situacao IN ('apagada', 'insuficiente') THEN 1 ELSE 0 END)    AS confirmadas_pelo_robo,
       round(100 * sum(CASE WHEN situacao IN ('apagada', 'insuficiente') THEN 1 ELSE 0 END)
             / count(*), 1)                                                        AS precisao_pct
FROM pares
WHERE rn = 1
GROUP BY faixa_confianca
ORDER BY faixa_confianca DESC;

-- COMMAND ----------

-- Q6 · IMPACTO: depois que o RPA abre o chamado, quanto tempo até o reparo,
-- e em quantos casos o robô confirmou a luz de volta na ronda seguinte?
-- A mediana dos concluídos sozinha engana no começo do piloto: só os reparos rápidos já
-- terminaram. Por isso a consulta mostra também há quantos dias os abertos estão esperando
-- (contado até o último evento registrado, para o resultado não mudar com o relógio).
WITH ref AS (SELECT max(coalesce(concluido_em, aberto_em)) AS agora FROM workspace.gs.fato_chamado)
SELECT subprefeitura,
       count(*)                                                                AS chamados,
       sum(CASE WHEN status = 'concluido' THEN 1 ELSE 0 END)                   AS concluidos,
       sum(CASE WHEN status IN ('aberto', 'em_atendimento') THEN 1 ELSE 0 END) AS em_aberto,
       round(percentile_approx(horas_ate_conclusao, 0.5) / 24, 1)              AS mediana_dias_ate_reparo,
       round(percentile_approx(horas_ate_conclusao, 0.9) / 24, 1)              AS p90_dias_ate_reparo,
       round(100 * sum(CASE WHEN reparo_confirmado_pelo_robo THEN 1 ELSE 0 END)
             / nullif(sum(CASE WHEN status = 'concluido' THEN 1 ELSE 0 END), 0), 1) AS pct_reparo_confirmado,
       round(percentile_approx(CASE WHEN status IN ('aberto', 'em_atendimento')
             THEN (unix_timestamp(ref.agora) - unix_timestamp(aberto_em)) / 86400 END, 0.5), 1)
                                                                               AS mediana_dias_esperando_abertos
FROM workspace.gs.fato_chamado CROSS JOIN ref
GROUP BY subprefeitura
ORDER BY mediana_dias_ate_reparo DESC;
