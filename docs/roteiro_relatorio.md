# Roteiro do relatório em PDF (na ordem exigida pelo professor)

Fora da ordem tira até 1 ponto; print sem legenda também. Todo print com legenda curta.

| # | Seção | O que colocar | De onde tirar |
|---|---|---|---|
| 01 | Integrantes | Caíque Walter Silva (RM550693), Guilherme Nobre Bernardo (RM98604), Guilherme Monteiro Espim (RM99499), João Paulo Fonseca Zamperlini (RM99279), Matheus José de Lima Costa (RM551157) | README |
| 02 | Produto e perguntas | 1 frase do produto + as 5 perguntas do README | README |
| 03 | Diagrama do fluxo | fonte → Airflow → chegada → Volume → Bronze → Prata → Ouro → MVP | diagrama do README redesenhado |
| 04 | Tabela de fontes | origem, formato, frequência | `docs/fontes.md` |
| 05 | Requisitos 01 a 09 | um print + 2 linhas por requisito | mapa no README |
| 06 | Airflow | prints das visões **Graph** e **Grid** com execuções concluídas (e uma falha com retry, se tiver) | UI do Airflow |
| 07 | Contagens da Bronze | antes/depois do 2º lote e da 2ª execução sem lote novo (`linhas_novas = 0`) | `SELECT * FROM gs.controle_cargas` |
| 08 | 5 consultas SQL | resultado + interpretação de 2 a 3 linhas cada | `04_consultas.sql` |
| 09 | Conclusão e link | o que funcionou, limites (dados próprios simulados até a integração), próximo passo | |

## Calendário até a entrega (19/11)

| Quando | O quê |
|---|---|
| 05/11 | Lote 1 (01–03) e lote 2 (04–05) carregados; prints de Volume, Bronze (incremental e `novas = 0`), Prata, Q1, Q2 e Q4 **já valem** |
| 06 a 15/11 | Airflow ligado ao menos uma vez a cada 2 ou 3 dias; o `catchup` coleta os dias que faltarem |
| 16/11 | Subir os arquivos novos no Volume; rodar 01 → 04; refazer os prints de **Q3, Q5 e Q6** (precisam de ~2 semanas de dado) e do Grid do Airflow |
| 17–18/11 | Montar o PDF nesta ordem; revisão cruzada com o grupo |

## Frases de interpretação (resultado de 05/11, 5 dias; ajustar aos números finais)

* **Q1**: 15,9% a 18,2% das luminárias inspecionadas com falha; Campo Limpo é a pior
  (33 apagadas e 40 insuficientes). "Uma em cada seis luminárias não ilumina o mínimo."
* **Q2**: a fila soma evidências: apagada vale 60, relato de cidadão +10, visão com confiança
  ≥ 0,8 +10. O topo (prioridade 80) tem as três; a placa vai pronta para a equipe da ILUME.
* **Q3 (impacto)**: ~96% das falhas achadas pelo robô não tinham relato de cidadão. Ressalva
  obrigatória: o volume de fotos é simulado; o número real depende da adesão ao app.
* **Q4 (dado real, o mais forte do trabalho)**: no SP156, pedidos de poste em mau estado e
  fiação solta levam de 8 a 45 dias (mediana) conforme a subprefeitura, e várias têm mais de
  80% em aberto. "Hoje a cidade leva semanas; o robô detecta em até 3 noites." Atenção:
  "finalizada" no SP156 pode significar encaminhada à concessionária, não consertada.
* **Q5 (IA responsável)**: com 5 dias só havia 5 alegações comparáveis; não concluir nada com
  isso. Na simulação de 10 noites a precisão subiu com a confiança (≈66% → 87%). A frase final
  vem do print de 16/11.
* **Q6**: com poucos dias, a mediana dos concluídos mostra só os reparos rápidos (viés de
  sobrevivência); por isso a consulta traz também `mediana_dias_esperando_abertos`. O robô
  volta a cada área a cada 3 noites, então a confirmação do reparo só aparece com mais dias.
* **Bronze × Prata (item 07)**: a Bronze do SP156 tem 5.565 linhas (5 cópias do recorte
  trimestral, guardado como chegou); a Prata tem 1.113 pedidos únicos. O MERGE pela chave hash
  é o que garante que reprocessar não duplica. Luminárias reenviadas (01–03) não entraram de
  novo: o checkpoint do Auto Loader reconhece arquivo já lido.
* **Qualidade (Prata)**: das 1.077 medições, as regras barraram 28 fora do horário noturno,
  26 com lux negativo, 14 sem GPS e 14 longe de luminária; ocorrências: 3 sem categoria,
  3 com confiança fora de 0..1, 2 fora do município. Vão para a quarentena, não somem.
