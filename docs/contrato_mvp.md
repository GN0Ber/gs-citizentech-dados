# Contrato de dados do MVP (LuzViva, nome provisório · iluminação pública)

Este arquivo é a fronteira entre o componente **Dados** e os demais. Quem produz um
formato daqui não muda campo sem avisar quem consome.

## 1. O que entra no pipeline (outros componentes → Dados)

Cada componente grava **um arquivo JSON por dia** em `chegada/<fonte>/<fonte>_AAAA-MM-DD.json`,
no envelope abaixo. Enquanto o componente não estiver pronto, o simulador do Airflow
gera o mesmo formato, então nada muda quando o dado real chegar.

```json
{"_coleta": {"fonte": "medicoes_robo", "data_coleta": "2026-11-04", "url": "robo://ROBO-01"},
 "dados": [ { ...um objeto por registro... } ]}
```

### 1.1 `medicoes_robo` (Maker Lab: robô de inspeção luminosa)

| campo | tipo | obrigatório | observação |
|---|---|---|---|
| id_medicao | string | sim | único; reenviar o mesmo id não duplica (MERGE) |
| robo_id | string | sim | ex.: `ROBO-01` |
| data_hora | string ISO 8601 local | sim | só vale entre 18h e 6h |
| lat, lon | número (WGS84) | sim | GPS do robô; sem fix = medição descartada |
| lux | número | sim | leitura do LDR convertida para lux, >= 0 |
| situacao_robo | string | não | classificação feita no próprio robô (TinyML) |
| placa | string | não | placa do ponto de teste; sem GPS, o pipeline liga a medição pela placa |
| sensor | string | não | ex.: `LDR-GL5528` |
| bateria_pct | inteiro | não | |
| area_piloto | string | sim | `SE`, `PINHEIROS` ou `CAMPO LIMPO` |

Regras que o dado precisa passar: lux >= 0, GPS dentro de SP, horário noturno e a até
**30 m** de uma luminária do cadastro ILUME. Limiares de situação: `< 1 lux` apagada,
`< 5 lux` insuficiente (calibrar com o LDR real).

### 1.2 `ocorrencias_app` (Computer Vision + app do cidadão)

| campo | tipo | obrigatório | observação |
|---|---|---|---|
| id_ocorrencia | string | sim | único |
| data_hora | string ISO 8601 | sim | |
| categoria | string | sim | `luminaria_apagada`, `luminaria_piscando`, `luminaria_acesa_de_dia`, `poste_danificado`, `fiacao_exposta` |
| origem | string | sim | `app_foto_cidadao` ou `camera_visao` |
| confianca_visao | número 0..1 | sim | saída do modelo de visão |
| modelo_visao | string | sim | ex.: `yolo-luminaria-v1` (versão do modelo) |
| foto_id | string | sim | id da imagem guardada pelo app (a foto não entra no lake) |
| lat, lon | número | sim | |
| area_piloto | string | sim | |
| placa | string | **fortemente recomendado** | placa ILUME lida na foto ou escolhida no mapa; ver "Por que a placa" abaixo |
| cidadao | objeto ou null | não | `{nome, email, telefone}`: só fica na Bronze; vira `cidadao_hash` na Prata |

### 1.3 `chamados_rpa` (RPA: robô que abre e acompanha chamados)

Um arquivo por dia com os chamados abertos ou alterados naquele dia. O mesmo `id_chamado` pode
aparecer em vários dias: a Prata guarda o último estado (MERGE pela chave).

| campo | tipo | obrigatório | observação |
|---|---|---|---|
| id_chamado | string | sim | único, ex.: `CH-20261110-IP0723371` |
| placa | string | sim | placa ILUME da luminária |
| area_piloto | string | sim | |
| prioridade | inteiro 0..100 | sim | a da fila no momento da abertura |
| situacao_detectada | string | sim | `apagada` ou `insuficiente` |
| status | string | sim | `aberto`, `em_atendimento`, `concluido`, `cancelado` |
| aberto_em, atualizado_em | ISO 8601 | sim | |
| concluido_em | ISO 8601 ou null | se concluído | não pode ser anterior à abertura |
| equipe | string | não | |

Com isso a Ouro calcula o tempo até o reparo (`fato_chamado`) e confere se o robô mediu luz
normal na luminária depois da conclusão (`reparo_confirmado_pelo_robo`).

### Por que a placa (achado do piloto)

Na zona de teste, a luminária mais próxima de outra fica a **12,7 m** (mediana) e 34% dos postes
têm um vizinho a menos de 10 m, abaixo do erro de um GPS de celular (5 a 15 m). Ligar foto ou
medição ao poste só pela coordenada troca o poste com frequência. Todo poste da ILUME tem a placa
pintada (ex.: `IP0723371`): o app deve pedir para enquadrar a placa (a visão lê) ou deixar o
cidadão tocar o poste no mapa, e o robô deve ler a etiqueta do ponto. O pipeline liga pela placa
e usa o GPS só como reserva (coluna `metodo_casamento` na Prata).

## 2. O que sai do pipeline (Dados → MVP, IA e Automação)

Três views na Ouro, exportadas como JSON pelo notebook `05_export_mvp.py`
(`{"view", "gerado_em", "linhas", "dados": [...]}`). Nenhuma expõe dado pessoal.

### `vw_mvp_mapa_luminarias` → tela do mapa (MVP)
`id_luminaria, placa, endereco, subprefeitura, lat, lon, ultima_medicao, ultimo_lux,
situacao (ok | insuficiente | apagada | sem_medicao), ocorrencias_app`

### `vw_mvp_fila_manutencao` → automação (RPA) abre e acompanha chamados
`id_luminaria, placa, endereco, subprefeitura, lat, lon, situacao, ultimo_lux,
ultima_medicao, ocorrencias_app, chamado_aberto, status_chamado, confianca_visao, prioridade (0..100)`

Prioridade = apagada 60 / insuficiente 30 + 10 por relato de cidadão (máx. 30) + 10 se a
visão também apontou com confiança >= 0,8. A placa (`IP0298167`) é o que identifica o
ponto para a equipe de campo da ILUME.

### `vw_mvp_painel_subprefeitura` → painel do gestor e pitch
`subprefeitura, luminarias_cadastradas, luminarias_inspecionadas, apagadas,
insuficientes, pct_com_falha, mediana_horas_ate_reparo, chamados_em_aberto`

## 3. Identificadores compartilhados

* **id_luminaria**: `cd_identificador_iluminacao_publica` do cadastro ILUME (GeoSampa). É a
  chave que liga robô, visão, chamado e mapa. Ninguém inventa id de luminária.
* **placa**: `cd_placa` (ex.: `IP0723371`). Há postes com mais de um ponto: 47.736 pontos e
  40.351 placas na área-piloto.
* **subprefeitura**: em maiúsculas sem acento, igual à coluna "Prefeitura Operacional" do SP156.
