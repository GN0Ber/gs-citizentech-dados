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
| cidadao | objeto ou null | não | `{nome, email, telefone}`: só fica na Bronze; vira `cidadao_hash` na Prata |

## 2. O que sai do pipeline (Dados → MVP, IA e Automação)

Três views na Ouro, exportadas como JSON pelo notebook `05_export_mvp.py`
(`{"view", "gerado_em", "linhas", "dados": [...]}`). Nenhuma expõe dado pessoal.

### `vw_mvp_mapa_luminarias` → tela do mapa (MVP)
`id_luminaria, placa, endereco, subprefeitura, lat, lon, ultima_medicao, ultimo_lux,
situacao (ok | insuficiente | apagada | sem_medicao), ocorrencias_app`

### `vw_mvp_fila_manutencao` → automação (RPA) abre e acompanha chamados
`id_luminaria, placa, endereco, subprefeitura, lat, lon, situacao, ultimo_lux,
ultima_medicao, ocorrencias_app, confianca_visao, prioridade (0..100)`

Prioridade = apagada 60 / insuficiente 30 + 10 por relato de cidadão (máx. 30) + 10 se a
visão também apontou com confiança >= 0,8. A placa (`IP0298167`) é o que identifica o
ponto para a equipe de campo da ILUME.

### `vw_mvp_painel_subprefeitura` → painel do gestor e pitch
`subprefeitura, luminarias_cadastradas, luminarias_inspecionadas, apagadas,
insuficientes, pct_com_falha`

## 3. Identificadores compartilhados

* **id_luminaria**: `cd_identificador_iluminacao_publica` do cadastro ILUME (GeoSampa). É a
  chave que liga robô, visão, chamado e mapa. Ninguém inventa id de luminária.
* **placa**: `cd_placa` (ex.: `IP0723371`). Há postes com mais de um ponto: 47.736 pontos e
  40.351 placas na área-piloto.
* **subprefeitura**: em maiúsculas sem acento, igual à coluna "Prefeitura Operacional" do SP156.
