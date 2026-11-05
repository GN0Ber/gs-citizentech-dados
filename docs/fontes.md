# Fontes de dados (requisito 01 e item 04 do relatório)

Conferidas contra os portais em 05/11/2026.

| Fonte | Origem | Tipo | Formato | Frequência | Volume no piloto | Licença |
|---|---|---|---|---|---|---|
| luminarias | GeoSampa WFS, camada `geoportal:iluminacao_publica` (SMUL/ILUME) | externa real | GeoJSON via WFS, paginado | diária (cadastro atualizado pela ILUME; carga de 21/10/2026) | 47.736 pontos em 3 áreas (cidade: 662.945) | dado aberto da Prefeitura de SP |
| sp156 | Portal de Dados Abertos SP, dataset `dados-do-sp156` (CKAN) | externa real | CSV Latin-1 `;`, ~100 MB por trimestre | diária (arquivo novo a cada trimestre; cache evita novo download) | ~1.800 pedidos de poste/fiação em 6 meses, cidade toda | CC0 |
| clima | Open-Meteo `/v1/forecast` | externa real | JSON | diária | 3 pontos | CC BY 4.0 |
| medicoes_robo | robô LDR (Maker Lab) | própria (simulada até a integração) | JSON | diária (rota noturna) | 150 a 260 medições/noite | própria |
| ocorrencias_app | app + visão computacional | própria (simulada até a integração) | JSON | diária | 20 a 45/dia | própria |

## Achados que viram texto no relatório

1. **Iluminação não aparece no SP156 aberto.** No 2º TRI 2026 só 1 de 463.686 pedidos fala
   em iluminação; lâmpada apagada segue pelo canal da ILUME e não é publicada. A cidade
   tem cadastro de 663 mil pontos, mas nenhum dado aberto de quais estão apagados. É a
   lacuna que o produto preenche.
2. **Formato de data muda entre arquivos do SP156.** 2º TRI publica `01/04/2026` (dia/mês),
   3º TRI publica `07/14/2026` (mês/dia). A Prata detecta o padrão por arquivo.
3. **SP156 não tem protocolo.** A chave é um hash dos campos estáveis + nº da ocorrência;
   status e finalização ficam fora da chave para o MERGE atualizar o pedido.
4. **"Prefeitura Operacional" mistura órgãos**: aparece `SUPERINTENDENCIA DAS USINAS DE
   ASFALTO` como se fosse subprefeitura.
5. **Cadastro ILUME**: 100% LED na área-piloto (a modernização já ocorreu, então a
   falha agora é de manutenção, não de tecnologia) e mais pontos que placas (postes com
   dois braços).
6. **Open-Meteo**: o `/archive` do exemplo do professor chega com ~5 dias de atraso, por
   isso usamos `/forecast` com `start_date/end_date`. A cota diária gratuita é por IP.

## Fontes avaliadas e descartadas

* **Infosiga SP (acidentes por turno)**: seria ótima para cruzar escuro × atropelamento, mas
  desde a versão 3.0 o download (`/rest/painel/download/file/dados_infosiga.zip`) responde
  HTTP 401. Pode entrar como download manual, fora do Airflow.
* **ANEEL / tarifas**: só fazia sentido no tema de energia.
