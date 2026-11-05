# Dados simulados: o que, por que e como (exigência do briefing)

**Por que simular.** O robô do Maker Lab e o modelo de visão ficam prontos na mesma janela
de entrega do pipeline. Para o pipeline não esperar por eles, `dags/coletores/simulado.py`
gera as três fontes próprias (robô, visão e RPA) **no formato do contrato** (`docs/contrato_mvp.md`). Quando o
dado real chegar, ele cai na mesma pasta e nada muda dali em diante.

**Ancorado no dado real.** Fontes simuladas: `medicoes_robo`, `ocorrencias_app` e `chamados_rpa`. O robô simulado percorre luminárias que existem no cadastro
ILUME (arquivo de luminárias mais recente), com ruído de GPS de ~6 m. Por isso o DAG só
roda as fontes simuladas depois de `coletar_luminarias`.

| Parâmetro | Valor | Racional |
|---|---|---|
| Rota do robô | 150 a 260 luminárias por noite, uma área-piloto por noite em rodízio | um robô por noite, 40 s por ponto |
| Estado das luminárias | 6% apagadas, 10% insuficientes, fixo por placa | lâmpada queimada continua queimada até o reparo |
| Lux medido | apagada 0,1 a 1; insuficiente 1,5 a 4,5; LED ok 15 a 40 | ordem de grandeza de iluminância sob poste |
| Ocorrências | 20 a 45 por dia; 70% em luminárias que falham, 30% alarme falso | testa a precisão da visão (consulta Q5) |
| Sujeira proposital | ~6% (lux negativo, GPS sem fix, medição de dia, categoria nula, coordenada no Rio, confiança 1,7) + ~3% reenvios duplicados | faz as regras da Prata barrarem algo real |
| Determinismo | seed = data | rodar o DAG de novo gera o mesmo arquivo |
| Dados pessoais | nomes fictícios, e-mails `@exemplo.com` | nenhum dado pessoal real no repositório |

## Chamados do RPA (`chamados_rpa`)

Simula o robô de RPA do Caíque até ele estar integrado. A cada dia: abre um chamado para cada
luminária medida abaixo de 5 lux naquela noite (um por placa enquanto estiver aberto), passa para
`em_atendimento` no dia seguinte e conclui quando vence um prazo sorteado por placa (1 a 10
dias); cerca de 3% são cancelados ("endereço não localizado"). O robô de inspeção lê os chamados
já concluídos e passa a medir luz normal naquela placa, o que permite à Ouro confirmar o reparo
em campo. Determinístico por data, como as demais fontes simuladas.
