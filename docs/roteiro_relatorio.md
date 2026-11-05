# Roteiro do relatório em PDF (na ordem exigida pelo professor)

Fora da ordem tira até 1 ponto; print sem legenda também. Todo print com legenda curta.

| # | Seção | O que colocar | De onde tirar |
|---|---|---|---|
| 01 | Integrantes | nome completo e RM de todos | |
| 02 | Produto e perguntas | 1 frase do produto + as 5 perguntas do README | README |
| 03 | Diagrama do fluxo | fonte → Airflow → chegada → Volume → Bronze → Prata → Ouro → MVP | diagrama do README redesenhado |
| 04 | Tabela de fontes | origem, formato, frequência | `docs/fontes.md` |
| 05 | Requisitos 01 a 09 | um print + 2 linhas por requisito | mapa no README |
| 06 | Airflow | prints das visões **Graph** e **Grid** com execuções concluídas (e uma falha com retry, se tiver) | UI do Airflow |
| 07 | Contagens da Bronze | antes/depois do 2º lote e da 2ª execução sem lote novo (`linhas_novas = 0`) | `SELECT * FROM gs.controle_cargas` |
| 08 | 5 consultas SQL | resultado + interpretação de 2 a 3 linhas cada | `04_consultas.sql` |
| 09 | Conclusão e link | o que funcionou, limites (dados próprios simulados até a integração), próximo passo | |

## Frases de interpretação (ajustar aos números finais)

* **Q1**: no ensaio local, 13% a 17% das luminárias inspecionadas tinham falha (Pinheiros 13,5%, Sé 16,7%).
* **Q3 (impacto)**: praticamente todas as falhas achadas pelo robô não tinham relato de cidadão.
  É o argumento central: hoje a cidade só sabe da lâmpada apagada quando alguém reclama.
* **Q4 (dado real)**: de abril a setembro de 2026 o SP156 recebeu ~1.800 pedidos de poste em
  mau estado e fiação solta; 87% dos de fiação solta seguiam em aberto e os finalizados
  levaram mediana de 19 dias. Atenção: "finalizada" no SP156 pode significar encaminhada à
  concessionária, não consertada.
* **Q5**: com poucos dias de dado a amostra é pequena; com uma semana de coleta a precisão
  por faixa de confiança fica mostrável.
