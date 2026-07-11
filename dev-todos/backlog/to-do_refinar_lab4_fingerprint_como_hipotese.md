# To-do: apresentar o fingerprint do Lab 4 como hipótese operacional

## Contexto

O fingerprint organiza StageMetrics em um perfil útil, mas não deve ser vendido
como root-cause analysis automática ou classificação universal.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_4/guide_lab4.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_4/README.md`
- `src/apps/labs/lab_4/docs/stage_workload_fingerprint_class_notes.md`
- `src/apps/labs/lab_4/lab_4_stage_workload_fingerprint.py`
- `src/apps/labs/lab_4/run_stage_workload_fingerprint.sh`
- `src/apps/labs/lab_4/lab_4_utils/experiments.yaml`
- `src/apps/labs/lab_4/lab_4_utils/fingerprint_rules.yaml`
- `src/apps/labs/lab_4/lab_4_utils/fingerprint.py`

## Enquadramento da aula

- **Pergunta norteadora:** como transformar contadores agregados em uma primeira
  hipótese sobre o perfil operacional do workload?
- **Por que esta aula aparece agora:** o Lab 3 estabeleceu o custo da coleta; o
  Lab 4 explora o valor interpretativo da camada de stage.
- **Resultado de aprendizagem:** ligar perfil, flags, métricas brutas, ratios e
  próximo passo sem confundir interpretação com prova.
- **Modo de condução:** exercício principal guiado, executado ao vivo; inspeções
  de persistência e History opcionais.

## Alterações pedagógicas

1. Reduzir o pré-requisito ao link preciso para o Lab 0 e ao restart necessário.
2. Manter workload, runner e bloco terminal como fluxo principal.
3. Condensar fórmulas e calibração histórica no guia quando as class notes já
   guardam os detalhes.
4. Preservar profiles, flags, ratios, thresholds e paths existentes.
5. Explicar que `input_bytes` indisponível não é igual a zero real.
6. Manter persistência, MinIO, Spark History e cleanup como opcionais finais.
7. Integrar o checkpoint à leitura do bloco diagnóstico existente, sem repetir
   profiles, flags, métricas, ratios ou fórmulas detalhadas que já estejam nas
   class notes linkadas.
8. Ao final da tarefa, toda a narrativa do guide, inclusive a prosa preexistente
   em inglês, deverá estar em português. Comandos, outputs literais, paths,
   markers, nomes de campos e termos técnicos permanecem intactos.

## Checkpoint de raciocínio — fingerprint

- **Pergunta:** qual perfil é compatível com a relação observada entre input,
  shuffle, spill, GC, tasks e executor runtime?
- **Hipótese:** regras simples conseguem resumir o primeiro sinal operacional e
  indicar uma investigação seguinte.
- **Evidência:** profile, diagnostic flags, métricas brutas, disponibilidade dos
  campos, ratios normalizados e regra acionada.
- **Conclusão:** o fingerprint é uma hipótese explicável quando as flags podem
  ser reconstruídas a partir das métricas.
- **Limitação:** thresholds são locais; o fingerprint não localiza uma task, não
  lê o plano e não prova causa raiz.

## Ponte para a próxima aula

Encerrar mostrando que uma hipótese operacional pode ser transformada em uma
regra de promoção. O Lab 5 fará essa passagem para policy.

## Critérios de aceite

- O fingerprint aparece explicitamente como primeira hipótese.
- A interpretação segue profile → flags → métricas → ratios → próximo passo.
- Indisponibilidade e zero verdadeiro continuam distintos.
- Thresholds locais não são tratados como universais.
- O checkpoint é conciso, reutiliza o bloco terminal existente e não cria um
  segundo relatório.
- O guide permanece integralmente em português na narrativa pedagógica e não
  cresce materialmente sem compensação.
- O guia termina com a ponte para o Lab 5.

## Validação e gate

1. Conferir profiles, metrics, rules, markers e paths contra código e YAML.
2. Conferir links e headings.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab4): enquadrar fingerprint como hipótese operacional
```
