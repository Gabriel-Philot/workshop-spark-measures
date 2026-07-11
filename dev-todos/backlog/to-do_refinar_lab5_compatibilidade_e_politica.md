# To-do: separar compatibilidade funcional e policy no Lab 5

## Contexto

O Lab 5 mostra que uma mudança pode estar funcionalmente correta e ainda ser
operacionalmente pior. A narrativa deve provar compatibilidade antes de aplicar
o runtime budget.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_5/guide_lab5.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_5/README.md`
- `src/apps/labs/lab_5/docs/stage_runtime_budget_guardrail_class_notes.md`
- `src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail.py`
- `src/apps/labs/lab_5/run_stage_runtime_budget_guardrail.sh`
- `src/apps/labs/lab_5/lab_5_utils/experiments.yaml`
- `src/apps/labs/lab_5/lab_5_utils/budget_rules.yaml`
- `src/apps/labs/lab_5/lab_5_utils/budget.py`
- runtime e transformations do Lab 5

## Enquadramento da aula

- **Pergunta norteadora:** uma implementação funcionalmente correta também cabe
  no orçamento operacional definido pela equipe?
- **Por que esta aula aparece agora:** o Lab 4 produziu uma hipótese; o Lab 5
  transforma StageMetrics em uma decisão de engenharia.
- **Resultado de aprendizagem:** separar equivalência de negócio, mudança
  relativa de custo e decisão de policy.
- **Modo de condução:** exercício principal guiado, ao vivo; evidência persistida
  e History opcionais.

## Alterações pedagógicas

1. Manter baseline e candidate como o centro da história.
2. Fazer a compatibilidade funcional anteceder qualquer leitura de budget.
3. Preservar regras YAML, decisões PASS/FAIL/WARNING_LOW_SIGNAL e o fato de que
   um FAIL didático retorna exit code zero.
4. Rotular deltas locais como exemplo validado, não SLA universal.
5. Explicar que Lab 5 confia na telemetria recebida; ele não valida o contrato
   dessa telemetria.
6. Manter MinIO, History e cleanup depois da aula principal.

## Checkpoints de raciocínio

### Compatibilidade funcional

- **Pergunta:** baseline e candidate entregam o mesmo resultado de negócio?
- **Hipótese:** as duas variantes são semanticamente equivalentes apesar do
  plano físico diferente.
- **Evidência:** schema, row count, receita dentro da tolerância e total de
  pedidos.
- **Conclusão:** somente após compatibilidade a comparação operacional é válida.
- **Limitação:** equivalência funcional não diz nada sobre custo ou performance.

### Runtime budget

- **Pergunta:** a candidate permanece dentro dos limites configurados?
- **Hipótese:** trabalho físico adicional aparece como crescimento relativo em
  runtime, shuffle, tasks, stages, GC ou spill.
- **Evidência:** StageMetrics de baseline/candidate, deltas com sinal,
  multiplicadores, failed rules e decisão final.
- **Conclusão:** a policy local pode rejeitar uma regressão mesmo quando o output
  é correto.
- **Limitação:** uma execução local de baixo sinal não é modelo universal de
  custo; por isso existe WARNING_LOW_SIGNAL.

## Ponte para a próxima aula

Finalizar com a pergunta: se a decisão depende dessas métricas, como saber se a
telemetria é confiável? Essa é a tese do Lab 6.

## Critérios de aceite

- Compatibilidade vem antes de policy.
- Os dois checkpoints estão separados.
- A diferença Lab 5 versus Lab 6 aparece de forma curta e inequívoca.
- Valores locais estão rotulados como exemplos.
- O guia termina com a pergunta que abre o Lab 6.

## Validação e gate

1. Conferir checks de compatibilidade, métricas, rules, decisions, markers e
   paths contra código e YAML.
2. Conferir links e headings.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab5): separar compatibilidade funcional e policy
```
