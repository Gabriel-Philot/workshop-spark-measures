# To-do: refinar a camada de confiança do Lab 6

## Contexto

O Lab 6 valida a telemetria como data product operacional. Ele não repete o gate
do Lab 5 e não tenta provar a precisão interna do sparkMeasure.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_6/guide_lab6.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_6/README.md`
- `src/apps/labs/lab_6/docs/stage_metrics_contract_gate_class_notes.md`
- `src/apps/labs/lab_6/lab_6_stage_metrics_contract_gate.py`
- `src/apps/labs/lab_6/run_stage_metrics_contract_gate.sh`
- `src/apps/labs/lab_6/lab_6_utils/experiments.yaml`
- `src/apps/labs/lab_6/lab_6_utils/contract_rules.yaml`
- `src/apps/labs/lab_6/lab_6_utils/contract.py`
- runtime e transformations do Lab 6

## Enquadramento da aula

- **Pergunta norteadora:** as métricas coletadas possuem schema, semântica e
  identidade confiáveis para alimentar automação?
- **Por que esta aula aparece agora:** o Lab 5 mostrou policy primeiro; o Lab 6
  volta um passo e questiona a confiança na evidência.
- **Resultado de aprendizagem:** validar schema, valores, correlação e
  disponibilidade antes de usar telemetria em decisões.
- **Modo de condução:** dois experimentos principais ao vivo — cenário limpo e
  falha controlada — conduzidos pelo instrutor com participação dos alunos.

## Alterações pedagógicas

1. Incorporar o conteúdo do todo anterior da ponte Lab 5→6.
2. Explicar as duas ordens:
   - workshop: policy → pergunta sobre confiança;
   - produção: collect → contract → policy → promotion decision.
3. Explicar a origem e o propósito de `run_id`, `application_id`, `app_name`,
   `lab_id`, `workload_name`, `workload_variant`, `collector_name`,
   `metric_scope`, `contract_version` e `created_at`.
4. Registrar que `application_id` permite correlação com History, mas não faz
   parte da uniqueness key atual.
5. Preservar a diferença entre zero real e placeholder seguro quando
   `*_available=false`.
6. Relacionar validation metadata a dashboards, alerts, runtime budgets, PR
   review e drift monitoring.
7. Manter persistência, MinIO, History e cleanup como opcionais finais.
8. Integrar os checkpoints ao bloco de contrato e à tabela de consumers já
   existentes, sem repetir columns, availability fields, rule results,
   expected outputs ou troubleshooting.
9. Manter toda narrativa criada ou editada no guide em português, preservando
   comandos, paths, markers, nomes de campos e termos técnicos.

## Checkpoints de raciocínio

### Cenário limpo

- **Pergunta:** a linha StageMetrics satisfaz as três camadas do contrato?
- **Hipótese:** uma execução válida produz schema estável, valores semanticamente
  válidos e identidade suficiente para correlação.
- **Evidência:** resultados SCHEMA, SEMANTIC e CORRELATION, disponibilidade das
  métricas opcionais, rule counts e decisão PASS.
- **Conclusão:** a linha está apta para os consumidores descritos pelo contrato.
- **Limitação:** PASS não prova que o collector mediu o mundo físico com precisão
  absoluta; prova conformidade com as regras configuradas.

### Falha controlada

- **Pergunta:** o contrato rejeita registros que poderiam corromper automação?
- **Hipótese:** null identity, zero stage/task, contador negativo, scope inválido
  e duplicidade geram falhas acionáveis.
- **Evidência:** rule IDs, rule type, severity, failed count, sample failed keys,
  recommendation e decisão FAIL.
- **Conclusão:** o gate impede que telemetria inválida pareça evidência confiável.
- **Limitação:** os registros inválidos são sintéticos e o lab não é um framework
  completo de data quality.

## Ponte para a próxima aula

Conectar métricas confiáveis e correlacionáveis ao Lab 7, que passa a analisá-las
por data de negócio e ao longo de várias aplicações.

## Critérios de aceite

- Lab 5 e Lab 6 possuem responsabilidades distintas.
- As ordens do workshop e de produção estão explícitas.
- Cenário limpo e falha injetada possuem checkpoints próprios.
- Zero e indisponibilidade permanecem diferentes.
- Metadata e consumidores estão conectados sem alterar schemas.
- Checkpoints concisos reutilizam os resultados por layer e não criam um segundo
  resumo do contrato.
- O guide permanece integralmente em português na narrativa pedagógica e não
  cresce materialmente sem reorganização de conteúdo repetido.
- O guia termina com a ponte para o Lab 7.

## Validação e gate

1. Conferir columns, availability fields, rules, uniqueness key, markers e paths
   contra código e YAML.
2. Conferir links e headings.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab6): refinar contrato de confiança das métricas
```
