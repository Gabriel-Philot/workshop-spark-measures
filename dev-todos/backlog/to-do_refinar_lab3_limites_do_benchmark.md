# To-do: refinar as fronteiras experimentais do Lab 3

## Contexto

O Lab 3 possui uma demonstração curta adequada à aula e um benchmark repetido
adequado a um post-mortem. Misturar os dois pode fazer uma única execução parecer
evidência universal sobre overhead.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_3/guide_lab3.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_3/README.md`
- `src/apps/labs/lab_3/docs/observability_overhead_postmortem.md`
- `src/apps/labs/lab_3/lab_3_observability_overhead_benchmark.py`
- `src/apps/labs/lab_3/run_observability_overhead_benchmark.sh`
- `src/apps/labs/lab_3/lab_3_utils/experiments.yaml`
- `src/apps/labs/lab_3/lab_3_utils/overhead_runtime.py`

## Enquadramento da aula

- **Pergunta norteadora:** quanto custo observável os collectors adicionam e o
  que é necessário para comparar esse custo com disciplina?
- **Por que esta aula aparece agora:** Labs 1 e 2 usaram StageMetrics e
  TaskMetrics; agora a turma examina o custo dessa escolha.
- **Resultado de aprendizagem:** distinguir demonstração, benchmark, warmup,
  fronteiras temporais e ruído do ambiente.
- **Modo de condução:** demonstração curta principal, conduzida ao vivo pelo
  instrutor; evidência repetida principal para discussão, porém pré-computada;
  regeneração completa opcional.

## Alterações pedagógicas

1. Resumir o pré-requisito usando o Lab 0 como referência canônica.
2. Separar claramente:
   - execução curta: uma repetição e nenhum warmup;
   - post-mortem com evidência repetida;
   - regeneração com dez repetições e warmup como opcional.
3. Explicar que `workload_wall_ms` é a principal fronteira comparável e que
   `spark_submit_wall_ms` inclui custos externos ao workload.
4. Preservar os comandos, modes, paths, markers e relatório nativo opcional.
5. Colocar manual submit, relatório expandido, benchmark completo, History e
   cleanup depois do núcleo.

## Checkpoints de raciocínio

### Demonstração curta

- **Pergunta:** os três modes executam o mesmo workload e produzem metadados
  comparáveis?
- **Hipótese:** none, stage e task preservam o workload, mas adicionam fronteiras
  diferentes de coleta.
- **Evidência:** markers, output compatibility, `workload_wall_ms`, collector
  begin/end e aggregate time por mode.
- **Conclusão:** a demonstração comprova o mecanismo de medição e comparação.
- **Limitação:** uma repetição sem warmup não estabelece uma distribuição nem um
  overhead confiável.

### Post-mortem repetido

- **Pergunta:** após warmup e repetições, existe um sinal consistente de custo?
- **Hipótese:** TaskMetrics tende a custar mais porque coleta eventos por task,
  enquanto StageMetrics permanece a primeira camada mais leve.
- **Evidência:** comparação das distribuições persistidas, volume de tasks,
  boundaries e orientação oficial documentada.
- **Conclusão:** os dados locais podem sustentar a direção do tradeoff sem
  produzir um percentual universal.
- **Limitação:** Spark startup, Delta, MinIO, Docker, ordem das execuções e ruído
  local podem aproximar ou inverter resultados individuais.

## Ponte para a próxima aula

Conectar o custo de coletar StageMetrics ao Lab 4, que transforma esses
agregados em uma primeira hipótese operacional.

## Critérios de aceite

- Execução curta e benchmark repetido não aparecem como a mesma evidência.
- Warmup e fronteiras temporais estão explicados.
- Evidência local está rotulada como exemplo validado.
- Não existe promessa de overhead universal.
- Caminhos longos permanecem disponíveis, mas opcionais.
- O guia termina com a ponte para o Lab 4.

## Validação e gate

1. Conferir variáveis, repetitions, warmup, modes, markers e paths contra runner,
   app e YAML.
2. Conferir referências do post-mortem e links externos já existentes.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab3): refinar limites do benchmark de observabilidade
```
