# To-do: refinar o diagnóstico com StageMetrics e TaskMetrics do Lab 1

## Contexto

O Lab 1 é a primeira história completa de diagnóstico. O guia deve destacar a
mudança consciente de granularidade: StageMetrics para enxergar pressão global
e TaskMetrics apenas quando a pergunta depende da distribuição entre tasks.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_1/guide_lab1.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_1/README.md`
- `src/apps/labs/lab_1/docs/random_task_outlier_class_notes.md`
- `src/apps/labs/lab_1/docs/task_metrics_native_api.md`
- `src/apps/labs/lab_1/lab_1a_global_sort_diagnosis.py`
- `src/apps/labs/lab_1/lab_1b_random_task_outlier_diagnosis.py`
- `src/apps/labs/lab_1/lab_1_utils/experiments.yaml`
- `src/apps/labs/lab_1/lab_1_utils/random_task_outlier_runtime.py`
- `src/apps/labs/lab_1/lab_1_utils/transformations.py`

## Enquadramento da aula

- **Pergunta norteadora:** quando os agregados de stage bastam e quando é
  necessário abrir a distribuição entre tasks?
- **Por que esta aula aparece agora:** o Lab 0 apresentou as ferramentas; o Lab
  1 aplica essas ferramentas em uma investigação controlada.
- **Resultado de aprendizagem:** iniciar com StageMetrics, reconhecer o limite
  do agregado e justificar a passagem para TaskMetrics.
- **Modo de condução:** aula principal, exercício guiado do aluno, executado ao
  vivo; inspeções adicionais permanecem opcionais.

## Alterações pedagógicas

1. Remover a repetição extensa do bootstrap e substituí-la por:
   - pré-requisito curto;
   - link preciso para as seções de preparação do Lab 0;
   - `make compose` quando apenas a stack estiver parada;
   - instrução clara para regenerar dados somente quando o storage estiver vazio.
2. Preservar os comandos específicos dos scripts 1A e 1B.
3. Manter a Spark History Server como evidência importante do 1A.
4. Deixar a segunda inspeção de History, MinIO e cleanup depois do núcleo como
   caminhos opcionais.
5. Explicar que a troca manual de `CONFIG_NAME` é um controle didático, não uma
   mudança de código do workload.
6. Integrar os checkpoints às seções de 1A e 1B, condensando as `Teacher notes`
   existentes em vez de repetir pergunta, métricas, outputs ou limitações.
7. Ao final da tarefa, toda a narrativa do guide, inclusive a prosa preexistente
   em inglês, deverá estar em português. Comandos, outputs literais, paths,
   markers, nomes de campos e termos técnicos permanecem intactos.

## Checkpoints de raciocínio

### 1A — global sort com StageMetrics

- **Pergunta:** o `orderBy` global introduziu movimento e custo operacional
  perceptíveis?
- **Hipótese:** a ordenação global cria `Exchange`, shuffle e trabalho adicional
  antes da escrita.
- **Evidência:** `explain`, Spark UI, shuffle read/write, número de tasks, stages
  e executor runtime coletados no modo observed-stage.
- **Conclusão:** o plano e as métricas mostram que a ordenação global envolveu
  `Exchange`, shuffle e trabalho distribuído nesta execução.
- **Limitação:** sem um baseline equivalente sem sort, o lab não quantifica o
  custo incremental causado pelo `orderBy`; os agregados também não identificam
  uma task específica.

Formulação técnica que o guide deverá preservar em português:

```text
O plano e as métricas mostram que a ordenação global envolveu Exchange, shuffle
e trabalho distribuído nesta execução. Sem um baseline equivalente sem sort, o
lab não quantifica o custo incremental causado pela ordenação.
```

### 1B — task outlier e validação do ajuste

- **Pergunta:** o tempo do stage está distribuído ou concentrado em poucas tasks?
- **Hipótese:** StageMetrics mostra o sintoma agregado, enquanto TaskMetrics
  deixa o outlier visível na distribuição.
- **Evidência:** percentis, máximo, pior task, executor runtime agregado e
  comparação com a variante fixed no bloco terminal.
- **Conclusão:** TaskMetrics é justificável quando a decisão depende da forma da
  distribuição.
- **Limitação:** o outlier é controlado para fins didáticos e uma melhora em uma
  task não garante redução proporcional do tempo total.

## Ponte para a próxima aula

Conectar o uso disciplinado de granularidade do Lab 1 aos quatro exercícios de
leitura de relações métricas do Lab 2.

## Critérios de aceite

- O guia isolado continua utilizável por meio de um pré-requisito curto e link
  correto ao Lab 0.
- 1A e 1B possuem checkpoints independentes.
- A ordem StageMetrics → TaskMetrics está explícita.
- Spark History do 1A permanece ligada à evidência central.
- Inspeções e cleanup não interrompem a sequência principal.
- O guia termina com a ponte para o Lab 2.
- Prosa integralmente em português e interfaces técnicas intactas.
- Checkpoints concisos, integrados ao conteúdo existente e sem duplicação de
  métricas, expected outputs, comandos ou troubleshooting.
- Crescimento material do guide exige remoção, reorganização ou referência
  explícita ao detalhe preservado em uma class note.

## Validação e gate

1. Conferir `CONFIG_NAME`, comandos, markers e Gold paths contra código e YAML.
2. Conferir links relativos e headings.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário antes do commit.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab1): refinar progressão de StageMetrics para TaskMetrics
```
