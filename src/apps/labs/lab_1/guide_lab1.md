# Guia do Lab 1: diagnóstico de global sort e investigação de task outlier

Este é o roteiro de aula do Lab 1.

Fluxo:

```text
confirmar as fontes Bronze compartilhadas
  -> investigar um global sort com evidência nativa do Spark
  -> condensar o sinal com StageMetrics
  -> reconhecer o limite do agregado de stage
  -> abrir TaskMetrics para localizar o outlier
  -> validar a variante preparada como ajuste
```

## Enquadramento da aula

- **Pergunta norteadora:** quando os agregados de stage bastam e quando é
  necessário abrir a distribuição entre tasks?
- **Por que esta aula aparece agora:** o Lab 0 apresentou as fontes, as
  ferramentas e o contrato; o Lab 1 aplica essa base na primeira investigação
  controlada.
- **Resultado de aprendizagem:** começar com StageMetrics, reconhecer o limite
  do agregado e justificar a passagem para TaskMetrics.
- **Modo de condução:** aula principal, exercício guiado do aluno e executado ao
  vivo; inspeções adicionais permanecem opcionais.

Referências para a discussão:

- [Notas da investigação de task outlier](docs/random_task_outlier_class_notes.md)
- [API nativa de TaskMetrics](docs/task_metrics_native_api.md)

## 0. Confirme os pré-requisitos compartilhados

Execute os comandos a partir da raiz do repositório:

```bash
cd workshop-spark-measures
```

Se esta for a primeira execução do workshop, ou se images e dados do MinIO foram
removidos, siga as seções 1–5 do
[guia do Lab 0](../lab_0/guide_lab0.md) antes de continuar. O bootstrap completo
não é repetido aqui.

Se somente os containers foram parados e os dados continuam no MinIO, reinicie
a stack:

```bash
make compose
```

As fontes compartilhadas esperadas são:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

Se `make clean-data` foi executado, volte à geração de dados da seção 5 do Lab 0.
Não continue enquanto uma falha de ambiente puder ser confundida com o
comportamento do workload.

## 1. Entre na pasta do Lab 1

Os próximos comandos usam paths relativos ao repositório para o Docker Compose.

```bash
cd src/apps/labs/lab_1
```

Verificação opcional:

```bash
ls
```

Scripts esperados:

```text
lab_1a_global_sort_diagnosis.py
lab_1b_random_task_outlier_diagnosis.py
```

## 2. Execute 1A: diagnóstico do global sort

Abra:

```text
lab_1a_global_sort_diagnosis.py
```

Comece com a configuração `native`:

```python
CONFIG_NAME = "lab1-global-sort-diagnosis-native"
```

Execute:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1a_global_sort_diagnosis.py
```

No terminal, localize `SPARK_EXPLAIN`. Os operadores importantes são
`Exchange` e `Sort`: eles mostram que o ranking exige ordenação distribuída. O
plano é preciso, mas verboso para uma primeira leitura operacional.

Depois, altere somente `CONFIG_NAME`:

```python
CONFIG_NAME = "lab1-global-sort-diagnosis-observed-stage"
```

Execute novamente o mesmo submit. Procure por `SPARKMEASURE_ENABLED=true` e
`SPARKMEASURE_METRICS`. Leia `numStages`, `numTasks`, `executorRunTime`,
`shuffleBytesWritten`, `memoryBytesSpilled` e `diskBytesSpilled` quando esses
campos estiverem presentes no relatório.

Markers esperados:

```text
native run:
  LAB1_GLOBAL_SORT_NATIVE_OK

observed-stage run:
  LAB1_GLOBAL_SORT_SPARKMEASURE_STAGE_OK
  LAB1_GLOBAL_SORT_DIAGNOSIS_OK
```

Output Gold esperado:

```text
s3a://lakehouse/gold/lab1/top_sales_global_sort
```

### 2.1 Relacione o plano com o Spark History Server

Abra:

```text
http://127.0.0.1:28090
```

Procure pelas aplicações:

```text
workshop-lab1-global-sort-native
workshop-lab1-global-sort-observed-stage
```

Use as descrições com prefixo `LAB1` para separar o workload de trabalho interno
do framework ou Delta. Em `Stages`, inspecione duração, quantidade de tasks,
shuffle read/write e spill. Em `SQL / DataFrame`, conecte `Exchange` e `Sort` do
plano aos sinais registrados durante a execução.

### 2.2 Checkpoint de raciocínio — global sort com StageMetrics

- **Pergunta:** quais sinais operacionais aparecem quando essa execução realiza
  a ordenação global?
- **Hipótese:** o plano contém `Exchange` e `Sort`, enquanto a execução apresenta
  shuffle e trabalho distribuído visíveis em nível de stage.
- **Evidência:** `explain`, Spark UI, shuffle read/write, tasks, stages e
  executor runtime do modo `observed-stage`.
- **Conclusão:** o plano e as métricas mostram que a ordenação global envolveu
  `Exchange`, shuffle e trabalho distribuído nesta execução.
- **Limitação:** sem um baseline equivalente sem sort, o lab não quantifica o
  custo incremental causado pelo `orderBy`; o agregado também não identifica
  uma task específica.

## 3. Prepare 1B: escolha a configuração do task outlier

Abra:

```text
lab_1b_random_task_outlier_diagnosis.py
```

Altere somente `CONFIG_NAME` durante o exercício.

Visão agregada de stage:

```python
CONFIG_NAME = "lab1-random-task-outlier-stage"
```

Visão diagnóstica de task:

```python
CONFIG_NAME = "lab1-random-task-outlier-task"
```

Validação da variante `fixed`:

```python
CONFIG_NAME = "lab1-random-task-outlier-fixed-task"
```

A troca manual é intencional: o script permanece próximo do template e a aula
pode pausar entre os mesmos dados observados em granularidades diferentes.
TaskMetrics não é apresentado como coletor padrão, mas como o microscópio
usado quando o agregado de stage não responde qual task forma a long tail.

## 4. Execute 1B: diagnóstico do task outlier

Depois de selecionar cada `CONFIG_NAME`, use o mesmo comando:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_1/lab_1b_random_task_outlier_diagnosis.py
```

Sequência recomendada:

1. Execute `lab1-random-task-outlier-stage` e leia o agregado.
2. Explique o que esse agregado mostra e o que ele esconde.
3. Troque para `lab1-random-task-outlier-task`.
4. Inspecione o box `LAB1_TASK_OUTLIER` no terminal.
5. Troque para `lab1-random-task-outlier-fixed-task`.
6. Compare a pior task e o executor runtime agregado.

Markers esperados por modo:

```text
LAB1_RANDOM_TASK_OUTLIER_STAGE_OK
LAB1_RANDOM_TASK_OUTLIER_TASK_OK
LAB1_RANDOM_TASK_OUTLIER_FIXED_TASK_OK
```

Marker diagnóstico de task esperado:

```text
LAB1_TASK_OUTLIER rank=1 stageId=... taskIndex=... executorRunTime=...
```

Outputs Gold esperados:

```text
s3a://lakehouse/gold/lab1/random_task_outlier/problematic
s3a://lakehouse/gold/lab1/random_task_outlier/fixed
```

O relatório completo do TaskMetrics é impresso primeiro. O box didático não é
uma segunda medição: ele projeta as tasks já coletadas por
`create_taskmetrics_DF(...)` e ordena as principais por `executorRunTime`.

### 4.1 Fronteiras válidas de comparação

As execuções com StageMetrics e TaskMetrics respondem a perguntas diagnósticas
diferentes. A diferença de runtime entre esses dois modos não deve ser
interpretada aqui como melhora ou regressão do workload, porque a granularidade
da coleta também mudou. O custo dos coletores será investigado no Lab 3.

A comparação controlada desta parte ocorre entre:

- `lab1-random-task-outlier-task`;
- `lab1-random-task-outlier-fixed-task`.

Essas duas variantes usam TaskMetrics. Isso permite observar a mudança do
workload mantendo a granularidade do coletor.

### 4.2 Checkpoint de raciocínio — task outlier e validação do ajuste

- **Pergunta:** o custo do stage está distribuído ou concentrado em poucas tasks?
- **Hipótese:** StageMetrics mostra o sintoma agregado, enquanto TaskMetrics
  deixa a long tail visível na distribuição.
- **Evidência:** distribuição do relatório nativo, diferença entre a maior
  `executorRunTime` e as demais tasks exibidas no top 5, pior task identificada
  no box e comparação das mesmas relações entre `problematic` e `fixed`.
- **Conclusão:** TaskMetrics é justificável quando a decisão depende de
  concentração ou dispersão entre tasks. A maior task só deve ser tratada como
  outlier quando se distancia materialmente da distribuição observada.
- **Limitação:** o outlier é controlado para fins didáticos; melhorar a pior task
  não garante redução proporcional do tempo total nem define uma regra universal
  de tuning.

Detalhes sobre o relatório e sobre casos em que `duration` permanece semelhante
estão nas [notas do Lab 1B](docs/random_task_outlier_class_notes.md).

## Material operacional opcional

### 5. Inspecione 1B no Spark History Server

Abra:

```text
http://127.0.0.1:28090
```

Procure pelas aplicações correspondentes às configurações selecionadas:

```text
workshop-lab1-random-task-outlier-stage
workshop-lab1-random-task-outlier-task
workshop-lab1-random-task-outlier-fixed-task
```

Use a UI como evidência de apoio: confirme application names, formato dos
stages e quantidade de tasks. O relatório de task no terminal permanece o
artefato principal desta parte. Se UI e sparkMeasure parecerem discordar,
reconcilie primeiro as fronteiras medidas.

### 6. Inspecione o MinIO

Abra:

```text
http://127.0.0.1:29011
```

Paths úteis:

```text
lakehouse/bronze/retail/vendors
lakehouse/bronze/retail/products
lakehouse/bronze/retail/customers
lakehouse/bronze/retail/sales
lakehouse/gold/lab1/top_sales_global_sort
lakehouse/gold/lab1/random_task_outlier/problematic
lakehouse/gold/lab1/random_task_outlier/fixed
observability/event-logs
```

O Lab 1 mantém a persistência de métricas do sparkMeasure desabilitada. A
evidência fica no relatório do terminal e no Spark History Server, enquanto os
outputs de negócio são persistidos em `lakehouse/gold/lab1`.

Credenciais padrão do MinIO:

```text
user:     sparkworkshop
password: sparkworkshop123
```

### 7. Limpeza opcional depois da aula

Volte à raiz do repositório:

```bash
cd ../../../..
```

Pare os containers:

```bash
make down
```

Remova os volumes e dados locais do projeto somente quando desejar uma nova
execução a partir de storage vazio:

```bash
make clean-data
```

## Ponte para a próxima aula

O Lab 1 mostrou que a granularidade deve responder à pergunta diagnóstica. O Lab
2 aplica essa disciplina em quatro mini-experimentos: dois resolvidos com
StageMetrics e dois que dependem da distribuição exposta por TaskMetrics.
