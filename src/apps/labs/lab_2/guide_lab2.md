# Guia do Lab 2: quatro exercícios de leitura de métricas

Este é o roteiro de aula do Lab 2.

Fluxo:

```text
confirmar as fontes Bronze compartilhadas
  -> diagnosticar movimento e custo de shuffle em nível de stage
  -> distinguir shuffle, spill e evidência de GC
  -> usar TaskMetrics para o extremo superior da distribuição
  -> usar TaskMetrics para o extremo inferior da distribuição
```

## Enquadramento da aula

- **Pergunta norteadora:** como transformar relações numéricas da Spark UI em
  perguntas diagnósticas reproduzíveis com sparkMeasure?
- **Por que esta aula aparece agora:** o Lab 1 ensinou a escolher a granularidade;
  o Lab 2 exercita essa escolha em quatro sintomas diferentes.
- **Resultado de aprendizagem:** distinguir movimento excessivo, pressão
  agregada, outlier no extremo superior e partitions vazias no extremo inferior.
- **Modo de condução:** quatro exercícios principais do aluno, executados ao vivo
  e discutidos pelo instrutor; questões de exame funcionam como contexto.

As perguntas foram selecionadas de simulados do Databricks Data Engineer
Professional para criar conhecimento proximal, não para transformar o workshop
em uma preparação completa para certificação. Leia os enunciados e respostas em:

[Questões selecionadas do Lab 2](docs/exam_questions.md)

O objetivo não é memorizar um threshold. É ler uma relação entre métricas,
conectá-la ao workload e escolher a granularidade adequada. sparkMeasure
complementa a Spark UI; não substitui plano, stage ou executor inspection.

## 0. Confirme os pré-requisitos compartilhados

Execute os comandos a partir da raiz do repositório:

```bash
cd workshop-spark-measures
```

Se esta for a primeira execução, ou se images e dados do MinIO foram removidos,
siga as seções 1–5 do [guia do Lab 0](../lab_0/guide_lab0.md). O bootstrap
completo não é repetido aqui.

Se somente os containers foram parados e os dados permanecem no MinIO:

```bash
make compose
```

As fontes Bronze esperadas são:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

Se `make clean-data` foi executado, volte à geração de dados da seção 5 do Lab 0.
Não diagnostique um workload enquanto o ambiente ainda puder ser a causa da
falha.

## 1. Entre na pasta do Lab 2

```bash
cd src/apps/labs/lab_2
```

Verificação opcional:

```bash
ls
```

Scripts esperados:

```text
lab_2a_shuffle_aggregation_diagnosis.py
lab_2b_stage_metrics_interpretation_drill.py
lab_2c_task_duration_skew_diagnosis.py
lab_2d_empty_partitions_diagnosis.py
```

Progressão da aula:

| Exercício | Coletor | Pergunta diagnóstica |
| --- | --- | --- |
| 2A | StageMetrics | Uma distribuição desnecessária está aumentando shuffle e tasks? |
| 2B | StageMetrics | Os agregados sustentam shuffle, spill ou pressão de GC? |
| 2C | TaskMetrics | Uma task no extremo superior é muito maior que a task típica? |
| 2D | TaskMetrics | Existem tasks vazias ou quase vazias no extremo inferior? |

StageMetrics permanece como primeira camada. TaskMetrics aparece somente quando
a pergunta depende da distribuição dentro de um stage.

## 2. Lab 2A: distribuição antes da agregação

Leia primeiro a questão de origem:

[Lab 2A: reduzindo shuffle durante uma agregação](docs/exam_questions.md)

### 2.1 Execute a variante baseline

Abra:

```text
lab_2a_shuffle_aggregation_diagnosis.py
```

Mantenha o controle didático em:

```python
CONFIG_NAME = "lab2-shuffle-aggregation-baseline"
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
  /opt/spark/src/apps/labs/lab_2/lab_2a_shuffle_aggregation_diagnosis.py
```

A sequência física intencional é:

```text
sales + vendors
  -> repartition round-robin(1024)
  -> selecionar o fato regional
  -> groupBy(vendor_region, sale_year_month)
  -> escrita Delta em Gold
```

A repartição round-robin não está alinhada às chaves da agregação. Ela movimenta
linhas largas e cria mais tasks do que o cluster local consegue aproveitar.

Leia no relatório nativo:

```text
numStages
numTasks
executorRunTime
shuffleBytesWritten
shuffleTotalBytesRead
memoryBytesSpilled
diskBytesSpilled
recordsRead
recordsWritten
```

Marker esperado:

```text
LAB2_SHUFFLE_AGGREGATION_BASELINE_OK
```

Output esperado:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/baseline
```

Formato do output de negócio:

```text
vendor_region
sale_year_month
sale_count
total_quantity
gross_sales_amount
average_sale_amount
```

### 2.2 Execute a variante optimized

Altere somente:

```python
CONFIG_NAME = "lab2-shuffle-aggregation-optimized"
```

Execute novamente o mesmo submit.

A variante:

```text
sales + vendors
  -> reduzir o fato da agregação
  -> repartition por vendor_region e sale_year_month
  -> groupBy pelas mesmas chaves
  -> escrita Delta em Gold
```

Marker esperado:

```text
LAB2_SHUFFLE_AGGREGATION_OPTIMIZED_OK
```

Output esperado:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/optimized
```

### 2.3 Compare a evidência

Exemplo local validado com `SCALE=xs`:

| Variante | Stages | Tasks | Executor runtime | Shuffle written | Shuffle read |
| --- | ---: | ---: | ---: | ---: | ---: |
| baseline | 14 | 1,296 | ~65s | ~69.1 MiB | ~69.1 MiB |
| optimized | 13 | 301 | ~43s | ~50.9 MiB | ~50.9 MiB |

A variante baseline reduziu aproximadamente cinco milhões de linhas de entrada
para quatro linhas de saída. A evidência compacta incluiu:

```text
recordsRead=5,000,367
recordsWritten=4
memoryBytesSpilled=0
diskBytesSpilled=0
```

#### Checkpoint de raciocínio — distribuição desnecessária

- **Pergunta:** a repartição não alinhada aumenta shuffle e volume de tasks?
- **Hipótese:** a variante `baseline` movimenta linhas largas e cria trabalho
  desnecessário antes de agregar.
- **Evidência:** `baseline` versus `optimized` em tasks, stages, shuffle
  read/write e executor runtime. As variantes foram construídas para produzir o
  mesmo grain e schema, mas o lab não compara automaticamente os dois datasets.
- **Conclusão:** o exemplo local mostra menor trabalho operacional na variante
  `optimized`; essa conclusão de otimização pressupõe que a equivalência
  funcional seja validada separadamente.
- **Limitação:** o marker confirma execução e escrita bem-sucedidas, não
  equivalência completa entre os outputs. O experimento também não afirma que
  toda repartição é ruim ou que shuffle deve chegar a zero.

## 3. Lab 2B: interpretação de shuffle, spill e GC

Leia as duas questões de origem separadamente:

- [Lab 2B: interpretando GC alto](docs/exam_questions.md)
- [Lab 2B: interpretando shuffle spill](docs/exam_questions.md)

A primeira questão usa 25% como sinal teórico de GC relevante. A segunda discute
memory e disk spill. O workload local conecta as duas relações, mas a evidência
validada reproduziu claramente spill e manteve a razão de GC perto de 3.4%.
Não apresente as duas questões como um único enunciado nem diagnostique o que a
execução não demonstrou.

### 3.1 Execute a variante pressure

Abra:

```text
lab_2b_stage_metrics_interpretation_drill.py
```

Comece com:

```python
CONFIG_NAME = "lab2-stage-metrics-drill-pressure"
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
  /opt/spark/src/apps/labs/lab_2/lab_2b_stage_metrics_interpretation_drill.py
```

A variante carrega payloads largos por uma repartição round-robin antes de
reduzir o fato:

```text
sales + vendors + products
  -> manter colunas largas de payload
  -> repartition round-robin(512)
  -> reduzir o fato
  -> groupBy(region, category, month)
  -> escrita Delta em Gold
```

Leia diretamente do relatório agregado:

```text
numStages
numTasks
executorRunTime
jvmGCTime
memoryBytesSpilled
diskBytesSpilled
shuffleTotalBytesRead
shuffleBytesWritten
```

Relações de evidência:

| Observação | Interpretação permitida |
| --- | --- |
| shuffle bytes > 0 | uma operação wide movimentou dados entre stages |
| memory e disk spill > 0 | houve spill observado nesta execução |
| spill = 0 | não houve spill observado; isso não elimina outros gargalos |
| `jvmGCTime / executorRunTime` material | pressão de memória pode estar presente |
| razão de GC baixa e spill zero | investigue movimento e partitioning antes |
| uma task lenta | não é demonstrada por agregados de StageMetrics |

Marker esperado:

```text
LAB2_STAGE_METRICS_DRILL_PRESSURE_OK
```

Output esperado:

```text
s3a://lakehouse/gold/lab2/stage_metrics_drill/pressure
```

As duas variantes produzem um resumo com este grain:

```text
vendor_region, category_id, sale_year_month
```

### 3.2 Execute a variante default

Altere somente:

```python
CONFIG_NAME = "lab2-stage-metrics-drill-default"
```

Execute novamente o mesmo submit. A variante `default` reduz os dados antes de
uma repartição por chave e produz o mesmo resumo de 50 linhas.

Marker esperado:

```text
LAB2_STAGE_METRICS_DRILL_DEFAULT_OK
```

Output esperado:

```text
s3a://lakehouse/gold/lab2/stage_metrics_drill/default
```

### 3.3 Compare a evidência

Exemplo local validado com `SCALE=xs`:

| Variante | Stages | Tasks | Executor runtime | Shuffle written | Memory spilled | Disk spilled | GC time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| default | 16 | 356 | ~59.3s | ~40.0 MiB | 0 B | 0 B | ~1.7s |
| pressure | 17 | 839 | ~94.2s | ~670.9 MiB | ~800.0 MiB | ~382.2 MiB | ~3.2s |

Evidência bruta da execução `pressure`:

```text
numStages => 17
numTasks => 839
executorRunTime => 94175
jvmGCTime => 3246
diskBytesSpilled => 400765052
memoryBytesSpilled => 838859488
shuffleTotalBytesRead => 703458127
shuffleBytesWritten => 703458127
```

Evidência bruta da execução `default`:

```text
numStages => 16
numTasks => 356
executorRunTime => 59274
jvmGCTime => 1676
diskBytesSpilled => 0
memoryBytesSpilled => 0
shuffleTotalBytesRead => 41940653
shuffleBytesWritten => 41940653
```

Na validação local documentada, uma comparação adicional dos outputs apresentou:

```text
default row count=50
pressure row count=50
default minus pressure=0
pressure minus default=0
```

Esses valores são evidência pré-computada da calibração. O `validate_result()`
da aplicação confirma somente que o output foi produzido; ele não executa essa
comparação entre variantes.

#### Checkpoint de raciocínio — pressão agregada

- **Pergunta:** os agregados sustentam pressão de shuffle, spill ou GC?
- **Hipótese:** carregar payload largo e reparticionar cedo aumenta movimento e
  pode ultrapassar a memória disponível para essa execução.
- **Evidência:** shuffle, memory spill, disk spill e a relação entre `jvmGCTime`
  e `executorRunTime` nas variantes `pressure` e `default`.
- **Conclusão:** spill positivo demonstra spill nesta execução; a razão local de
  GC sustenta uma observação de aproximadamente 3.4%, não o cenário teórico de
  25% da questão.
- **Limitação:** StageMetrics não identifica qual task concentrou a pressão. Em
  outra máquina, spill zero significa somente que spill não foi observado nessa
  execução; não autoriza forçar uma narrativa de memory pressure.

## 4. Lab 2C: outlier no extremo superior

Leia a questão de origem:

[Lab 2C: diagnosticando high-end task skew](docs/exam_questions.md)

StageMetrics foi suficiente em 2A e 2B porque as perguntas tratavam pressão
agregada. O 2C pergunta se uma ou poucas tasks são muito maiores que a task
típica e, por isso, exige uma distribuição.

### 4.1 Execute o workload com TaskMetrics

Abra:

```text
lab_2c_task_duration_skew_diagnosis.py
```

Configuração da aula:

```python
CONFIG_NAME = "lab2c-task-skew-task"
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
  /opt/spark/src/apps/labs/lab_2/lab_2c_task_duration_skew_diagnosis.py
```

O workload preserva o hot vendor gerado:

```text
sales
  -> repartition(27, vendor_id)
  -> shuffle join com vendors
  -> aggregate por vendor_id e vendor_region
  -> escrita Delta em Gold
```

AQE e automatic broadcast joins estão desabilitados para manter visível o
shuffle de 27 tasks.

Marker esperado:

```text
LAB2C_TASK_SKEW_TASK_OK
```

Output esperado:

```text
s3a://lakehouse/gold/lab2/task_skew/task
```

Grain do output de negócio:

```text
vendor_id, vendor_region
```

### 4.2 Leia o boxed report

O relatório nativo do TaskMetrics aparece primeiro. Em seguida, o lab projeta o
mesmo TaskMetrics DataFrame:

```text
LAB 2C TASKMETRICS DIAGNOSTIC REPORT
Selected stage
Metric summary
Top task outliers by shuffleTotalBytesRead
```

Regra de decisão:

```text
Se max for muito maior que p75 tanto para duration quanto para volume de dados,
diagnostique high-end task skew.
```

Exemplo local validado com `SCALE=xs`:

| Métrica | p75 | Max | Max / p75 |
| --- | ---: | ---: | ---: |
| `duration` | 227 ms | 2,033 ms | 8.96x |
| `shuffleTotalBytesRead` | 617,822 B | 28,946,218 B | 46.85x |
| `shuffleRecordsRead` | 75,904 | 3,561,478 | 46.92x |

A task de maior volume no exemplo validado foi:

```text
task=236
duration=2033 ms
shuffleRecordsRead=3,561,478
shuffleTotalBytesRead=27.6 MiB
memoryBytesSpilled=80.0 MiB
diskBytesSpilled=16.4 MiB
```

#### Checkpoint de raciocínio — high-end task skew

- **Pergunta:** uma ou poucas tasks processaram muito mais dados que a task
  típica?
- **Hipótese:** a chave dominante cria máximos muito acima de p75 para tempo e
  volume de dados.
- **Evidência:** max/p75 de `duration`, `shuffleTotalBytesRead` e
  `shuffleRecordsRead` no stage selecionado de 27 tasks.
- **Conclusão:** a forma da distribuição sustenta um sinal de skew controlado.
  Neste stage, `shuffleTotalBytesRead` é o sinal de volume por task usado para
  reproduzir o raciocínio distributivo da questão de origem; ele não é o mesmo
  contador que `Input Size`.
- **Limitação:** valores de duração variam com o scheduling local, e o exercício
  para no diagnóstico; ele não prova uma solução de produção.

Opções de remediação apenas para discussão:

- aplicar salting à hot key;
- usar agregação em duas etapas;
- processar a chave dominante separadamente;
- avaliar o tratamento de skew do AQE em produção.

## 5. Lab 2D: tasks vazias no extremo inferior

Leia a questão de origem:

[Lab 2D: diagnosticando partitions quase vazias](docs/exam_questions.md)

O 2D continua em TaskMetrics, mas inverte a pergunta: em vez de procurar um
máximo dominante, procura tasks no mínimo que não processaram dados úteis.

### 5.1 Execute o workload com TaskMetrics

Abra:

```text
lab_2d_empty_partitions_diagnosis.py
```

Configuração da aula:

```python
CONFIG_NAME = "lab2d-empty-partitions-task"
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
  /opt/spark/src/apps/labs/lab_2/lab_2d_empty_partitions_diagnosis.py
```

O workload deriva um bucket sintético de `sale_id` para não herdar o hot-vendor
skew do 2C:

```text
sales
  -> derivar partition_bucket com 48 buckets ativos
  -> repartition(27, partition_bucket)
  -> aggregate por partition_bucket
  -> escrita Delta em Gold
```

AQE está desabilitado para não coalescer a distribuição que a aula precisa
observar.

Marker esperado:

```text
LAB2D_EMPTY_PARTITIONS_TASK_OK
```

Output esperado:

```text
s3a://lakehouse/gold/lab2/empty_partitions/task
```

Grain do output de negócio:

```text
partition_bucket
```

### 5.2 Leia o boxed report

O relatório nativo é seguido por:

```text
LAB 2D TASKMETRICS EMPTY PARTITIONS REPORT
Selected stage
Metric summary
Lowest task outliers by shuffleRecordsRead
```

Regra de decisão:

```text
Se o mínimo de volume for muito menor que a mediana, inspecione as tasks no
extremo inferior. Se o máximo permanecer próximo de p75, não diagnostique
high-end skew.
```

Exemplo local validado com `SCALE=xs`:

| Métrica | Min | Mediana | p75 | Max | Max / p75 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `shuffleRecordsRead` | 0 | 104,633 | 311,706 | 521,959 | 1.67x |
| `shuffleTotalBytesRead` | 0 B | 831.9 KiB | 3.0 MiB | 5.4 MiB | 1.79x |
| `recordsWritten` | 0 | 1 | 3 | 5 | 1.67x |

O stage selecionado continha quatro tasks vazias. O extremo inferior mostrou:

```text
task=193 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
task=192 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
task=191 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
task=190 shuffleRecordsRead=0 shuffleTotalBytesRead=0 B recordsWritten=0
```

#### Checkpoint de raciocínio — partitions vazias

- **Pergunta:** o extremo inferior contém tasks sem trabalho útil?
- **Hipótese:** o mapeamento hash dos 48 buckets ativos para 27 shuffle
  partitions não ocupa necessariamente todas as partitions de maneira uniforme,
  podendo produzir tasks sem registros enquanto a mediana permanece positiva.
- **Evidência:** mínimo, mediana, p75, máximo, bytes e registros processados no
  stage selecionado.
- **Conclusão:** TaskMetrics mostra que algumas shuffle partitions não receberam
  registros. O padrão é `min = 0` com mediana positiva e sem um máximo
  proporcionalmente dominante.
- **Limitação:** o sinal demonstra ocupação desigual das partitions, mas não
  prova que a quantidade de partitions, isoladamente, seja a causa; duration
  local também pode oscilar por scheduling.

## 6. Comparação final da aula

| Sintoma | Primeiro coletor | Relação de evidência | Próxima investigação |
| --- | --- | --- | --- |
| movimento excessivo no stage | StageMetrics | shuffle bytes e task volume elevados | joins, aggregations e repartitions |
| pressão de memória no shuffle | StageMetrics | memory/disk spill maior que zero | partition size, memory e operações wide |
| pressão de GC | StageMetrics | razão GC/runtime material e repetível | object pressure, caching, serialization e memory |
| high-end task skew | TaskMetrics | max muito maior que p75 | hot keys e stragglers |
| partitions vazias no extremo inferior | TaskMetrics | min muito menor que mediana | partition count e key distribution |

Mensagem principal:

```text
Comece com StageMetrics para classificar o sintoma operacional amplo.
Use TaskMetrics como microscópio somente quando o diagnóstico depender da
distribuição dentro de um stage.
```

## Material operacional opcional

### 7. Inspecione Spark History Server e MinIO

Spark History Server:

```text
http://127.0.0.1:28090
```

Application names:

```text
workshop-lab2-shuffle-aggregation-baseline
workshop-lab2-shuffle-aggregation-optimized
workshop-lab2-stage-metrics-drill-pressure
workshop-lab2-stage-metrics-drill-default
workshop-lab2c-task-skew-task
workshop-lab2d-empty-partitions-task
```

Em `Jobs`, use descrições `LAB2`, `LAB2C` e `LAB2D`. Em `Stages`, compare
duração, tasks, shuffle e spill. Em `SQL / DataFrame`, conecte exchanges e
aggregations às métricas. Para 2C e 2D, use Summary Metrics do stage concluído
para revisar a distribuição.

MinIO Console:

```text
http://127.0.0.1:29011
```

Paths Gold esperados:

```text
s3a://lakehouse/gold/lab2/shuffle_aggregation/baseline
s3a://lakehouse/gold/lab2/shuffle_aggregation/optimized
s3a://lakehouse/gold/lab2/stage_metrics_drill/pressure
s3a://lakehouse/gold/lab2/stage_metrics_drill/default
s3a://lakehouse/gold/lab2/task_skew/task
s3a://lakehouse/gold/lab2/empty_partitions/task
```

A persistência das métricas está desabilitada. As métricas aparecem no terminal
e no event history; os outputs de negócio são persistidos como Delta.

Credenciais padrão do MinIO:

```text
user:     sparkworkshop
password: sparkworkshop123
```

### 8. Limpeza opcional depois da aula

Na raiz do repositório:

```bash
cd ../../../..
make down
```

Para remover também os dados do MinIO:

```bash
make clean-data
```

Para remover as images do workshop:

```bash
make removeimage
```

`make clean-data` remove dados usados pelos outros labs. Não execute esse comando
entre os exercícios do Lab 2.

### Apêndice A — Por que os boxed reports de TaskMetrics existem

2C e 2D preservam o output nativo do TaskMetrics. Os boxes não coletam novas
medições; eles criam uma visão didática a partir de:

```python
task_metrics = collector.create_taskmetrics_DF(...)
```

Fluxo compartilhado:

```text
TaskMetrics begin/end
  -> create_taskmetrics_DF
  -> selecionar um stage concluído útil
  -> agregar percentis de task
  -> coletar uma pequena tabela de outliers
  -> renderizar um bloco multilinha pelo logger
```

O 2C seleciona o sinal mais claro no extremo superior e calcula:

```text
p75
max
max / p75
```

Depois ordena os outliers por `shuffleTotalBytesRead` decrescente.

O 2D seleciona o sinal mais claro no extremo inferior e calcula:

```text
min
median
p75
max
median / min
max / p75
```

Depois ordena os outliers por `shuffleRecordsRead` crescente. Os dois boxes usam
uma única chamada multilinha do logger; o relatório nativo permanece disponível
imediatamente antes deles.

Quando `min = 0`, `median / min` não é uma razão numérica definida. O zero já é
o sinal explícito de uma task vazia; a implementação atual usa um sentinel para
esse caso e apresenta a razão como `∞` no boxed report.

### Apêndice B — Contexto da validação local

Os valores documentados foram coletados na stack local WSL, com dois Spark
workers e dados `SCALE=xs`:

```text
sales rows=5,000,000
sales files=114
sales total bytes≈762 MB
hot vendor share≈0.7001
```

Runtime e GC exatos dependem do ambiente. Docker, WSL, host, driver e executors
competem pelos mesmos recursos. A evidência durável está nas relações:

- shuffle comparado somente quando a compatibilidade funcional foi verificada
  pelo experimento ou por evidência de calibração separada;
- spill observado maior que zero ou ausência de spill observado;
- GC time em relação ao executor runtime;
- max comparado com p75 no extremo superior;
- min comparado com mediana no extremo inferior.

`SCALE=s` foi usado em stress tests anteriores, mas os exemplos documentados
mais recentes usam `SCALE=xs`. Reexecute a escala maior antes de apresentar
qualquer número histórico de `SCALE=s`.

## Ponte para a próxima aula

O Lab 2 mostrou quando StageMetrics basta e quando TaskMetrics é necessário.
O Lab 3 usa os mesmos coletores para investigar uma nova pergunta: qual é o
custo de coletar evidência mais detalhada?
