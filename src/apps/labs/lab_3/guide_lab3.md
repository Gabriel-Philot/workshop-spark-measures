# Guia do Lab 3: post-mortem do benchmark de overhead de observabilidade

Este guia organiza a execução em sala do Lab 3. O lab executa o mesmo workload
nos modes `none`, `stage` e `task`, persiste as fronteiras de tempo e discute por
que overhead de observabilidade exige warmup, repetições e interpretação
cuidadosa.

Evidência detalhada de desenvolvimento:

[Post-mortem do benchmark de overhead de observabilidade](docs/observability_overhead_postmortem.md)

Use este guia para a sequência da aula e o post-mortem para consultar as duas
calibrações completas e suas limitações.

## Enquadramento da aula

- **Pergunta norteadora:** quanto custo observável os collectors adicionam e o
  que é necessário para comparar esse custo com disciplina?
- **Por que esta aula aparece agora:** os Labs 1 e 2 usaram StageMetrics e
  TaskMetrics para diagnóstico; agora investigamos o custo de escolher cada
  granularidade.
- **Resultado de aprendizagem:** distinguir demonstração, benchmark, warmup,
  fronteiras temporais e ruído do ambiente sem transformar um resultado local
  em regra universal.
- **Modo de condução:** demonstração curta ao vivo pelo instrutor; evidência
  repetida pré-computada como núcleo da discussão; regeneração completa
  opcional.

## 0. Pré-requisitos

Comece na raiz do repositório:

```bash
cd workshop-spark-measures
```

O Lab 3 reutiliza as fontes Bronze `sales`, `vendors`, `products` e `customers`.
Se for a primeira execução do workshop, ou se imagens e dados do MinIO foram
removidos, siga as seções 1 a 5 do [guia do Lab 0](../lab_0/guide_lab0.md).

Se os dados continuam disponíveis e apenas os containers foram interrompidos:

```bash
make compose
```

As fontes esperadas são:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

> **Nota do instrutor:** a prontidão do ambiente importa mais em um benchmark
> do que em uma demonstração funcional. Worker indisponível, fonte ausente ou
> download inicial de dependência altera o tempo observado e invalida a
> comparação.

## 1. Entre na pasta do Lab 3

```bash
cd src/apps/labs/lab_3
```

Arquivos principais:

```text
lab_3_observability_overhead_benchmark.py
run_observability_overhead_benchmark.sh
guide_lab3.md
docs/
lab_3_utils/
```

O app Python executa uma run. O runner shell orquestra runs sequenciais,
alterando apenas o mode de observabilidade e a identidade da execução.

## 2. Entenda a comparação controlada

| Mode | Collector | Papel no experimento |
|---|---|---|
| `none` | desabilitado | baseline do workload |
| `stage` | StageMetrics | observabilidade agregada por stage |
| `task` | TaskMetrics | observabilidade detalhada por task |

Os três modes executam o mesmo código de negócio:

```text
sales + vendors + products + customers
  -> three joins
  -> deterministic benchmark bucket
  -> repartition to 384 shuffle partitions
  -> bucket-level aggregation
  -> second region/category/month aggregation
  -> ranking window
  -> unique Gold Delta output
```

Na execução local validada com `SCALE=xs`, o resultado final possui 200 linhas.
O YAML desabilita AQE e broadcast joins para que o Spark não elimine a pressão
de tasks que o benchmark precisa produzir.

O runner usa o mesmo app e as mesmas configurações de workload nos três modes.
O marker `LAB3_OVERHEAD_VALIDATION_OK` confirma escrita bem-sucedida e registra
o `row_count`; ele não executa uma comparação completa de conteúdo entre os
três datasets.

> **Nota do instrutor:** as fontes contêm vendor skew, mas este lab não é um
> diagnóstico de skew. A variável controlada é o collector; o workload deve
> permanecer estável.

## 3. Execute a demonstração curta

Execute uma repetição medida, sem warmup:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
bash run_observability_overhead_benchmark.sh
```

O runner dispara três `spark-submit` sequenciais:

```text
none -> stage -> task
```

No ambiente WSL/Docker validado, essa demonstração levou aproximadamente
`3m53s`. Esse número serve apenas para planejamento de aula; o tempo local varia.

Markers da orquestração:

```text
LAB3_BENCHMARK_STARTED
LAB3_SUBMIT_STARTED
LAB3_SUBMIT_COMPLETED
LAB3_BENCHMARK_COMPLETED
```

Markers esperados nos três submits:

```text
LAB3_OVERHEAD_VALIDATION_OK
LAB3_METADATA_WRITTEN
LAB3_OBSERVABILITY_OVERHEAD_NONE_OK
LAB3_OBSERVABILITY_OVERHEAD_STAGE_OK
LAB3_OBSERVABILITY_OVERHEAD_TASK_OK
WORKSHOP_EXPERIMENT_COMPLETED
```

### O que o warmup consegue e não consegue reduzir

Cada repetição deste benchmark é uma nova aplicação Spark. Portanto, o warmup
não reutiliza o driver JVM, o JIT ou os executor JVMs da aplicação seguinte.
Esses custos podem reaparecer em todo `spark-submit`.

O warmup serve principalmente para identificar e reduzir efeitos frios do
ambiente compartilhado, como containers recém-iniciados, artefatos ainda não
acessados, conexões iniciais com serviços e caches do sistema operacional ou do
MinIO. Esses efeitos também não são garantidamente eliminados.

Por isso, warmup melhora a disciplina experimental, mas não transforma as runs
medidas em execuções livres de startup ou de ruído.

O runner persiste essas execuções com:

```text
is_warmup=true
```

Uma análise medida deve filtrá-las. Adicionar uma rodada de warmup dobraria a
demonstração curta de três para seis submits; por isso a aula explica o conceito
e usa a evidência repetida do post-mortem para discutir overhead.

### Checkpoint de raciocínio — demonstração curta

- **Pergunta:** os três modes executam o mesmo mecanismo de workload e produzem
  metadados comparáveis?
- **Hipótese:** `none`, `stage` e `task` preservam o código do workload, mas
  adicionam fronteiras diferentes de coleta.
- **Evidência:** markers de sucesso e escrita, `row_count`,
  `workload_wall_ms`, `collector_begin_end_ms` e
  `collector_aggregate_ms` por mode.
- **Conclusão:** a demonstração comprova o mecanismo de execução, medição e
  persistência necessário à comparação.
- **Limitação:** uma repetição sem warmup não forma uma distribuição, não valida
  equivalência completa dos outputs e não estabelece overhead confiável.

## 4. Leia corretamente as fronteiras de tempo

O runner shell registra:

```text
spark_submit_wall_ms
```

Essa fronteira inclui Docker exec, startup de Python/JVM, SparkSession,
workload, validação, persistência de metadados e encerramento do processo. Ela é
útil para planejar o tempo ocupado pelo comando, não para isolar o collector.

A tabela Delta registra:

```text
workload_wall_ms
```

Essa é a fronteira principal da comparação. Ela mede o corpo do workload com o
collector selecionado ativo quando aplicável.

| Campo | Fronteira medida |
|---|---|
| `app_wall_ms` | início do app até o fim da validação, antes da escrita dos metadados |
| `spark_session_ms` | criação/reuso da SparkSession |
| `workload_wall_ms` | corpo do workload; campo principal da comparação |
| `collector_begin_end_ms` | janela completa de `begin()` a `end()`, incluindo o workload |
| `collector_report_ms` | renderização opcional do relatório nativo |
| `collector_aggregate_ms` | agregação das métricas após `end()` |
| `validation_wall_ms` | validação do output depois do workload |

`collector_begin_end_ms` não é overhead puro: ele contém o workload. O custo
isolado também não deve ser inferido subtraindo duas runs únicas e ruidosas.
`collector_report_ms` e `collector_aggregate_ms` ficam fora de
`workload_wall_ms`. A persistência dos metadados ocorre depois dessas etapas.

No mode `none`, os campos produzidos pelo sparkMeasure são persistidos como `0`
porque nenhum collector os emitiu. Esses zeros representam indisponibilidade
da métrica nesse mode, não prova de que o Spark executou zero stages, tasks,
executor time ou shuffle.

Ao analisar métricas do sparkMeasure, não inclua esses zeros em médias ou
comparações entre collectors. Use `mode` para tratá-los como indisponíveis.
`workload_wall_ms`, por outro lado, é válido nos três modes.

> **Regra da aula:** compare modes por `workload_wall_ms`. Use
> `spark_submit_wall_ms` apenas quando a pergunta for quanto tempo o comando
> completo ocupou a máquina.

`workload_wall_ms` estima o impacto da instrumentação enquanto o workload está
executando. Ele não representa o custo end-to-end completo de usar
sparkMeasure.

Os percentuais apresentados neste lab são deltas da fronteira do workload.
Custos de `begin/end`, agregação, relatório e persistência pertencem a outras
fronteiras e devem ser incluídos somente quando fizerem parte da pergunta
operacional.

## 5. Discuta a evidência repetida do post-mortem

Abra:

[Post-mortem do benchmark de overhead de observabilidade](docs/observability_overhead_postmortem.md)

A primeira calibração, baseada somente em `sales`, produziu 208 tasks medidas.
Mesmo com dez repetições, o ruído local foi maior que a diferença entre
collectors:

| Mode | Média de `workload_wall_ms` | Delta médio contra `none` |
|---|---:|---:|
| `none` | 24.369s | baseline |
| `stage` | 24.873s | +504ms |
| `task` | 24.510s | +141ms |

Essa inversão local não demonstra que StageMetrics seja mais caro. Ela mostra
que o experimento era leve demais para separar o sinal do ruído.

O workload atual de multi-join produziu, no exemplo local validado:

```text
20 stages
2,581 tasks
~643 MB shuffle written
200 output rows
```

Com três repetições medidas:

| Mode | Média de `workload_wall_ms` | Mediana | Delta médio contra `none` |
|---|---:|---:|---:|
| `none` | 63.463s | 63.432s | baseline |
| `stage` | 64.467s | 64.600s | +1.004s / +1.58% |
| `task` | 64.691s | 64.898s | +1.228s / +1.93% |

A segunda calibração é direcionalmente coerente com o custo adicional de
eventos por task, mas a diferença continua modesta. Os valores são evidência
local do Lab 3, não percentuais oficiais ou universais.

A orientação oficial do sparkMeasure é qualitativa: usar StageMetrics sempre
que possível por ser mais leve e recorrer a TaskMetrics quando skew, long tails
ou stragglers exigirem detalhe por task. A fonte e o trecho exato estão
preservados no [post-mortem](docs/observability_overhead_postmortem.md#official-sparkmeasure-guidance).

### Checkpoint de raciocínio — evidência repetida

- **Pergunta:** após warmup e repetições, existe sinal consistente de custo?
- **Hipótese:** TaskMetrics tende a adicionar mais trabalho porque coleta
  eventos por task, enquanto StageMetrics permanece a primeira camada mais
  leve.
- **Evidência:** distribuições de `workload_wall_ms`, quantidade de tasks,
  mesmas fronteiras de medição, rotação da ordem dos modes e orientação oficial.
- **Conclusão:** a segunda calibração sustenta localmente a direção do tradeoff;
  não sustenta um percentual universal de overhead.
- **Limitação:** três repetições permitem observar uma direção local, mas não
  estimam robustamente variância, intervalo de confiança ou significância
  estatística. Startup do Spark, Delta, MinIO, Docker, WSL, ordem das runs,
  event logs e caches ainda podem aproximar ou inverter resultados individuais.

## 6. Evidência persistida

Cada run escreve um output de negócio único:

```text
s3a://lakehouse/gold/lab3/observability_overhead/workload/
  benchmark_id=<benchmark_id>/
  mode=<mode>/
  iteration=<iteration>/
  run_id=<run_id>
```

A identidade física impede que uma run sobrescreva ou reutilize o output Delta
de outra.

Uma linha de metadados é adicionada a:

```text
s3a://observability/lab3/overhead_runs
```

Campos de identidade:

```text
benchmark_id
run_id
iteration
is_warmup
mode
config_name
app_name
application_id
```

Métricas do sparkMeasure quando existe collector:

```text
num_stages
num_tasks
executor_run_time_ms
shuffle_bytes_written
```

Antes de comparar distribuições, filtre:

```text
is_warmup=false
```

## 7. Caminhos opcionais

As atividades seguintes aprofundam a operação, mas não pertencem ao núcleo da
aula.

### 7.1 Submit manual de um mode

O runner é preferível porque cria identidades comparáveis e rotaciona a ordem
dos modes. Para observar apenas o app em mode `stage`:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB3_CONFIG_NAME=lab3-overhead-stage \
    LAB3_MODE=stage \
    /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_3/lab_3_observability_overhead_benchmark.py
```

Configurações disponíveis:

```text
lab3-overhead-none
lab3-overhead-stage
lab3-overhead-task
```

`LAB3_CONFIG_NAME` e `LAB3_MODE` precisam descrever o mesmo collector; o
runtime falha imediatamente quando eles divergem.

### 7.2 Relatório nativo do sparkMeasure

O benchmark repetido desabilita o relatório por padrão:

```text
LAB3_EMIT_SPARKMEASURE_REPORT=false
```

Para uma demonstração isolada:

```bash
LAB3_REPETITIONS=1 \
LAB3_WARMUP_REPETITIONS=0 \
LAB3_EMIT_SPARKMEASURE_REPORT=true \
bash run_observability_overhead_benchmark.sh
```

Renderizar o relatório possui custo próprio. Não misture runs com e sem
relatório na comparação principal, salvo quando esse custo fizer parte da
pergunta experimental.

### 7.3 Benchmark completo com warmup

> **Aviso:** este caminho não faz parte do fluxo ao vivo padrão. Ele executa 33
> aplicações Spark sequenciais e pode ocupar aproximadamente 43–44 minutos no
> ambiente local validado.

```bash
LAB3_REPETITIONS=10 \
LAB3_WARMUP_REPETITIONS=1 \
bash run_observability_overhead_benchmark.sh
```

Com os três modes:

```text
1 warmup round    x 3 modes =  3 warmup submits
10 measured rounds x 3 modes = 30 measured submits
total = 33 sequential spark-submit executions
```

O runner rotaciona o primeiro mode por iteração medida:

```text
iteration 1: none  -> stage -> task
iteration 2: stage -> task  -> none
iteration 3: task  -> none  -> stage
```

Ele permanece sequencial. Executar modes em paralelo transformaria o benchmark
em um experimento de contenção por CPU, memória, workers, disco, MinIO e event
logs.

### 7.4 UIs auxiliares

URLs do ambiente local:

```text
Spark Master UI:      http://127.0.0.1:28091
Spark History Server: http://127.0.0.1:28090
MinIO Console:        http://127.0.0.1:29011
```

Nomes das aplicações:

```text
workshop-lab3-overhead-none
workshop-lab3-overhead-stage
workshop-lab3-overhead-task
```

No Spark History Server, confirme a forma do workload, compare as descrições
`LAB3 | observability_overhead` e separe jobs de negócio dos jobs de validação
e escrita Delta dos metadados. A UI complementa a tabela persistida; IDs
numéricos de jobs e stages não são estáveis entre runs.

Use o Spark Master apenas para verificar os workers durante uma execução ativa
e o MinIO para confirmar os paths persistidos, sem transformar essas inspeções
em etapas obrigatórias do argumento pedagógico.

### 7.5 Cleanup depois da aula

Retorne à raiz:

```bash
cd ../../../..
make down
```

Para remover dados do MinIO:

```bash
make clean-data
```

Para remover imagens do workshop:

```bash
make removeimage
```

Não execute `make clean-data` entre repetições. O comando remove tanto as
fontes Bronze compartilhadas quanto a evidência persistida do benchmark.

## Conclusão da aula

sparkMeasure possui overhead observável, mas o tamanho desse overhead depende
da granularidade do collector, do volume de eventos, da forma do workload e do
ambiente. StageMetrics continua sendo a primeira camada diagnóstica recomendada;
TaskMetrics deve responder a uma pergunta que realmente dependa da distribuição
entre tasks.

O ponto do Lab 3 não é provar que sparkMeasure sempre adiciona X milissegundos.
É aprender a construir e interpretar uma comparação repetível sem confundir
uma demonstração ao vivo com evidência estatística.

## Ponte para a próxima aula

Depois de medir o custo de coletar StageMetrics, o Lab 4 mostra como transformar
seus agregados em uma primeira hipótese operacional sobre o perfil do workload.
