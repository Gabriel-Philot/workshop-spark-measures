# Storyboard - Secao teorica: como o sparkMeasure funciona

Este storyboard e uma proposta de producao para a proxima secao da apresentacao. Ele nao altera os slides ja aprovados; a ideia e revisar narrativa, layout e densidade tecnica antes de transformar em HTML/CSS/SVG.

## Direcao visual

- Base: grafite/preto azulado, com textura sutil de terminal, grid tecnico ou linhas de circuito.
- Laranja Spark: acento para Spark, shuffle, gargalo, alerta, mudanca de stage e pontos de decisao.
- Cyan/azul petroleo: eventos, ListenerBus, fluxo de dados e metricas.
- Verde terminal: snippets, logs, outputs e comandos.
- Cinza claro: texto principal, labels, bordas e anotacoes.
- Evitar cards grandes com cara de landing page. Preferir superficies de engenharia: terminal, tabela, DAG, log, diff, console, flame strip e diagramas compactos.
- Cada slide deve reservar pelo menos um espaco para asset futuro, mas sem esconder a teoria.

---

## Slide T1 - Abertura: abrindo a caixa do sparkMeasure

**Titulo**

Como o sparkMeasure funciona por dentro

**Subtitulo**

Do plano Spark aos eventos, metricas e relatorios que viram diagnostico

**Mensagem central**

A partir daqui, a narrativa muda: nao estamos mais explicando por que medir Spark importa; estamos abrindo o caminho interno que permite medir sem reescrever o job.

**Layout sugerido**

```text
+---------------------------------------------------------------------+
| Como o sparkMeasure funciona por dentro                             |
| Do plano Spark aos eventos, metricas e relatorios...                |
+---------------------------------------+-----------------------------+
| [PLACEHOLDER ASSET FUTURO]            |  Spark SQL                  |
| imagem/animacao do pipeline interno   |      -> Scheduler           |
|                                       |      -> ListenerBus         |
|                                       |      -> sparkMeasure        |
|                                       |      -> diagnostico         |
+---------------------------------------+-----------------------------+
| Spark ja produz sinais. O sparkMeasure transforma esses sinais em    |
| evidencias organizadas para investigar performance.                  |
+---------------------------------------------------------------------+
```

**SVG customizado**

Mini pipeline com cinco nos: `Spark SQL`, `Scheduler`, `ListenerBus`, `sparkMeasure`, `Diagnostico`. Usar laranja Spark nos pontos de entrada/saida e cyan no caminho dos eventos.

**Asset futuro**

Uma imagem abstrata/tecnica de "inside Spark runtime", preferencialmente sem texto.

---

## Slide T2 - Recap: o Spark ja deixa rastros

**Titulo**

Antes do sparkMeasure, o Spark ja deixava rastros

**Mensagem central**

O Spark ja gera planos, jobs, stages, tasks, metricas, UI, EventLog e History Server. O problema nao e ausencia de sinais; e transformar sinais espalhados em diagnostico rapido.

**Layout sugerido**

```text
+-------------------------------+-------------------------------------+
| snippet PySpark               | fluxo nativo do Spark                |
|                               | Codigo -> Planos -> Jobs             |
| df = spark.read.parquet(...)  |      -> Stages -> Tasks              |
| result = df.filter(...)       |      -> Metrics -> UI/EventLog       |
| result.show()                 |                                     |
+-------------------------------+-------------------------------------+
| [PLACEHOLDER ASSET] ou SVG horizontal com rastro de execucao          |
+---------------------------------------------------------------------+
```

**Snippet**

```python
df = spark.read.parquet("/data/orders")

result = (
    df.filter("country = 'BR'")
      .groupBy("vendor_id")
      .count()
)

result.show()
```

**Texto curto no slide**

`sparkMeasure nao cria a telemetria do zero. Ele usa eventos e metricas que o runtime Spark ja produz.`

**SVG customizado**

Fluxo em camadas:

```text
SQL/DataFrame -> Catalyst -> Physical Plan -> DAGScheduler
              -> Jobs/Stages -> Tasks -> SparkListenerEvents
              -> UI/EventLog/History/sparkMeasure
```

**Observacao de narrativa**

Este slide deve funcionar como recap mesmo. Pode ter mais "numeros conceituais" do Spark: `jobs`, `stages`, `tasks`, `shuffle`, `event log`, `metrics`.

---

## Slide T3 - Plano nao e metrica

**Titulo**

Plano mostra intencao. Metrica mostra execucao.

**Mensagem central**

`explain()` ajuda a entender como o Spark pretende executar. Mas gargalo real aparece nas metricas de runtime: tempo de executor, CPU, GC, shuffle, spill e distribuicao de tasks.

**Layout sugerido**

```text
+-------------------------------+-------------------------------------+
| EXPLAIN FORMATTED             | runtime observado                    |
|                               | tabela de metricas                   |
| == Physical Plan ==           | stageId | cpu | shuffle | spill      |
| Exchange hashpartitioning...  |   12    | 81s | 8.7 GB  | 2.1 GB     |
| SortMergeJoin                 |   13    | 22s | 480 MB  | 0 MB       |
+-------------------------------+-------------------------------------+
| Plano responde "como deve executar". Metrica responde "o que custou".|
+---------------------------------------------------------------------+
```

**Snippet/console**

```text
== Physical Plan ==
*(5) HashAggregate(keys=[vendor_id#42], functions=[count(1)])
+- Exchange hashpartitioning(vendor_id#42, 200)
   +- *(2) Project [vendor_id#42]
      +- *(2) Filter (country#18 = BR)
         +- *(2) FileScan parquet ...
```

**Tabela exemplo**

| stageId | tasks | executorRunTime | executorCpuTime | shuffleRead | spill |
|---:|---:|---:|---:|---:|---:|
| 12 | 200 | 96 s | 81 s | 8.7 GB | 2.1 GB |
| 13 | 200 | 34 s | 22 s | 480 MB | 0 MB |

**SVG customizado**

Dois trilhos paralelos:

- trilho superior: `EXPLAIN -> plano`
- trilho inferior: `SparkListenerEvents -> metricas`

Um marcador no meio reforca: `complementares, nao substitutos`.

---

## Slide T4 - Como o Spark transforma execucao em eventos

**Titulo**

Execucao distribuida vira eventos no driver

**Mensagem central**

Quando jobs, stages e tasks avancam, o Spark publica `SparkListenerEvent`. Listeners registrados recebem callbacks como `onStageCompleted` e `onTaskEnd`.

**Layout sugerido**

```text
+---------------------------------------------------------------------+
| Executor / TaskScheduler / DAGScheduler / SQLExecution               |
|        -> SparkListenerEvent                                         |
|        -> LiveListenerBus / AsyncEventQueue                          |
|        -> SparkListener callbacks                                    |
+---------------------------------------+-----------------------------+
| snippet Scala                         | lista curta de eventos       |
| override def onTaskEnd(...)           | JobStart / JobEnd            |
| override def onStageCompleted(...)    | StageSubmitted / Completed   |
|                                       | TaskStart / TaskEnd          |
+---------------------------------------+-----------------------------+
```

**Snippet**

```scala
class WorkloadListener extends SparkListener {
  override def onStageCompleted(
      event: SparkListenerStageCompleted
  ): Unit = {
    val info = event.stageInfo
  }

  override def onTaskEnd(
      event: SparkListenerTaskEnd
  ): Unit = {
    val metrics = event.taskMetrics
  }
}
```

**Eventos para mostrar**

| Evento | Sinal principal |
|---|---|
| `SparkListenerJobStart` | job entrou no scheduler |
| `SparkListenerStageCompleted` | stage terminou com `StageInfo` |
| `SparkListenerTaskEnd` | task terminou com `TaskInfo` e `TaskMetrics` |
| `SparkListenerExecutorMetricsUpdate` | metricas vindas de executors |
| `SparkListenerSQLExecutionStart/End` | execucoes Spark SQL |

**SVG customizado**

Pipeline tecnico com fila assíncrona no centro. Usar cyan para o bus e laranja Spark nas bordas de stage/task.

---

## Slide T5 - Onde o sparkMeasure entra dentro do Spark

**Titulo**

sparkMeasure entra como listener especializado

**Mensagem central**

O sparkMeasure se registra no `SparkContext`, escuta eventos do Spark, achata os dados no driver e entrega relatorios, mapas, DataFrames ou sinks externos.

**Layout sugerido**

```text
+---------------------------------------------------------------------+
| SparkContext                                                        |
|   -> addSparkListener(...)                                          |
|   -> StageInfoRecorderListener / TaskInfoRecorderListener           |
|   -> callbacks                                                      |
|   -> ListBuffer no driver                                           |
|   -> report / Map / DataFrame / sink                                |
+-------------------------------+-------------------------------------+
| modo instrumentado            | modo Flight Recorder                 |
| begin/end/runAndMeasure       | listener via configuracao            |
+-------------------------------+-------------------------------------+
```

**Snippet PySpark**

```python
from sparkmeasure import StageMetrics

stagemetrics = StageMetrics(spark)
stagemetrics.begin()

spark.sql("""
  SELECT vendor_id, count(*)
  FROM orders
  GROUP BY vendor_id
""").show()

stagemetrics.end()
stagemetrics.print_report()
```

**Console conceitual**

```text
Spark runtime emits events
ListenerBus delivers events
sparkMeasure flattens metrics
driver stores StageVals / TaskVals
report, DataFrame or external sink is produced
```

**SVG customizado**

Um encaixe visual tipo "plug-in":

```text
SparkContext --addSparkListener--> sparkMeasure Listener
     |                                  |
     v                                  v
 Spark UI / EventLog              StageVals / TaskVals
```

**Ponto de precisao**

Nao apresentar como profiler externo. Nao apresentar como exporter JMX. A frase certa: `consumidor especializado dos eventos do Spark`.

---

## Slide T6 - StageMetrics: visao operacional barata

**Titulo**

StageMetrics: uma linha por stage, diagnostico rapido

**Mensagem central**

StageMetrics e o modo default para observar workload Spark sem explodir volume de dados. Ele agrega metricas por stage e ajuda a comparar execucoes.

**Layout sugerido**

```text
+---------------------------------------------------------------------+
| [PLACEHOLDER ASSET] linha de stages / DAG simplificado               |
+---------------------------------------------------------------------+
| tabela: stageId | tasks | duration | cpu | shuffle | spill | gc       |
+---------------------------------------------------------------------+
| bom para: baseline | regressao | CI/CD | tuning inicial               |
+---------------------------------------------------------------------+
```

**Tabela**

| stageId | tasks | duration | executorCpuTime | shuffleRead | memorySpill | gcTime |
|---:|---:|---:|---:|---:|---:|---:|
| 3 | 128 | 41 s | 29 s | 1.2 GB | 0 MB | 1.8 s |
| 4 | 256 | 118 s | 76 s | 12.4 GB | 3.7 GB | 9.5 s |
| 5 | 64 | 18 s | 15 s | 0 MB | 0 MB | 0.4 s |

**Texto curto**

`StageMetrics responde: onde o workload comecou a custar mais?`

**SVG customizado**

Tres stages como blocos horizontais. Cada bloco tem mini barras internas para CPU, shuffle, spill e GC.

**Regra didatica**

`StageMetrics = O(stages)`  
`bom para observar sempre ou quase sempre`

---

## Slide T7 - TaskMetrics: investigacao cirurgica

**Titulo**

TaskMetrics: quando o problema mora dentro do stage

**Mensagem central**

Quando um stage parece lento, TaskMetrics abre a distribuicao das tasks. E o modo para procurar skew, stragglers, long tail, spill concentrado e executor problemático.

**Layout sugerido**

```text
+---------------------------------------------------------------------+
| tabela de tasks com uma linha laranja destacada                      |
+-------------------------------+-------------------------------------+
| mini histograma long tail      | checklist de perguntas              |
|                               | - alguma task 10x maior?             |
|                               | - shuffle concentrado?               |
|                               | - spill em poucos executors?          |
+-------------------------------+-------------------------------------+
```

**Tabela**

| taskId | executor | duration | cpuTime | shuffleRead | spill | sinal |
|---:|---|---:|---:|---:|---:|---|
| 801 | exec-2 | 7.8 s | 6.9 s | 44 MB | 0 MB | normal |
| 802 | exec-5 | 8.1 s | 7.0 s | 48 MB | 0 MB | normal |
| 803 | exec-3 | 93.4 s | 28.5 s | 4.8 GB | 1.9 GB | straggler |
| 804 | exec-1 | 7.5 s | 6.8 s | 41 MB | 0 MB | normal |

**Texto curto**

`StageMetrics mostra onde doi. TaskMetrics mostra qual task esta causando a dor.`

**SVG customizado**

Histograma com muitas barras pequenas em cyan e uma barra longa em laranja Spark, rotulada `long tail`.

**Regra didatica**

`TaskMetrics = O(tasks)`  
`usar quando houver suspeita, nao como reflexo automatico em job gigante`

---

## Slide T8 - Overhead e limites: leve nao significa gratis

**Titulo**

Leve porque reaproveita eventos. Nao gratis porque coleta dados.

**Mensagem central**

sparkMeasure evita instrumentar cada operador ou registro, mas ainda processa eventos, guarda estruturas no driver e pode exportar dados. A granularidade define o custo.

**Layout sugerido**

```text
+-------------------------------+-------------------------------------+
| StageMetrics = O(stages)      | TaskMetrics = O(tasks)               |
| barato e recorrente           | poderoso e volumoso                  |
+-------------------------------+-------------------------------------+
| event delivery                | listener processing                  |
| driver memory                 | external sink                        |
+-------------------------------+-------------------------------------+
```

**Alertas**

| Limite | O que significa |
|---|---|
| Driver memory | `TaskVals` demais podem pesar no driver |
| Listener processing | listener lento pressiona a fila de eventos |
| Concorrencia | `begin/end` mede delta; acoes simultaneas podem confundir |
| Cobertura | EventLog pode conter mais coisas do que o sparkMeasure agrega |
| Sink externo | Kafka/Influx/Prometheus/arquivo adicionam serializacao e I/O |

**Texto curto**

`Primeiro StageMetrics. Depois TaskMetrics quando a pergunta exigir granularidade.`

**SVG customizado**

Balanca tecnica: de um lado `baixo volume / recorrente`; do outro `alta granularidade / investigativo`.

---

## Slide T9 - Do diagnostico a pratica operacional

**Titulo**

Performance deixa de ser sensacao e vira evidencia comparavel

**Mensagem central**

Quando metricas entram no ciclo de engenharia, o time consegue comparar versoes, detectar regressao, explicar custo e decidir tuning com base em evidencia.

**Layout sugerido**

```text
+---------------------------------------------------------------------+
| run_id + commit + config + input_size + metricas                     |
|        -> baseline -> comparacao -> regressao -> tuning/alerta       |
+-------------------------------+-------------------------------------+
| antes                         | depois                               |
| "parece lento"                | shuffleWrite +70%                    |
| "talvez GC"                   | jvmGCTime por stage                  |
| "cluster ruim?"               | executorCpuTime vs executorRunTime   |
+-------------------------------+-------------------------------------+
```

**Tabela antes/depois**

| Antes | Com metricas |
|---|---|
| "Esse job parece lento" | `duration +42%` contra baseline |
| "Acho que foi shuffle" | `shuffleWrite +70%` depois do novo join |
| "Talvez GC" | `jvmGCTime` concentrado em dois stages |
| "Cluster ruim?" | `executorCpuTime` caiu, `executorRunTime` subiu |

**Texto curto**

`A saida deixa de ser opiniao sobre lentidao e vira comparacao tecnica entre execucoes.`

**SVG customizado**

Pipeline de engenharia:

```text
metrics per run -> baseline -> diff -> regression marker -> tuning decision
```

Usar verde terminal para evidencias, cyan para fluxo, laranja Spark para regressao/alerta.

---

## Observacoes para producao real

- A nova secao deve ser criada depois dos slides atuais, sem alterar visual ou estrutura dos slides aprovados.
- O navegador lateral deve ganhar novos pontos de navegacao apenas quando a producao HTML for aceita.
- Snippets e tabelas devem ser responsivos: em telas menores, virar blocos empilhados com fonte menor e scroll horizontal discreto apenas para tabela.
- Usar SVG inline ou arquivos em `assets/svg/` para facilitar ajuste fino.
- Placeholders devem ser sobrios: caixa escura, borda tracejada fina, texto `SUBSTITUIR ASSET` pequeno.
- O slide T5 e a ponte mais importante: se ele ficar claro, StageMetrics e TaskMetrics deixam de parecer "magic API".

## Fontes tecnicas usadas como base conceitual

- sparkMeasure README: https://github.com/LucaCanali/sparkMeasure
- Apache Spark monitoring/EventLog/History Server: https://spark.apache.org/docs/latest/monitoring.html
- SparkListenerInterface API: https://spark.apache.org/docs/latest/api/java/org/apache/spark/scheduler/SparkListenerInterface.html
- Spark Listener/EventLog mental model usado pela Spark UI e History Server.

