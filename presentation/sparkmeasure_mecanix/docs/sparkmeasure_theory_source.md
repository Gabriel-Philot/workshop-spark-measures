Slide 1 — O que o Spark já gera naturalmente
Ideia central

Antes do sparkMeasure existir, o Spark já produzia uma trilha rica de execução. Essa trilha vem de várias camadas:

Código / SQL / DataFrame
        ↓
Catalyst / QueryExecution
        ↓
Logical Plan / Optimized Logical Plan / Physical Plan
        ↓
DAGScheduler
        ↓
Jobs / Stages
        ↓
TaskScheduler / Executors
        ↓
Tasks / TaskInfo / TaskMetrics / Accumulators
        ↓
SparkListenerEvents
        ↓
Spark UI / EventLog / History Server / REST API / Metrics System

O sparkMeasure nasce explorando essa infraestrutura já existente. Ele não precisa “entrar dentro” de cada operador do Spark manualmente; ele escuta os eventos que o próprio Spark emite durante a execução.

1. Código Spark / SQL / DataFrame: o ponto de partida

Quando o aluno escreve algo como:

df = spark.read.parquet("/data/orders")
result = (
    df.filter("country = 'BR'")
      .groupBy("vendor_id")
      .count()
)
result.show()

há duas fases diferentes:

Definição da transformação
        ↓
Criação de plano
        ↓
Ação dispara execução

O Spark SQL/DataFrame trabalha sobre uma representação de consulta. Quando você chama explain, o Spark consegue expor essa representação em diferentes níveis. O EXPLAIN EXTENDED, por exemplo, mostra plano lógico parseado, plano lógico analisado, plano lógico otimizado e plano físico; já o EXPLAIN FORMATTED separa outline físico e detalhes dos nós físicos.

O ponto importante para aula: plano não é métrica de execução. O plano diz “como o Spark pretende executar”. Métricas dizem “o que aconteceu de verdade”. Essa diferença é central para entender por que sparkMeasure não substitui explain(), e sim complementa.

2. Catalyst / QueryExecution: quem gera planos lógicos e físicos

Para DataFrames e SQL, o Spark passa por uma cadeia parecida com:

Parsed Logical Plan
        ↓
Analyzed Logical Plan
        ↓
Optimized Logical Plan
        ↓
Physical Plan
        ↓
Executed Plan

O plano parseado ainda tem referências não resolvidas. O plano analisado resolve colunas, tabelas, tipos e relações. O plano otimizado aplica regras de otimização. Depois o Spark escolhe estratégias físicas, como broadcast hash join, sort merge join, hash aggregate, exchange, scan, project e filter. O documento oficial de EXPLAIN descreve exatamente essa separação entre plano lógico parseado, analisado, otimizado e físico.

Para aula, vale mostrar que isso responde perguntas como:

O Spark vai fazer shuffle?
O join virou broadcast?
Existe Exchange?
Existe Sort?
Existe WholeStageCodegen?
Existe scan em excesso?
O plano mudou com AQE?

Mas isso ainda não responde:

Quanto tempo cada stage levou?
Quanto foi CPU real?
Quanto teve de GC?
Quanto spill aconteceu?
Quanto shuffle realmente foi lido/escrito?
Alguma task ficou muito mais lenta que as outras?

Essas respostas vêm depois, com eventos e métricas de runtime.

3. Physical Plan / SparkPlan / AQE: plano físico pode mudar em runtime

O plano físico é a ponte entre “consulta declarativa” e “execução distribuída”. Em Spark SQL, os nós físicos geram jobs, stages e tasks quando uma ação realmente dispara a execução.

Com Adaptive Query Execution, existe uma camada adicional: o plano físico inicial pode ser ajustado durante a execução. No código do Spark SQL UI existe o evento SparkListenerSQLAdaptiveExecutionUpdate, carregando executionId, descrição do plano físico e SparkPlanInfo. Isso é importante porque o que você viu no plano inicial pode não ser exatamente o plano final executado.

Para os alunos, a mensagem é:

EXPLAIN é intenção/plano.
Spark UI/EventLog/sparkMeasure são execução observada.
AQE pode mudar o plano durante a execução.
4. DAGScheduler: quem transforma execução em jobs e stages

O DAGScheduler é a peça que quebra a computação em jobs e stages. Ele observa dependências entre RDDs e cria fronteiras de stage principalmente onde há shuffle. A documentação de StageInfo diz que cada stage está associado a um ou mais RDDs, e que a fronteira de um stage é marcada por dependências de shuffle.

Isso é o que faz aparecer na Spark UI:

Job 0
  Stage 0
  Stage 1
  Stage 2

Job 1
  Stage 3
  Stage 4

O StageInfo é a entidade usada para passar informações de stage do scheduler para os SparkListeners. Ele carrega dados como stageId, attemptId, nome, número de tasks, RDDs, parents, detalhes, métricas, horário de submissão, horário de conclusão, motivo de falha e acumuladores.

Para o slide 1, isso é um bloco importante:

DAGScheduler gera:
- JobStart / JobEnd
- StageSubmitted / StageCompleted
- StageInfo
- relação Job ↔ Stage
- fronteiras de shuffle
- metadados de DAG
5. TaskScheduler / TaskSetManager: quem materializa tasks

Depois que o DAGScheduler define os stages, cada stage vira um conjunto de tasks. Normalmente, uma task representa o trabalho sobre uma partição.

O TaskInfo representa uma tentativa de task dentro de um TaskSet. Ele contém dados como taskId, índice da task dentro do task set, partitionId, launchTime, finishTime, executorId, host, localidade, se a task é especulativa, status, duração e acumuladores intermediários.

Para aula, isso ajuda a explicar a diferença:

Stage = bloco lógico/físico de execução entre shuffles.
Task = unidade paralela concreta dentro do stage.
Partition = pedaço de dados processado por uma task.
Task attempt = tentativa específica de executar aquela task.

Isso importa para skew: o stage pode parecer “um stage lento”, mas o problema real pode ser uma ou poucas tasks muito maiores ou muito mais lentas que as outras.

6. Executors: quem gera TaskMetrics de verdade

As métricas mais valiosas vêm da execução real das tasks nos executors. Quando uma task termina, o evento SparkListenerTaskEnd carrega:

stageId
stageAttemptId
taskType
reason
TaskInfo
ExecutorMetrics
TaskMetrics

Isso aparece diretamente na documentação da classe SparkListenerTaskEnd.

É aqui que entram sinais como:

executorRunTime
executorCpuTime
executorDeserializeTime
executorDeserializeCpuTime
resultSerializationTime
jvmGCTime
resultSize
memoryBytesSpilled
diskBytesSpilled
peakExecutionMemory
input bytes/records
output bytes/records
shuffle read bytes/records/blocks/fetch wait
shuffle write bytes/records/write time

Esse é o ouro do sparkMeasure. Ele pega essas métricas que já existem no Spark e transforma em relatório agregado, DataFrame e saída para análise.

7. Accumulators e SQL Metrics: métricas internas de operadores

Além de TaskMetrics, o Spark também usa acumuladores para métricas internas. No TaskInfo, aparecem acumuladores intermediários atualizados durante a task. A documentação avisa inclusive que o mesmo acumulador pode aparecer mais de uma vez em uma task, ou que acumuladores com mesmo nome e IDs diferentes podem coexistir.

No mundo Spark SQL, isso se conecta com métricas como:

number of output rows
scan time
duration
shuffle records
spill size

Essas métricas alimentam a aba SQL da Spark UI. No código do Spark SQL UI, eventos como SparkListenerDriverAccumUpdates atualizam métricas de SQL no driver usando executionId e pares de accumulator id/value.

Mensagem para aula:

TaskMetrics = métricas gerais da execução da task.
SQL Metrics = métricas associadas aos operadores físicos do Spark SQL.
Accumulators = mecanismo usado para carregar/atualizar várias dessas medições.
8. Spark UI: consumidor nativo dos eventos

Todo SparkContext sobe uma Web UI, por padrão na porta 4040. A documentação oficial diz que essa UI mostra lista de stages e tasks, resumo de tamanhos de RDD e memória, informações de ambiente e executors.

A Spark UI é um consumidor dos eventos do Spark. Ela não está “adivinhando” o que aconteceu. Ela reconstrói visualmente o estado da aplicação a partir de eventos e dados mantidos no driver.

Por isso, quando você vê:

Jobs
Stages
Tasks
Storage
Environment
Executors
SQL

você está vendo uma materialização visual dos eventos e métricas que o Spark já produziu.

9. EventLog: persistência dos eventos

Por padrão, a UI só existe enquanto a aplicação está viva. Para olhar depois, você habilita o EventLog com:

spark.eventLog.enabled true
spark.eventLog.dir hdfs://.../spark-logs

A documentação oficial diz que isso faz o Spark registrar eventos que codificam a informação exibida na UI em armazenamento persistente.

O EventLog é essencial porque ele transforma runtime em histórico. Sem ele, você tem observabilidade enquanto o job está vivo; com ele, você tem análise pós-morte.

Para aula:

Spark UI = visão live.
EventLog = trilha persistida.
History Server = replay visual dessa trilha.
sparkMeasure = coleta e agrega parte dessa trilha para análise operacional.
10. History Server: replay do EventLog

O History Server consegue reconstruir a UI de aplicações se os event logs existirem. A documentação oficial diz que ele carrega event logs a partir de um diretório configurado e cria uma interface para aplicações completas e incompletas.

Isso é muito próximo da mentalidade SRE:

Sem histórico:
"Esse job está lento agora."

Com histórico:
"Esse job ficou 40% mais lento depois da versão X."
"O shuffle dobrou."
"O spill começou ontem."
"A regressão apareceu quando alteramos a estratégia de join."

O sparkMeasure facilita exatamente essa virada: sair do diagnóstico visual manual e ir para análise programática.

11. Metrics System: não confundir com SparkListener

O Spark também tem um sistema de métricas baseado em Dropwizard. A documentação oficial separa instâncias como driver, executor, master, worker, shuffleService e permite sinks como JMX, CSV, Graphite, StatsD, servlet JSON e Prometheus servlet.

Esse sistema é diferente do Listener/EventLog:

SparkListener/EventLog:
- eventos discretos de aplicação
- task start/end
- stage submitted/completed
- job start/end
- SQL execution start/end
- usado pela UI e History Server

Metrics System:
- gauges/counters/timers por componente
- driver/executor/jvm/shuffle service
- bom para monitoramento contínuo
- exposto via sinks

Para aula, vale reforçar:

sparkMeasure é majoritariamente Listener-based.
Não é só um exporter de JMX.
Ele trabalha no nível semântico de job/stage/task.
Slide 2A — Como os eventos nascem e passam pelo ListenerBus
Ideia central

O caminho interno é:

Executor / TaskScheduler / DAGScheduler / SQLExecution
        ↓
SparkListenerEvent
        ↓
LiveListenerBus / AsyncEventQueue
        ↓
SparkListener callbacks
        ↓
UI / EventLog / History / sparkMeasure / sinks customizados

O SparkListenerInterface é a interface para ouvir eventos do scheduler. A própria documentação recomenda que aplicações normalmente estendam SparkListener ou SparkFirehoseListener, em vez de implementar a interface diretamente. Ela também avisa que é uma interface interna e pode mudar entre versões.

1. SparkListenerEvent: o envelope de evento

Cada acontecimento relevante vira um evento:

SparkListenerApplicationStart
SparkListenerApplicationEnd
SparkListenerJobStart
SparkListenerJobEnd
SparkListenerStageSubmitted
SparkListenerStageCompleted
SparkListenerTaskStart
SparkListenerTaskEnd
SparkListenerExecutorAdded
SparkListenerExecutorRemoved
SparkListenerExecutorMetricsUpdate
SparkListenerBlockUpdated
SparkListenerSQLExecutionStart
SparkListenerSQLExecutionEnd
SparkListenerSQLAdaptiveExecutionUpdate

A classe SparkListener tem callbacks específicos como onJobStart, onStageSubmitted, onStageCompleted, onTaskStart, onTaskEnd, onExecutorMetricsUpdate e onOtherEvent para eventos SQL ou eventos não cobertos pelos callbacks principais.

Mensagem para o aluno:

O listener não fica fazendo polling.
O Spark publica eventos.
O listener reage quando esses eventos passam pelo bus.
2. DAGScheduler publica eventos de job/stage/task

No código do DAGScheduler, a task iniciada, task em fase de busca de resultado, task finalizada e heartbeat de executor são encaminhados para o fluxo de eventos. O trecho de código mostra métodos como taskStarted, taskGettingResult, taskEnded e executorHeartbeatReceived; no heartbeat, o scheduler posta SparkListenerExecutorMetricsUpdate.

Isso quer dizer que o Spark tem um canal interno onde acontecimentos da execução são convertidos em mensagens.

Exemplo conceitual:

Task terminou no executor
        ↓
TaskScheduler reporta completion
        ↓
DAGScheduler recebe CompletionEvent
        ↓
Spark gera SparkListenerTaskEnd
        ↓
ListenerBus entrega para listeners
        ↓
Spark UI / EventLog / sparkMeasure recebem
3. LiveListenerBus / AsyncEventQueue: distribuição assíncrona

O Spark usa um mecanismo de event bus para entregar eventos aos listeners. O AsyncEventQueue é descrito no código como uma fila assíncrona: eventos postados nessa fila são entregues aos listeners filhos em uma thread separada. O mesmo código mostra que existe uma capacidade configurável para evitar crescimento infinito da fila e OOM quando eventos entram mais rápido do que são drenados.

Esse detalhe é essencial para explicar overhead:

Não é porque é assíncrono que é grátis.
Se o listener processa devagar demais:
- a fila cresce
- o driver sofre
- eventos podem atrasar
- memória pode virar gargalo

Mas também explica por que o sparkMeasure consegue ser leve em muitos cenários:

Ele não bloqueia diretamente cada operador Spark.
Ele recebe eventos já emitidos pelo runtime.
O custo principal está em transportar, processar, agregar e armazenar esses eventos no driver.
4. SparkListener callbacks: onde ferramentas entram

Uma ferramenta baseada em listener escolhe quais callbacks implementar.

Exemplo conceitual:

class MyListener extends SparkListener {
  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    // coletar StageInfo + métricas agregadas
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    // coletar TaskInfo + TaskMetrics
  }
}

A documentação do SparkListener mostra exatamente esses callbacks: onStageCompleted é chamado quando um stage completa com sucesso ou falha, onTaskEnd quando uma task termina, onExecutorMetricsUpdate quando o driver recebe métricas de task de um executor via heartbeat.

Essa é a base mental para entender o sparkMeasure.

Slide 2B — Como o sparkMeasure se encaixa nesse fluxo
Ideia central

O sparkMeasure é um conjunto de listeners + APIs de conveniência + estruturas de agregação.

Spark runtime
   ↓ emite eventos
ListenerBus
   ↓ entrega eventos
sparkMeasure Listener
   ↓ achata dados
ListBuffer no driver
   ↓ agrega
relatório / dict / DataFrame / arquivo / sink externo

O próprio projeto descreve o sparkMeasure como uma ferramenta para facilitar medição e troubleshooting de jobs Spark, focada em coletar e analisar métricas Spark. O README também lista usos como troubleshooting interativo, integração com desenvolvimento/CI-CD, Flight Recorder, monitoramento com Kafka/Prometheus/InfluxDB e exemplo educacional de Spark Listeners.

1. Modos principais de uso

Existem dois modos mentais:

Modo interativo/instrumentado:
- você cria StageMetrics ou TaskMetrics
- chama begin()
- executa o trecho Spark
- chama end()
- imprime/consulta/agrega métricas

Modo Flight Recorder:
- você anexa listener via configuração
- o job roda sem mudar código
- métricas são gravadas/exportadas automaticamente

Na documentação Python, o uso aparece como:

from sparkmeasure import StageMetrics

stagemetrics = StageMetrics(spark)
stagemetrics.begin()
spark.sql("...").show()
stagemetrics.end()

stagemetrics.print_report()
metrics = stagemetrics.aggregate_stagemetrics()

A documentação mostra esse fluxo explicitamente.

2. StageMetrics: listener em nível de stage

A documentação de referência mostra que StageMetrics coleta métricas em granularidade de stage e fornece funções de agregação e relatório. Ela lista métricas como número de stages, número de tasks, elapsed time, stage duration, executor run time, executor CPU time, deserialize time, GC time, shuffle, spill, leitura e escrita.

Internamente, o StageMetrics se apoia em eventos como:

onJobStart
onStageCompleted
onExecutorMetricsUpdate

A documentação de API mostra que o método onStageCompleted dispara no fim do stage e coleta métricas achatadas em stageMetricsData, além de registrar mapeamentos como stage id para job id/job group.

Fluxo conceitual:

Stage começou
        ↓
Spark emite StageSubmitted
        ↓
Tasks executam nos executors
        ↓
Stage completa
        ↓
Spark emite StageCompleted(StageInfo)
        ↓
sparkMeasure lê StageInfo / TaskMetrics agregado
        ↓
salva StageVals no driver
        ↓
gera relatório agregado

O StageMetrics é bom para responder:

Quantos stages?
Quantas tasks?
Quanto tempo total?
Quanto tempo executor?
Quanto tempo CPU?
Quanto GC?
Quanto shuffle?
Quanto spill?
Quanto input/output?

Ele é ruim para responder sozinho:

Qual task específica foi straggler?
A distribuição de tasks é assimétrica?
Existe skew dentro do stage?
Qual executor concentrou tasks lentas?
3. TaskMetrics: listener em nível de task

A documentação de referência mostra que TaskMetrics coleta dados em granularidade de task, sendo mais fino que StageMetrics e potencialmente coletando muito mais dados. Ela também mostra que TaskMetrics implementa onTaskEnd e armazena dados em taskMetricsData: ListBuffer[TaskVals].

Fluxo conceitual:

Task termina
        ↓
Spark emite SparkListenerTaskEnd
        ↓
evento contém TaskInfo + TaskMetrics
        ↓
sparkMeasure extrai campos relevantes
        ↓
salva TaskVals no driver
        ↓
permite análise por task

Com TaskMetrics, você consegue estudar:

stragglers
skew
long tail
tasks especulativas
tasks por executor
distribuição de executorRunTime
distribuição de executorCpuTime
diferença CPU time vs wall-clock
shuffle read concentrado em poucas tasks
spill concentrado em poucas tasks
GC concentrado em poucos executors/tasks

Para aula, a frase boa é:

StageMetrics mostra o raio-X do stage.
TaskMetrics mostra a tomografia das tasks.
Slide 3 — Como ele mede sem adicionar muita latência
Ideia central

O sparkMeasure tende a ser leve porque ele aproveita dados que o Spark já calcula e eventos que o Spark já emite. Ele não precisa envolver manualmente cada transformação, nem interceptar cada linha, nem medir registro a registro.

Mas isso não significa “zero overhead”.

O blog original do Luca Canali explica que o sparkMeasure é baseado na interface Spark Listener; listeners transportam Task Metrics dos executors para o driver, a mesma instrumentação usada por Spark Web UI e History Server. Também explica que as métricas podem ser coletadas em granularidade de stage ou task, e que são achatadas e armazenadas em estruturas locais no driver.

1. Por que stage-level é barato

Stage-level gera volume proporcional a stages:

O(number_of_stages)

Exemplo:

Job com 8 stages
→ sparkMeasure armazena ~8 registros StageVals

Mesmo que cada stage tenha milhares de tasks, a análise stage-level trabalha com dados agregados por stage. Por isso é adequado para uso frequente em notebook, benchmark, CI/CD e comparação de versões.

Esse é o modo que eu ensinaria como default:

Primeiro StageMetrics.
Só vá para TaskMetrics se houver suspeita concreta de skew, long tail ou comportamento anômalo.
2. Por que task-level pode ficar caro

Task-level gera volume proporcional a tasks:

O(number_of_tasks)

Exemplo:

Job com 8 stages
Cada stage com 20.000 tasks
→ 160.000 registros TaskVals

Cada task finalizada emite evento. Cada evento precisa ser entregue pelo listener bus. O sparkMeasure precisa processar e armazenar esses dados no driver. O blog do Luca alerta que métricas são coletadas no driver e que o driver pode virar gargalo; também diz que, na versão descrita, o sparkMeasure bufferiza os dados em memória no driver.

A documentação de Flight Recorder para InfluxDB reforça a diferença de volume: InfluxDBSink gera dados relativamente pequenos na maioria das aplicações, em ordem de número de stages; já InfluxDBSinkExtended, que registra cada task, pode gerar muito dado em ordem de número de tasks e deve ser usado com cuidado.

Essa é uma parte que vale colocar sem suavizar:

TaskMetrics é poderoso, mas não é para ligar sempre em qualquer job monstro.
Ele pode virar problema de observabilidade criando mais pressão no driver.
3. Onde exatamente aparece overhead

O overhead pode aparecer em quatro lugares:

1. Publicação e entrega de eventos
2. Processamento do listener
3. Armazenamento no driver
4. Exportação/sink externo
1. Publicação e entrega de eventos

O Spark já publica eventos para UI/EventLog. O sparkMeasure adiciona mais um consumidor. Em stage-level, isso costuma ser barato. Em task-level, o número de eventos é muito maior.

2. Processamento do listener

Cada evento recebido precisa ser transformado:

SparkListenerTaskEnd
        ↓
extrai TaskInfo
        ↓
extrai TaskMetrics
        ↓
normaliza campos
        ↓
cria TaskVals
        ↓
append em ListBuffer
3. Armazenamento no driver

O driver guarda as estruturas locais. Isso é confortável para stage-level, mas pode pesar para task-level em jobs com muitas tasks. O próprio material do Luca destaca que os dados são achatados e coletados em memória local no driver.

4. Exportação para sink

Quando você exporta para Kafka, InfluxDB, Prometheus Pushgateway ou arquivo, existe custo de serialização e I/O. O README atual lista integrações com Kafka, Prometheus Push Gateway, Prometheus JMX Exporter e InfluxDB.

Slide 4 — StageMetrics vs TaskMetrics
StageMetrics

Use quando você quer responder:

O job piorou?
O shuffle aumentou?
O spill apareceu?
O GC está alto?
O tempo de executor aumentou?
A CPU está sendo bem usada?
O volume lido/escrito mudou?
A versão nova é mais cara que a anterior?

Ele é ideal para:

benchmark
comparação entre versões
CI/CD de performance
baseline por job
observabilidade recorrente
diagnóstico inicial

Formato mental:

1 linha por stage
métricas agregadas
baixo volume
bom para painel e regressão
TaskMetrics

Use quando StageMetrics aponta cheiro de problema, mas não explica a causa.

Perguntas que TaskMetrics responde melhor:

Existe skew?
Uma task ficou 10x mais lenta?
Poucas tasks leram a maior parte do shuffle?
O spill está concentrado?
O GC está concentrado em executor específico?
A distribuição de runtime tem long tail?
Speculative execution ajudou ou mascarou o problema?

Formato mental:

1 linha por task
métricas granulares
alto volume
bom para investigação pontual
Regra didática

Eu colocaria literalmente assim:

StageMetrics:
"onde está doendo?"

TaskMetrics:
"qual task está causando a dor?"

StageMetrics:
uso recorrente, barato, operacional.

TaskMetrics:
uso cirúrgico, investigativo, com cuidado.
Slide 5 — O insight SRE do Luca / CERN
Ideia central

A virada SRE é parar de olhar performance como sensação e começar a tratar performance como dado versionado.

Sem sparkMeasure ou algo equivalente:

"Esse job parece lento."
"Ontem rodou mais rápido."
"Acho que foi shuffle."
"Talvez o cluster estava ruim."

Com métricas persistidas:

run_id
git_commit
job_name
cluster_policy
runtime_version
input_size
num_stages
num_tasks
executorRunTime
executorCpuTime
jvmGCTime
shuffleBytesRead
shuffleBytesWritten
memoryBytesSpilled
diskBytesSpilled
recordsRead
recordsWritten

Agora vira engenharia:

A versão B aumentou shuffle write em 70%.
O spill começou depois do novo join.
O CPU time caiu mas wall-clock subiu: possível espera de I/O/shuffle.
O job está com long tail em 3 tasks.
O runtime mudou e alterou o plano físico.
O custo subiu sem aumento proporcional de volume.

O README atual do sparkMeasure explicita usos como troubleshooting interativo, integração com desenvolvimento/CI-CD para comparar métricas sob diferentes configurações ou mudanças de código, Flight Recorder para batch jobs e monitoramento com sistemas externos.

Como eu transformaria isso em sequência visual
Slide 1 — Spark já deixa rastros

Visual:

Código → Planos → Jobs → Stages → Tasks → Metrics → UI/EventLog

Mensagem:

O Spark já gera planos, eventos e métricas.
O problema é transformar isso em diagnóstico rápido.
Slide 2 — O caminho dos eventos

Visual:

Executors / DAGScheduler / SQLExecution
        ↓
SparkListenerEvent
        ↓
LiveListenerBus / AsyncEventQueue
        ↓
SparkListeners
        ↓
UI / EventLog / sparkMeasure

Mensagem:

sparkMeasure entra no barramento de eventos do Spark.
Slide 3 — sparkMeasure StageMetrics

Visual:

StageCompleted
        ↓
StageInfo + métricas agregadas
        ↓
StageVals no driver
        ↓
report/DataFrame/baseline

Mensagem:

StageMetrics é a visão operacional barata.
Slide 4 — sparkMeasure TaskMetrics

Visual:

TaskEnd
        ↓
TaskInfo + TaskMetrics
        ↓
TaskVals por task
        ↓
skew / straggler / long tail

Mensagem:

TaskMetrics é granular, poderoso e mais caro.
Slide 5 — Engenharia/SRE

Visual:

Métricas por execução
        ↓
baseline
        ↓
comparação
        ↓
regressão
        ↓
alerta / dashboard / decisão de tuning

Mensagem:

Performance vira contrato observável.
Minha recomendação prática para sua aula

Eu não deixaria isso em 3 slides. Ficaria apertado e misturaria conceitos. A estrutura boa é:

1. Spark nativo: planos, UI, EventLog, History, métricas
2. Listener internals: eventos, ListenerBus, callbacks
3. sparkMeasure StageMetrics: como coleta e agrega
4. sparkMeasure TaskMetrics: granularidade, skew e overhead
5. SRE/engenharia: baseline, regressão, custo e observabilidade