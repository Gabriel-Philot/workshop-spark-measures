# Guia do Lab 4: fingerprint de workload em nível de stage

Este guia organiza a execução em sala do Lab 4. O lab coleta StageMetrics,
normaliza contadores agregados e produz uma primeira hipótese operacional sobre
o workload.

Notas detalhadas:

[Notas de aula sobre o fingerprint](docs/stage_workload_fingerprint_class_notes.md)

Consulte-as para as definições completas dos ratios, a calibração local e as
limitações do `input_bytes`.

## Enquadramento da aula

- **Pergunta norteadora:** como transformar contadores agregados em uma primeira
  hipótese sobre o perfil operacional do workload?
- **Por que agora:** o Lab 3 estabeleceu o custo e os limites da coleta; o Lab 4
  explora o valor interpretativo da camada de stage.
- **Resultado de aprendizagem:** conectar profile, flags, métricas brutas,
  ratios e próximo passo sem confundir interpretação com prova de causa raiz.
- **Modo de condução:** exercício principal guiado e executado ao vivo;
  inspeções da persistência, MinIO e Spark History Server opcionais.

## 0. Pré-requisitos

Comece na raiz do repositório:

```bash
cd workshop-spark-measures
```

O Lab 4 reutiliza as fontes Bronze compartilhadas pelos Labs 1–6. Se for a
primeira execução do workshop, ou se imagens e dados do MinIO foram removidos,
siga as seções 1 a 5 do [guia do Lab 0](../lab_0/guide_lab0.md).

Se os dados continuam disponíveis e apenas os containers foram interrompidos:

```bash
make compose
```

Inputs esperados:

```text
s3a://lakehouse/bronze/retail/sales
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
```

> **Nota do instrutor:** `make down` preserva os dados do MinIO;
> `make clean-data` os remove. Não regenere as fontes antes de cada lab.

## 1. Entre na pasta do Lab 4

```bash
cd src/apps/labs/lab_4
```

Pontos de entrada:

```text
lab_4_stage_workload_fingerprint.py
run_stage_workload_fingerprint.sh
```

## 2. Estabeleça a pergunta diagnóstica

Pergunte à turma:

```text
Como este workload Spark se comporta a partir da perspectiva de execução em
nível de stage?
```

O objetivo não é automatizar root-cause analysis. O fingerprint organiza
StageMetrics em um vocabulário operacional capaz de produzir estes profiles:

- `SHUFFLE_HEAVY`;
- `MEMORY_PRESSURE`;
- `IO_HEAVY_SCAN`;
- `GC_PRESSURE`;
- `MANY_SMALL_TASKS`;
- `LOW_PARALLELISM_SIGNAL`;
- `BALANCED_OR_LOW_SIGNAL`.

O profile é apenas a hipótese prioritária. As demais flags continuam
importantes e podem apontar sinais simultâneos. Spark UI, plano e TaskMetrics
permanecem disponíveis quando os agregados não respondem à pergunta.

## 3. Localize os controles da aula

Abra:

```text
lab_4_stage_workload_fingerprint.py
lab_4_utils/experiments.yaml
lab_4_utils/fingerprint_rules.yaml
lab_4_utils/fingerprint.py
```

O app mantém um único seletor visível:

```python
CONFIG_NAME = os.environ.get(
    "LAB4_CONFIG_NAME",
    "lab4-stage-workload-fingerprint",
)
```

`experiments.yaml` define os quatro inputs, `96` shuffle partitions, `512`
fingerprint buckets, StageMetrics como único collector e os outputs Delta.
`fingerprint_rules.yaml` separa as decisões dos contadores coletados.

Os thresholds são deliberadamente simples e específicos desta calibração. Eles
não são SLAs nem limites universais. Durante a aula, reconstrua cada flag usando
o valor da métrica e a regra correspondente no YAML.

## 4. Entenda o workload antes do profile

O app executa:

```text
sales
  -> select the required fact columns
  -> derive deterministic fingerprint_bucket
  -> join vendors, products, and customers
  -> repartition by fingerprint_bucket
  -> aggregate by bucket, regions, category, and month
  -> repartition by the final business keys
  -> aggregate the final summary
  -> write Delta
```

Os joins, repartitions e as duas agregações criam sinais de shuffle e tasks
suficientes para a classificação em nível de stage.

O output de negócio contém:

```text
vendor_region
customer_region
category_id
sale_year_month
sale_count
customer_count
product_count
total_quantity
gross_sales_amount
average_sale_amount
fingerprint_bucket_count
```

> **Nota do instrutor:** leia a transformação antes do resultado. As regras
> classificam sintomas; o código e o plano mostram quais operações podem ter
> produzido esses sintomas.

## 5. Execute o fingerprint

```bash
bash run_stage_workload_fingerprint.sh
```

O wrapper realiza um `spark-submit`, preserva o output da aplicação e falha se o
app ou algum marker obrigatório falhar.

Markers esperados:

```text
LAB4_STAGE_METRICS_CAPTURED_OK
LAB4_WORKLOAD_FINGERPRINT_RULES_OK
LAB4_WORKLOAD_PROFILE_ASSIGNED_OK
LAB4_WORKLOAD_FINGERPRINT_WRITTEN_OK
```

Marker final do runner:

```text
LAB4_FINGERPRINT_COMPLETED output=s3a://observability/lab4/workload_fingerprints
```

## 6. Leia o bloco diagnóstico como evidência

Ao final do submit, o app imprime:

```text
## STAGE WORKLOAD FINGERPRINT DIAGNOSTIC

### Profile
workload_profile: ...
diagnostic_flags: ...

### StageMetrics signals
num_stages: ...
num_tasks: ...
executor_run_time_ms: ...
input_bytes: ...
shuffle_bytes_read: ...
shuffle_bytes_written: ...
memory_bytes_spilled: ...
disk_bytes_spilled: ...
jvm_gc_time_ms: ...

### Normalized ratios
shuffle_amplification_ratio: ...
gc_time_ratio: ...
spill_ratio: ...
task_density_score: ...

### Recommended next step
...
```

Leia sempre na ordem:

1. identifique o `workload_profile`;
2. localize todas as `diagnostic_flags`;
3. reconstrua cada flag usando os contadores e thresholds do YAML;
4. inspecione a ordem de precedência em `classify_workload()` para explicar por
   que uma flag foi escolhida como profile principal;
5. verifique se os denominadores dos ratios são confiáveis;
6. trate a recomendação como próxima investigação, não como correção automática.

Não comece pela recomendação. Um fingerprint só é explicável quando a turma
consegue reconstruir suas flags a partir das métricas e regras. O YAML define
thresholds, mas não a precedência entre profiles. Quando várias flags são
verdadeiras, `classify_workload()` escolhe o profile principal por uma ordem
explícita no código; ele não representa necessariamente o maior valor numérico
observado.

### Checkpoint de raciocínio — fingerprint

- **Pergunta:** qual profile é compatível com a relação observada entre input,
  shuffle, spill, GC, tasks e executor runtime?
- **Hipótese:** regras simples podem resumir o primeiro sinal operacional e
  indicar qual evidência investigar em seguida.
- **Evidência:** profile, flags, contadores brutos, disponibilidade das métricas,
  ratios normalizados e thresholds acionados.
- **Conclusão:** o fingerprint é uma primeira hipótese explicável quando as
  flags podem ser reconstruídas a partir das métricas e thresholds, e o profile
  principal pode ser explicado pela precedência explícita do classificador.
- **Limitação:** os thresholds são locais; o fingerprint não localiza uma task,
  não lê o plano e não prova causa raiz.

## 7. Interprete os ratios com cuidado

O app deriva:

```text
shuffle_amplification_ratio = total shuffle bytes / input bytes
gc_time_ratio                = JVM GC time / executor runtime
spill_ratio                  = spill bytes / largest available volume signal
task_density_score           = tasks / stages
```

`task_density_score` usa a quantidade agregada de tasks observada por
StageMetrics. Ele não examina duração ou distribuição de tasks; portanto, uma
flag de task overhead é uma pista grosseira, não prova de many-small-tasks.

`input_bytes` corresponde ao contador `bytesRead` reportado por StageMetrics,
não ao tamanho físico da tabela Delta. O Lab 4 ainda não possui um campo
explícito de disponibilidade para essa métrica. O normalizador converte um
contador ausente ou `None` para `0` e, por segurança, o classificador trata
qualquer zero como indisponível para o ratio.

Essa é uma convenção conservadora do Lab 4, não uma prova de que todo valor zero
seja realmente uma métrica ausente. A distinção explícita entre zero real e
indisponibilidade será tratada pelo contrato de métricas do Lab 6.

Na prática, o classificador separa:

- `input_bytes = 0`: `INPUT_BYTES_UNAVAILABLE_FOR_RATIO`; o zero representa
  indisponibilidade para esse cálculo segundo a convenção do Lab 4, não ausência
  comprovada de leitura;
- `0 < input_bytes < minimum_reliable_input_bytes`:
  `INPUT_BYTES_LOW_CONFIDENCE_FOR_RATIO`; o ratio pode ser exibido, mas não
  aciona `HIGH_SHUFFLE_AMPLIFICATION`.

Quando o denominador não é confiável, `HIGH_SHUFFLE_VOLUME` permite interpretar
o volume absoluto de shuffle sem inventar precisão na amplification. A
calibração e os valores locais completos permanecem nas
[notas de aula](docs/stage_workload_fingerprint_class_notes.md#ratios-used-by-the-lab).

## 8. Caminhos opcionais

### 8.1 Inspecione a evidência persistida

O output de negócio é sobrescrito para manter apenas o resultado atual:

```text
s3a://lakehouse/gold/lab4/stage_workload_fingerprint/workload_summary
```

As linhas normalizadas de StageMetrics são adicionadas a:

```text
s3a://observability/lab4/stage_metrics
```

Os fingerprints interpretados são adicionados a:

```text
s3a://observability/lab4/workload_fingerprints
```

As duas tabelas de observabilidade compartilham `run_id`, `application_id`,
identidade do workload e `created_at`, preservando evidência e interpretação.

MinIO Console:

```text
http://127.0.0.1:29011
```

Credenciais locais padrão:

```text
user:     sparkworkshop
password: sparkworkshop123
```

### 8.2 Correlacione com o Spark History Server

```text
http://127.0.0.1:28090
```

Use `application_id` e o application name impresso pelo submit para localizar a
run. A UI oferece plano, jobs, stages e executors; o fingerprint apenas condensa
os agregados e não substitui essa evidência.

### 8.3 Execute o submit manual

Comando equivalente a partir da pasta do Lab 4:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB4_CONFIG_NAME=lab4-stage-workload-fingerprint \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_4/lab_4_stage_workload_fingerprint.py
```

### 8.4 Cleanup depois da aula

Retorne à raiz do repositório:

```bash
cd ../../../..
make down
```

Isso interrompe os containers e preserva os dados do MinIO. Use
`make clean-data` somente quando a próxima execução precisar começar com storage
vazio.

## Conclusão da aula

StageMetrics não precisa terminar como uma lista de contadores. Regras
explícitas podem transformar esses sinais em um fingerprint leve, reproduzível
e útil para alinhar a próxima pergunta de engenharia.

O resultado continua sendo uma hipótese. Observação, interpretação e prova de
causa raiz são níveis diferentes de evidência.

## Ponte para a próxima aula

O Lab 5 transforma essa hipótese operacional em uma regra de promoção: compara
um baseline com um candidate e decide se a mudança permanece dentro de um
runtime budget.
