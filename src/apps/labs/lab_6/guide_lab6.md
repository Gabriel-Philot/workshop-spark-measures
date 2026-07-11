# Guia do Lab 6: contract gate para StageMetrics

Este guia organiza a execução em sala do Lab 6. Não estamos testando se o
sparkMeasure funciona nem validando a precisão interna de seus contadores. O
lab verifica se a linha normalizada, enriquecida e persistida preserva
contexto e metadata suficientes para que um consumidor automatizado decida se
pode utilizá-la.

Notas detalhadas:

[Notas de aula sobre o StageMetrics contract gate](docs/stage_metrics_contract_gate_class_notes.md)

Consulte-as para disponibilidade de métricas, tipos de falha, premissas dos
consumidores e o racional de data product.

## Enquadramento da aula

- **Pergunta norteadora:** esta linha normalizada preserva informação suficiente
  para uma automação distinguir valor real, métrica indisponível, produtor
  incompatível e execução duplicada?
- **Por que agora:** o Lab 5 mostrou a policy primeiro; o Lab 6 volta um passo e
  questiona se a evidência integrada merece confiança.
- **Resultado de aprendizagem:** validar schema, valores, disponibilidade e
  correlação antes de usar telemetria em decisões.
- **Modo de condução:** dois experimentos principais ao vivo — cenário limpo e
  falha controlada — conduzidos pelo instrutor com participação da turma.

Uma aplicação isolada usando diretamente uma versão fixada do sparkMeasure tende
a receber a mesma estrutura de métricas. O contrato ganha valor quando várias
aplicações normalizam, enriquecem e publicam essas métricas em um produto
central. Nessas etapas, perda de campos, placeholders incorretos, identidades
ausentes, scopes incompatíveis e duplicidade podem transformar uma métrica
válida em uma linha operacionalmente inadequada.

## 0. Pré-requisitos

Comece na raiz do repositório:

```bash
cd workshop-spark-measures
```

O Lab 6 reutiliza as fontes Bronze compartilhadas pelos Labs 1–6. Se for a
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

## 1. Entre na pasta do Lab 6

```bash
cd src/apps/labs/lab_6
```

Pontos de entrada:

```text
lab_6_stage_metrics_contract_gate.py
run_stage_metrics_contract_gate.sh
```

## 2. Compare a ordem do workshop com a ordem de produção

O workshop escolhe uma ordem didática:

```text
Lab 5: usar StageMetrics como policy
  -> Lab 6: questionar a confiança na telemetria que alimentou a policy
```

Uma plataforma de produção deve aplicar a ordem operacional:

```text
coletar métricas
  -> validar contract
  -> aplicar policy
  -> decidir a promoção
```

O Lab 6 não repete o runtime budget. Ele torna linhas inadequadas detectáveis e
registra uma decisão de contrato. O bloqueio efetivo depende de o consumidor ou
orquestrador exigir `PASS`.

## 3. Onde isso aparece em uma plataforma real

```text
milhares de Spark jobs
  -> sparkMeasure produz maps de métricas
  -> uma camada normaliza os nomes
  -> as linhas são gravadas em uma tabela Delta central
  -> consumidores usam a tabela para alerts e gates
```

Considere um erro de integração:

```text
sparkMeasure emite shuffleTotalBytesRead
  -> um normalizador incorreto procura shuffleBytesRead
  -> shuffle_bytes_read recebe placeholder 0
  -> shuffle_bytes_read_available=false preserva a indisponibilidade
```

O schema contract atual exige a existência das colunas `*_available`, mas não
reprova automaticamente uma métrica opcional apenas porque `available=false`.
Cada consumidor precisa decidir quais métricas são obrigatórias para sua
decisão. Um runtime budget dependente de shuffle deveria recusar-se a decidir
quando `shuffle_bytes_read_available=false`, em vez de interpretar zero como
ausência real de shuffle. Essa recusa pertence ao consumidor e não está
implementada pelo Lab 6.

As regras atuais conseguem rejeitar, quando presentes no DataFrame avaliado:

- `metric_scope=task` quando o contrato espera `stage`;
- `collector_name` incompatível;
- identidade obrigatória nula;
- duplicidade dentro do mesmo batch;
- contador configurado com valor negativo;
- `num_stages = 0` ou `num_tasks = 0` para uma linha de ação Spark;
- required columns ou availability metadata ausentes.

## 4. Entenda o workload e a fronteira raw versus trusted

Abra:

```text
lab_6_stage_metrics_contract_gate.py
lab_6_utils/transformations.py
lab_6_utils/experiments.yaml
lab_6_utils/contract.py
lab_6_utils/contract_rules.yaml
```

O workload lê `sales`, faz joins com `vendors`, `products` e `customers`, reduz
o fato às colunas de negócio, reparticiona por mês, região e categoria, agrega e
escreve o Gold output:

```text
sales + vendors + products + customers
  -> select business fact -> repartition -> aggregate -> write Gold
```

O resultado contém `order_month`, `region`, `category`, `gross_revenue`,
`order_count` e `customer_count`.

StageMetrics envolve leitura, transformação e escrita do workload. Depois que o
collector termina, o runtime normaliza os contadores, enriquece a identidade e
escreve a linha em `stage_metrics_raw`. O contrato avalia um DataFrame e grava
resultados por regra e summary em tabelas separadas.

Raw telemetry continua existindo quando a decisão é `FAIL`. O lab não cria
quarentena, não remove linhas do raw e não materializa uma tabela `trusted`
contendo somente registros aprovados. Na falha didática, os registros
controlados permanecem isolados no demo input, enquanto a linha limpa continua
no raw.

Em produção, um consumidor ou orquestrador deve exigir `decision=PASS` antes de
publicar ou utilizar a telemetria em automação. O Lab 6 modela essa decisão, mas
não implementa a promoção física para um produto trusted.

## 5. Leia as três camadas do contrato

O contrato está em:

```text
lab_6_utils/contract_rules.yaml
```

Versão atual:

```yaml
contract:
  version: "1.0.0"
```

### 5.1 Schema contract

Verifica required columns e exige que cada métrica opcional exponha o valor e a
coluna `*_available` correspondente.

### 5.2 Semantic contract

Exige `num_stages > 0`, `num_tasks > 0`, `created_at` não nulo e contadores
configurados não negativos.

### 5.3 Correlation contract

Verifica identidades não nulas, valores esperados de `collector_name` e
`metric_scope` e esta uniqueness key:

```text
run_id + workload_name + workload_variant + metric_scope
```

A uniqueness é avaliada somente dentro do DataFrame recebido nesta execução.
Ela detecta duplicidade intra-batch, mas não consulta o histórico de
`stage_metrics_raw` e não garante idempotência histórica, deduplicação entre
execuções ou proteção contra retries em batches diferentes.

## 6. Entenda identidade e disponibilidade

| Campo | Origem | Propósito no lab |
|---|---|---|
| `run_id` | UUID criado pelo runtime | identidade da evidência e parte da uniqueness key |
| `application_id` | `spark.sparkContext.applicationId` | correlação com Spark History Server |
| `app_name` | `experiments.yaml` | identificação legível da aplicação |
| `lab_id` | valor fixo `lab_6` | ownership didático da telemetria |
| `workload_name` | `experiments.yaml` | identidade lógica do workload |
| `workload_variant` | `experiments.yaml` | variante que produziu a evidência |
| `collector_name` | `sparkmeasure_stage_metrics` | produtor esperado |
| `metric_scope` | `stage` | impede mistura silenciosa com métricas de task |
| `contract_version` | `contract_rules.yaml` | versão das expectativas aplicadas |
| `created_at` | timestamp UTC criado no registro | auditoria e análise histórica |

`validation_run_id` relaciona os resultados por regra ao summary da validação.
`application_id` permite chegar ao History Server, mas não pertence à
uniqueness key nem às identity columns obrigatórias atuais.

- **Métricas obrigatórias:** `num_stages`, `num_tasks` e
  `executor_run_time_ms`.
- **Métricas opcionais:** `shuffle_bytes_written`, `shuffle_bytes_read`,
  `jvm_gc_time_ms`, `memory_bytes_spilled`, `disk_bytes_spilled` e
  `input_bytes`.

O normalizador considera uma métrica disponível quando a chave existe e o valor
não é `None`:

```text
value=0, available=true  -> o collector emitiu um zero real
value=0, available=false -> o contador não foi emitido; zero é placeholder seguro
```

A semântica correta da linha limpa é produzida pelo normalizador. O contrato
`1.0.0` garante a presença das colunas `*_available`, mas não valida a coerência
entre cada valor e sua flag.

## 7. O que o contrato atual garante

- presença das required columns;
- presença das availability columns das métricas opcionais;
- stages e tasks maiores que zero;
- contadores configurados não negativos;
- identidades obrigatórias não nulas;
- `collector_name` e `metric_scope` esperados;
- uniqueness dentro do DataFrame avaliado;
- resultados e summary versionados por `contract_version`.

`severity` é metadata descritiva: qualquer regra com falha contribui para o
`FAIL` global. A versão atual não produz `WARNING` com base na severidade.

## 8. O que o contrato atual não garante

- precisão física interna do sparkMeasure;
- comparação das métricas com Spark UI ou event logs;
- coerência entre valor e `*_available`;
- reprovação automática de métrica opcional apenas por `available=false`;
- uniqueness contra todo o histórico Delta;
- proteção contra retries ou duplicidade em batches separados;
- read-back do Delta para validar o round trip de storage;
- tabela `trusted` contendo somente linhas aprovadas;
- quarentena ou bloqueio automático dos consumidores;
- decisões diferentes por severity.

Também não existe validação de compatibilidade semântica entre versões além das
regras e colunas configuradas. Schema ou version drift continua sendo um risco
de integração a ser modelado explicitamente.

## 9. Experimento A — cenário limpo

Execute:

```bash
bash run_stage_metrics_contract_gate.sh
```

Markers de progresso:

```text
LAB6_STAGE_METRICS_CAPTURED_OK
LAB6_STAGE_METRICS_INPUT_OK
LAB6_CONTRACT_RULES_LOADED_OK
LAB6_SCHEMA_CONTRACT_EVALUATED
LAB6_SEMANTIC_CONTRACT_EVALUATED
LAB6_CORRELATION_CONTRACT_EVALUATED
LAB6_CONTRACT_RESULTS_WRITTEN_OK
```

Marker final esperado:

```text
LAB6_STAGE_METRICS_CONTRACT_PASS
```

O bloco terminal apresenta:

```text
## LAB 6 STAGE METRICS CONTRACT GATE

### Final contract decision
decision: ...
demo_mode: ...
total_rules: ...
passed_rules: ...
failed_rules: ...

### Optional metric availability
status: ...

### Contract layers
schema: ...
semantic: ...
correlation: ...

### Failed rule details
rule_id: failed_count=... sample=...

### Delta outputs
...
```

Leia `demo_mode`, decisão, rule counts, availability metadata, resultados por
layer e failed rule details. Os markers usam `EVALUATED` porque uma camada
avaliada pode produzir `PASS` ou `FAIL`.

### Checkpoint de raciocínio — cenário limpo

- **Pergunta:** a linha normalizada satisfaz as três camadas do contrato?
- **Hipótese:** o caminho limpo preserva schema, valores, availability metadata
  e identidade configurados.
- **Evidência:** resultados `SCHEMA`, `SEMANTIC` e `CORRELATION`, availability
  columns, rule counts e decisão `PASS`.
- **Conclusão:** a linha satisfaz as regras configuradas e possui a estrutura
  necessária para os consumidores modelados pelo lab.
- **Limitação:** `PASS` não prova precisão interna do collector, coerência de
  todas as combinações possíveis, uniqueness histórica ou promoção para trusted.

## 10. Experimento B — falha controlada

Execute:

```bash
LAB6_INJECT_INVALID_RECORDS=true \
bash run_stage_metrics_contract_gate.sh
```

O runtime preserva a linha limpa em `stage_metrics_raw` e cria um demo input
separado. Os casos injetados estão identificados nas categorias abaixo.

Marker final esperado:

```text
LAB6_STAGE_METRICS_CONTRACT_FAIL
```

O `FAIL` esperado retorna exit code `0` para permitir a inspeção. Erro técnico
continua retornando exit code diferente de zero; em produção, a orquestração
deve transformar `decision=FAIL` em bloqueio.

### Falhas mais próximas de problemas de plataforma

- identidade ausente — `run_id` nulo é injetado no demo;
- `metric_scope` incorreto — `task` é injetado no demo;
- `collector_name` incompatível;
- duplicidade no batch — injetada no demo;
- availability metadata ausente;
- incompatibilidade de schema ou versão.

O demo injeta somente parte desses casos. As regras atuais detectam schema
ausente, mas compatibilidade completa entre versões permanece fora do escopo.

### Falhas principalmente didáticas neste caminho local

- `num_stages = 0`, injetado no demo;
- `num_tasks = 0`, injetado no demo;
- `shuffle_bytes_written = -1`, injetado no demo;
- `created_at` nulo, injetado no demo.

O normalizador e o schema controlado pelo próprio Lab 6 já impedem parte dessas
falhas no caminho limpo. As linhas sintéticas demonstram categorias de um
contract gate; elas não afirmam que sparkMeasure normalmente produza contadores
negativos ou identidades nulas.

Na tabela Delta de resultados por regra, leia:

```text
rule_id
rule_type
severity
decision
failed_count
sample_failed_keys
recommendation
```

### Checkpoint de raciocínio — falha controlada

- **Pergunta:** o contrato torna visíveis linhas inadequadas para automação?
- **Hipótese:** falhas estruturais, semânticas e de correlação aparecem como
  resultados acionáveis antes do consumo automatizado.
- **Evidência:** rule IDs, layer, severity, failed count, sample failed keys,
  recommendation e decisão `FAIL`.
- **Conclusão:** o gate torna falhas estruturais, semânticas e de correlação
  visíveis antes do uso automatizado.
- **Limitação:** os registros são sintéticos; o experimento demonstra categorias
  de contrato, não falhas naturais do sparkMeasure nem um framework completo de
  data quality.

## 11. Relacione o contrato aos consumidores

| Consumidor | Premissa protegida pelo contrato |
|---|---|
| Dashboard | identidade e timestamps permitem agrupar e filtrar |
| Alerts | contadores configurados são não negativos antes dos thresholds |
| Runtime budgets | required metrics existem; o consumidor ainda deve exigir availability das métricas usadas |
| PR review | `run_id` e `workload_variant` identificam a evidência comparada |
| Drift monitoring | uniqueness detecta duplicidade intra-batch; deduplicação histórica permanece fora do escopo |

## 12. Caminhos opcionais

### 12.1 Inspecione a evidência Delta

```text
s3a://lakehouse/gold/lab6/stage_metrics_contract_gate/business_output
s3a://observability/lab6/stage_metrics_raw
s3a://observability/lab6/stage_metrics_contract_demo_input
s3a://observability/lab6/stage_metrics_contract_results
s3a://observability/lab6/stage_metrics_contract_summary
```

Os resultados por regra preservam `validation_run_id`, `source_path`,
`contract_version`, tipo, severity, decisão, contagem, amostra, recomendação e
`created_at`.

MinIO Console:

```text
http://127.0.0.1:29011
```

Credenciais locais padrão:

```text
user:     sparkworkshop
password: sparkworkshop123
```

### 12.2 Correlacione com o Spark History Server

```text
http://127.0.0.1:28090
```

Use `application_id` para localizar a aplicação. O History Server explica a
execução Spark; o contract gate valida a linha integrada.

### 12.3 Execute o submit manual do cenário limpo

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB6_CONFIG_NAME=lab6-stage-metrics-contract-gate \
    LAB6_INJECT_INVALID_RECORDS=false \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_6/lab_6_stage_metrics_contract_gate.py \
    --inject-invalid-records false
```

### 12.4 Cleanup depois da aula

Retorne à raiz:

```bash
cd ../../../..
make down
```

Isso interrompe os containers e preserva os dados do MinIO. Use o soft cleanup
drill somente quando a próxima execução precisar começar sem o estado anterior.

## Conclusão da aula

Não estamos testando se o sparkMeasure funciona. Estamos verificando se a linha
normalizada, enriquecida e persistida continua acompanhada de contexto e de uma
decisão de contrato suficientes para que o consumidor avalie se pode utilizá-la:

```text
sparkMeasure output
  -> normalized telemetry row
  -> explicit contract
  -> consumer-aware decision
  -> future trusted observability product
```

## Ponte para a próxima aula

Com métricas contratadas e correlacionáveis, o Lab 7 muda a pergunta: como o
workload se comporta ao longo do tempo de negócio e através de várias
aplicações Spark?
