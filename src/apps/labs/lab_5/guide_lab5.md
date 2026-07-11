# Guia do Lab 5: runtime budget guardrail em nível de stage

Este guia organiza a execução em sala do Lab 5. O lab compara uma implementação
aprovada com uma candidate, valida invariantes funcionais e usa StageMetrics
para aplicar uma policy operacional.

Notas detalhadas:

[Notas de aula sobre o runtime budget guardrail](docs/stage_runtime_budget_guardrail_class_notes.md)

Consulte-as para o racional do workload, a semântica dos budgets, o tratamento
de low signal e o mapeamento para produção.

## Enquadramento da aula

- **Pergunta norteadora:** uma implementação funcionalmente correta também cabe
  no orçamento operacional definido pela equipe?
- **Por que esta aula aparece agora:** o Lab 4 produziu uma hipótese operacional;
  o Lab 5 transforma StageMetrics em uma decisão de engenharia.
- **Resultado de aprendizagem:** separar compatibilidade funcional, mudança
  relativa de custo e decisão de policy.
- **Modo de condução:** exercício principal guiado e executado ao vivo;
  evidência Delta, MinIO e Spark History Server opcionais.

## 0. Pré-requisitos

Comece na raiz do repositório:

```bash
cd workshop-spark-measures
```

O Lab 5 reutiliza as fontes Bronze compartilhadas pelos Labs 1–6. Se for a
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

## 1. Entre na pasta do Lab 5

```bash
cd src/apps/labs/lab_5
```

Pontos de entrada:

```text
lab_5_stage_runtime_budget_guardrail.py
run_stage_runtime_budget_guardrail.sh
```

## 2. Estabeleça a pergunta de promoção

Pergunte à turma:

```text
Uma mudança Spark funcionalmente correta ainda pode ser cara demais para ser
promovida?
```

O lab representa uma revisão de PR:

1. `baseline` representa a implementação aprovada;
2. `candidate` representa a mudança proposta;
3. os outputs precisam passar pelos invariantes funcionais escolhidos;
4. StageMetrics mede o comportamento operacional das duas variantes;
5. as regras YAML decidem se o crescimento da candidate é aceitável.

Compatibilidade funcional e aceitabilidade operacional são gates diferentes.
Não interprete a policy antes de confirmar os checks de negócio.

## 3. Localize configuração e policy

Abra:

```text
lab_5_stage_runtime_budget_guardrail.py
lab_5_utils/experiments.yaml
lab_5_utils/budget_rules.yaml
lab_5_utils/budget.py
```

O app mantém um único seletor visível:

```python
CONFIG_NAME = os.environ.get(
    "LAB5_CONFIG_NAME",
    "lab5-stage-runtime-budget-guardrail",
)
```

`experiments.yaml` define as identidades das variantes, partition counts,
`revenue_tolerance`, StageMetrics e os outputs Delta. `budget_rules.yaml` define
os limites de crescimento de runtime, shuffle, tasks e stages, além das regras
de spill, low signal e overrides por profile.

O Lab 5 recalcula um profile leve para selecionar ou anotar a policy; ele não
depende do fingerprint persistido pelo Lab 4. Os valores YAML são budgets da
calibração didática, não thresholds universais de Spark.

O profile do Lab 5 é um seletor de policy, não o fingerprint completo do Lab 4.
Ele é calculado em `classify_budget_profile()` antes da escolha dos overrides:

1. spill novo na candidate, quando a baseline não tinha spill, seleciona
   `MEMORY_PRESSURE`;
2. caso contrário, shuffle total da candidate de pelo menos 1 MiB seleciona
   `SHUFFLE_HEAVY`;
3. os demais casos usam `BALANCED_OR_LOW_SIGNAL`.

O Python escolhe o profile; o YAML define os budgets padrão e os overrides
aplicáveis a esse profile.

## 4. Compare as implementações

As duas variantes foram construídas para produzir estas colunas:

```text
order_month
region
category
gross_revenue
order_count
customer_count
```

### 4.1 Baseline aprovada

```text
sales
  -> prune fact columns
  -> join vendor region
  -> join product category
  -> select the business fact
  -> repartition by business keys
  -> aggregate the Gold result
```

### 4.2 Candidate proposta

```text
sales
  -> carry wider payload columns
  -> join vendor region
  -> join product category
  -> join unused customer context
  -> unnecessary round-robin repartition
  -> derive an unused bucket and CPU-burden expression
  -> repartition by the unused bucket
  -> repartition again by business keys
  -> aggregate the same Gold result
```

A candidate introduz, de propósito, join sem uso no resultado, movimentação de
linhas mais largas, expressão de CPU descartada e repartitions extras. Antes de
mostrar as métricas, peça à turma que identifique esse trabalho físico.

## 5. Execute o runtime budget guardrail

```bash
bash run_stage_runtime_budget_guardrail.sh
```

O runner realiza um `spark-submit`. Dentro da mesma aplicação, `baseline` e
`candidate` executam sequencialmente, cada uma com seu próprio collector de
StageMetrics.

Markers de progresso:

```text
LAB5_BASELINE_STAGE_METRICS_OK
LAB5_CANDIDATE_STAGE_METRICS_OK
LAB5_OUTPUT_COMPATIBILITY_OK
LAB5_BUDGET_RULES_LOADED_OK
LAB5_RUNTIME_BUDGET_DECISION_WRITTEN_OK
```

Exatamente um marker final deve aparecer:

```text
LAB5_RUNTIME_BUDGET_PASS
LAB5_RUNTIME_BUDGET_FAIL
LAB5_RUNTIME_BUDGET_WARNING_LOW_SIGNAL
```

A configuração padrão foi calibrada para produzir:

```text
LAB5_RUNTIME_BUDGET_FAIL
```

Esse `FAIL` didático retorna exit code `0`: a aplicação executou corretamente e
a policy rejeitou a candidate. Fonte ausente, YAML inválido, schema de métricas
incompatível, output de negócio incompatível ou falha de escrita continuam
erros técnicos e retornam exit code diferente de zero.

O exit code `0` é uma escolha didática para permitir que a aula observe e
inspecione uma decisão `FAIL` esperada. Em produção, a orquestração ou o pipeline
de CI deve converter `decision=FAIL` em bloqueio efetivo da promoção, mesmo que
a aplicação de medição tenha terminado tecnicamente com sucesso.

### Fronteira da medição e da validação

Cada collector mede a execução de sua variante, incluindo leitura,
transformação e escrita do output. Depois que os collectors de `baseline` e
`candidate` terminam, o runtime executa novos Spark jobs para validar schema,
row count, receita e pedidos.

Esses jobs de compatibilidade não fazem parte das StageMetrics comparadas pelo
budget. A compatibilidade funciona como pré-condição lógica da decisão, não
como parte do custo medido de cada variante.

## 6. Leia primeiro a compatibilidade funcional

O bloco final contém:

```text
### Functional compatibility
status: OK
rows: baseline=... candidate=...
total_revenue: baseline=... candidate=...
total_order_count: baseline=... candidate=...
```

Antes de discutir performance, confirme os quatro invariantes implementados:

- mesmo schema;
- mesmo `row_count`;
- `gross_revenue` total dentro de `revenue_tolerance`;
- mesmo `order_count` total.

Se qualquer check falhar, o runtime interrompe a execução antes de carregar e
aplicar os budgets.

### Checkpoint de raciocínio — compatibilidade funcional

- **Pergunta:** baseline e candidate entregam um resultado compatível para a
  decisão do lab?
- **Hipótese:** apesar do plano físico diferente, as variantes passam pelos
  invariantes funcionais escolhidos.
- **Evidência:** schema, row count, receita dentro da tolerância e total de
  pedidos.
- **Conclusão:** as variantes são compatíveis segundo os invariantes funcionais
  definidos pelo lab.
- **Limitação:** esses checks não demonstram equivalência semântica completa
  além dos campos e agregados selecionados, nem dizem isoladamente algo sobre
  custo ou performance.

## 7. Aplique a policy somente depois

O mesmo bloco apresenta:

```text
## LAB 5 RUNTIME BUDGET GUARDRAIL

### Final decision
decision: ...
workload_profile: ...
failed_rules: ...
warning_flags: ...

### Baseline StageMetrics
...

### Candidate StageMetrics
...

### Candidate delta versus baseline
executor runtime: baseline -> candidate | signed percentage | multiplier
shuffle written: baseline -> candidate | signed percentage | multiplier
shuffle read: baseline -> candidate | signed percentage | multiplier
tasks: baseline -> candidate | signed percentage | multiplier
stages: baseline -> candidate | signed percentage | multiplier
GC time: baseline -> candidate | signed percentage | multiplier | supporting signal
spill total: baseline -> candidate | signed percentage | multiplier | supporting signal

### Delta outputs
...
```

Leia na ordem:

1. compare os contadores brutos de baseline e candidate;
2. leia o sinal e o multiplicador de cada delta;
3. identifique o profile e os thresholds aplicáveis no YAML;
4. reconstrua as `failed_rules`;
5. interprete `warning_flags`;
6. somente então discuta a decisão final.

Delta positivo significa que a candidate cresceu em relação à baseline. Em um
budget de crescimento máximo, esse sinal é operacionalmente pior. No exemplo
local validado, `+121.98% | 2.22x baseline` significa aproximadamente 2,22 vezes
o valor da baseline — não 121,98 vezes.

Delta negativo representa redução. GC e spill aparecem no bloco como supporting
signals porque oscilam em workloads locais pequenos; GC não possui budget neste
lab, enquanto spill novo ainda pode acionar uma regra explícita. Quando a
baseline é zero e a candidate é positiva, o renderer mostra `+100.00% | new`.
Esse `100%` é um sentinel da implementação, não uma porcentagem convencional
calculada a partir de um denominador diferente de zero.

### Checkpoint de raciocínio — runtime budget

- **Pergunta:** a candidate permanece dentro dos limites configurados?
- **Hipótese:** trabalho físico adicional aparece como crescimento relativo em
  runtime, shuffle, tasks, stages, GC ou spill.
- **Evidência:** StageMetrics das duas variantes, deltas com sinal,
  multiplicadores, thresholds aplicados, failed rules e decisão final.
- **Conclusão:** a policy local pode rejeitar uma regressão operacional mesmo
  quando o output passa pelos invariantes funcionais.
- **Limitação:** baseline e candidate executam uma única vez, em ordem fixa e
  na mesma aplicação. Cache, aquecimento e estado residual podem favorecer uma
  das variantes. Produção exige baselines repetidas e representativas, ou uma
  estratégia controlada de isolamento e alternância da ordem.

## 8. Entenda as decisões

`PASS`

: A candidate permaneceu dentro de todos os budgets aplicáveis.

`FAIL`

: Ao menos uma métrica excedeu o budget configurado.

`WARNING_LOW_SIGNAL`

: Runtime e shuffle ficaram abaixo dos mínimos necessários para uma decisão
  forte.

Low signal tem precedência sobre `PASS` e `FAIL`. As `failed_rules` ainda podem
ser exibidas como sinais, mas a decisão evita tratá-las como gate autoritativo
quando a evidência é pequena demais.

## 9. Fronteira entre o Lab 5 e o Lab 6

O Lab 5 faz um fail-fast mínimo para `numStages`, `numTasks` e
`executorRunTime`, mas depois confia na telemetria normalizada. Métricas
opcionais ausentes podem virar zero, e o lab não valida disponibilidade
explícita, identidade do collector, unicidade ou chaves de correlação como um
contrato de dados faria.

Ele valida o output de negócio e aplica a policy, mas não valida um contrato
completo da telemetria antes da decisão.

Essa confiança é uma fronteira deliberada da aula. Se uma automação depende de
runtime, shuffle, spill e identidades de execução, a próxima pergunta é se esses
dados têm schema, semântica e chaves de correlação confiáveis.

## 10. Caminhos opcionais

### 10.1 Inspecione a evidência persistida

Outputs de negócio sobrescritos a cada run:

```text
s3a://lakehouse/gold/lab5/stage_runtime_budget/baseline
s3a://lakehouse/gold/lab5/stage_runtime_budget/candidate
```

Uma linha de métricas por variante é adicionada a:

```text
s3a://observability/lab5/stage_runtime_budget_runs
```

Uma decisão final é adicionada a:

```text
s3a://observability/lab5/stage_runtime_budget_decisions
```

A decisão preserva os run IDs, profile, decision, failed rules, warning flags,
deltas e métricas serializadas das duas variantes.

MinIO Console:

```text
http://127.0.0.1:29011
```

Credenciais locais padrão:

```text
user:     sparkworkshop
password: sparkworkshop123
```

### 10.2 Correlacione com o Spark History Server

```text
http://127.0.0.1:28090
```

Use `application_id` para localizar a aplicação. O History Server explica os
jobs e stages por trás do budget; o guardrail fornece a decisão compacta.

### 10.3 Execute o submit manual

Comando equivalente a partir da pasta do Lab 5:

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    LAB5_CONFIG_NAME=lab5-stage-runtime-budget-guardrail \
    /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.driver.host=spark-master \
    --conf spark.eventLog.dir=s3a://observability/event-logs \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
    /opt/spark/src/apps/labs/lab_5/lab_5_stage_runtime_budget_guardrail.py
```

### 10.4 Cleanup depois da aula

Retorne à raiz:

```bash
cd ../../../..
make down
```

Isso interrompe os containers e preserva os dados do MinIO. Use o soft cleanup
drill somente quando a próxima execução precisar começar sem o estado anterior.

## Conclusão da aula

Uma mudança Spark pode passar pelos checks funcionais escolhidos e ainda ser
operacionalmente pior. Runtime budgets transformam StageMetrics em um controle
leve antes da promoção, desde que baseline, ambiente e policy sejam
representativos e tenham ownership explícito.

## Ponte para a próxima aula

Se a decisão de promoção depende dessas métricas, como sabemos que a telemetria
é confiável? O Lab 6 transforma essa pergunta em um contrato para StageMetrics.
