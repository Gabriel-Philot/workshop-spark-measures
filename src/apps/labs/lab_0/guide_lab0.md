# Guia do Lab 0: inventário das fontes e introdução ao sparkMeasure

Este é o roteiro de aula do Lab 0.

Fluxo:

```text
preparar a stack local
  -> gerar os dados Bronze de retail
  -> verificar se as fontes estão prontas
  -> apresentar a API nativa do sparkMeasure
  -> apresentar o contrato do workshop com sparkMeasure habilitado
```

## Enquadramento da aula

- **Pergunta norteadora:** quais evidências o Spark oferece nativamente, o que o
  sparkMeasure condensa e por que o workshop usa um contrato de execução?
- **Por que esta aula aparece agora:** a plataforma, as fontes e o vocabulário de
  evidência precisam estar estabelecidos antes dos labs de diagnóstico.
- **Resultado de aprendizagem:** distinguir preparação da plataforma, evidência
  nativa do Spark, API nativa do sparkMeasure e contrato do workshop.
- **Modo de condução:** preparação pré-aula obrigatória; aula principal conduzida
  pelo instrutor e executada ao vivo.

### Vocabulário usado neste lab

| Expressão | Significado |
| --- | --- |
| Evidência nativa do Spark | `explain`, Spark UI, event logs, jobs, stages, tasks e executors produzidos pelo próprio Spark. |
| API nativa do sparkMeasure | Uso direto de `StageMetrics`, `begin()`, `end()`, `print_report()` e `aggregate_stagemetrics()`, sem o contrato do workshop. |
| Modo `native` do Lab 0C | Nome configurado para a execução do contrato com a coleta do sparkMeasure desabilitada e o `explain` habilitado. |
| Modo `observed` do Lab 0C | A mesma transformação com StageMetrics habilitado pelo contrato; as métricas são exibidas, mas não persistidas neste lab. |

Referência para explicar por que a API nativa aparece antes do contrato:

[Racional do contrato do Lab 0](docs/contract_rationale.md)

## Parte A — Preparação da plataforma antes da aula

### 0. Comece na raiz do repositório

```bash
cd workshop-spark-measures
```

Esperado:

- `Makefile` existe;
- `.env.example` existe;
- `src/apps/labs/lab_0` existe.

### 1. Prepare as dependências locais

Execute uma vez por máquina ou sempre que as dependências fixadas mudarem.

```bash
make bootstrap
```

Esse comando:

- cria ou atualiza `.env`;
- sincroniza o ambiente Python local;
- baixa as base images fixadas;
- baixa os artefatos de Spark, Delta, S3A e sparkMeasure;
- prepara o cache de wheels Python usado pelos Spark jobs.

Linha final esperada:

```text
Bootstrap completed
```

### 2. Construa as images locais

```bash
make build
```

O comando constrói as images locais do runtime Spark, Spark History, MinIO e do
dashboard do Lab 7, embora o Lab 0 ainda não utilize esse dashboard.

Esperado:

- o comando termina com status `0`;
- não há erros de artefatos ausentes do bootstrap.

### 3. Inicie a plataforma

```bash
make compose
```

Esse comando inicia MinIO, cria os buckets obrigatórios e sobe Spark Master,
dois Spark workers e Spark History Server.

Linhas de prontidão esperadas:

```text
Validation passed
MinIO is ready
Spark Master is ready
Spark Workers (2) is ready
Spark History is ready
```

UIs úteis:

```text
Spark Master UI:     http://127.0.0.1:28091
Spark History Server: http://127.0.0.1:28090
MinIO Console:       http://127.0.0.1:29011
```

Credenciais padrão do MinIO:

```text
user:     sparkworkshop
password: sparkworkshop123
```

### 4. Execute o dry test do sparkMeasure

```bash
make dry-test
```

O dry test comprova que Spark submete jobs, Delta e S3A leem e escrevem pelo
MinIO e o JAR e o pacote Python do sparkMeasure funcionam juntos.

Esperado:

- o comando termina com status `0`;
- o log local existe em:

```text
build/var/dry-test.log
```

Não comece a aula enquanto essa etapa falhar. Corrija a plataforma primeiro.

### 5. Gere os dados Bronze de retail

```bash
make generate SCALE=xs GENERATOR_RUN_ID=workshop-sparkMeasures-lab1-6
```

O `GENERATOR_RUN_ID` não altera schema ou escala. Ele identifica a execução nos
manifests, logs e troubleshooting da aula. O valor
`workshop-sparkMeasures-lab1-6` identifica as fontes de retail compartilhadas
pelos Labs 0–6; o Lab 7 usa uma fonte temporal separada.

Paths Bronze esperados:

```text
s3a://lakehouse/bronze/retail/vendors
s3a://lakehouse/bronze/retail/products
s3a://lakehouse/bronze/retail/customers
s3a://lakehouse/bronze/retail/sales
```

Log local esperado:

```text
build/var/generate-xs.log
```

## Parte B — Fluxo principal da aula

### 6. Entre na pasta do Lab 0

Os próximos comandos ficam mais fáceis de apresentar a partir da pasta do lab,
mantendo paths relativos ao repositório para o Docker Compose.

```bash
cd src/apps/labs/lab_0
```

Verificação opcional:

```bash
ls
```

Scripts esperados:

```text
lab_0a_source_inventory.py
lab_0b_sparkmeasure_native_api.py
lab_0c_sparkmeasure_presentation.py
```

### 7. Execute 0A: inventário das fontes

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0a_source_inventory.py
```

Markers esperados:

```text
LAB0_SOURCE_VOLUME
LAB0_RELATIONSHIP_CHECK
LAB0_SOURCE_CHARACTERISTIC
WORKSHOP_RUN_COMPLETED
LAB0_SOURCE_INVENTORY_OK
```

#### Checkpoint de raciocínio — prontidão das fontes

- **Pergunta:** as fontes possuem volume, layout físico e relacionamentos
  adequados para os próximos labs?
- **Hipótese:** o gerador produziu as quatro tabelas e preservou as chaves de
  relacionamento esperadas.
- **Evidência:** linhas, arquivos, bytes, tamanhos por arquivo e violações de
  chaves aparecem no bloco final do inventário.
- **Conclusão:** as fontes estão prontas quando os markers e as validações de
  integridade passam.
- **Limitação:** o inventário descreve os dados e sua prontidão; ele ainda não é
  uma demonstração do sparkMeasure nem um diagnóstico de performance.

### 8. Execute 0B: API nativa do sparkMeasure

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0b_sparkmeasure_native_api.py
```

Esse é o momento da biblioteca sem o wrapper do workshop: o código torna
visíveis `StageMetrics(spark)`, `begin()`, `end()`, `print_report()` e
`aggregate_stagemetrics()`.

Markers esperados:

```text
SPARKMEASURE_NATURAL_API_BEGIN
SPARKMEASURE_NATURAL_API_END
SPARKMEASURE_NATURAL_API_METRICS
LAB0_SPARKMEASURE_NATURAL_API_OK
```

#### Checkpoint de raciocínio — API nativa

- **Pergunta:** como o sparkMeasure delimita e agrega uma ação Spark?
- **Hipótese:** `begin()` e `end()` ao redor da ação produzem um resumo compacto
  de métricas de stage.
- **Evidência:** a API está explícita no script, seguida pelo relatório nativo e
  por `aggregate_stagemetrics()`.
- **Conclusão:** o collector mede a região executada entre as fronteiras, não o
  script inteiro.
- **Limitação:** agregados de stage não explicam a distribuição entre tasks; isso
  exigiria outra pergunta e outro nível de coleta.

### 9. Execute 0C: apresentação do contrato do workshop

```bash
docker compose --env-file ../../../../.env -f ../../../../build/docker-compose.yml exec -T spark-master \
  env PYTHONPATH=/opt/spark/src:/opt/spark/generator/src /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --conf spark.driver.host=spark-master \
  --conf spark.eventLog.dir=s3a://observability/event-logs \
  --conf spark.executorEnv.PYTHONPATH=/opt/spark/src:/opt/spark/generator/src \
  /opt/spark/src/apps/labs/lab_0/lab_0c_sparkmeasure_presentation.py
```

O script executa o mesmo enriquecimento Bronze-to-Silver em dois modos. O modo
native imprime o `explain`; o modo observed habilita StageMetrics por YAML. A
transformação permanece legível enquanto a observabilidade fica na configuração.

Markers esperados:

```text
LAB0_PRESENTATION_NATIVE_OK
LAB0_PRESENTATION_SPARKMEASURE_OK
LAB0_SPARKMEASURE_PRESENTATION_OK
SPARKMEASURE_METRICS
WORKSHOP_RUN_COMPLETED
```

Output Silver esperado:

```text
s3a://lakehouse/silver/lab0/sales_enriched
```

Para explicar a estrutura do script, use o
[racional do contrato](docs/contract_rationale.md).

Para seguir do terminal até jobs, stages, plano físico e executors, use o
[walkthrough da Spark UI até o sparkMeasure](docs/spark_ui_to_sparkmeasure_walkthrough.md).

### 10. Investigue a execução no Spark History Server

Abra:

```text
http://127.0.0.1:28090
```

Procure pelas aplicações:

```text
workshop-lab0-source-inventory
workshop-lab0-sparkmeasure-native-api
workshop-lab0-sparkmeasure-presentation-native
workshop-lab0-sparkmeasure-presentation-observed
```

Inspecione:

- `Jobs`: descrições como `SPARK_WORKLOAD | ...` e
  `Delta: SPARK_WORKLOAD | ...`;
- o main materialization/write job: a linha `SPARK_WORKLOAD | ...` com
  `save at NativeMethodAccessorImpl.java:0`; a página deve apresentar um
  `Associated SQL Query` e um stage concluído com `Input` e `Output`;
- `Stages`: duração, quantidade de tasks e colunas de shuffle;
- `SQL / DataFrame`: detalhes da execução física quando disponíveis.

Não interprete esse único Spark UI Job como todo o workload. Ele é a melhor
âncora para a escrita final, mas Delta snapshot, file filtering, broadcast
preparation, subexecuções assíncronas e commit/statistics podem aparecer como
jobs separados dentro da mesma fronteira `SPARK_WORKLOAD`. Use o
`Associated SQL Query` para chegar ao plano físico mais amplo e o sparkMeasure
para comparar a região agregada medida.

O walkthrough detalhado mostra como localizar esses elementos sem depender de
job ID, stage ID ou query ID fixos:

[Walkthrough da Spark UI até o sparkMeasure](docs/spark_ui_to_sparkmeasure_walkthrough.md)

#### Checkpoint de raciocínio — evidência nativa e contrato

- **Pergunta:** o que o contrato e o sparkMeasure simplificam, e o que ainda
  exige `explain` ou Spark UI?
- **Hipótese:** a execução observed condensa sinais úteis sem substituir plano,
  jobs, stages, tasks e executors.
- **Evidência:** os modos native e observed executam a mesma transformação; o
  terminal mostra `explain` e métricas agregadas, enquanto a UI registra a
  decomposição real da execução.
- **Conclusão:** contrato, sparkMeasure, `explain` e Spark UI são superfícies de
  evidência complementares.
- **Limitação:** um job isolado da UI não representa toda a aplicação, e essa
  comparação introdutória não prova uma causa raiz de performance.

## Material operacional opcional

### 11. Inspecione o MinIO

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
lakehouse/silver/lab0/sales_enriched
observability/event-logs
```

O Lab 0 não persiste as métricas do sparkMeasure como tabelas Delta. A
persistência está desabilitada nesse experimento para manter o History Server
focado nos jobs do workload, sem jobs adicionais de escrita das métricas.

### 12. Limpeza opcional depois da aula

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

O Lab 0 estabeleceu as fontes, as ferramentas nativas e o contrato de execução.
O Lab 1 usa essa base na primeira investigação real: começa com StageMetrics e
abre TaskMetrics somente quando a pergunta depende da distribuição entre tasks.
