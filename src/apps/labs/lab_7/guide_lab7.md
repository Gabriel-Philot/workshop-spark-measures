# Guia do Lab 7: temporal backfill observability

Este é o roteiro de aula do Lab 7.

Notas de apoio:

[Notas de aula sobre temporal backfill
observability](docs/temporal_backfill_observability_class_notes.md)

Mantenha essas notas abertas para consultar o desenho da fonte, as limitações de
`input_bytes` e a evidência local validada.

## Enquadramento da aula

- **Pergunta norteadora:** quais datas de negócio alteraram o perfil de execução
  do backfill e quais relações de StageMetrics tornam essa mudança visível?
- **Por que esta aula aparece agora:** os labs anteriores diagnosticaram ou
  governaram uma execução. O Lab 7 adiciona contexto temporal persistido e
  compara várias aplicações associadas a datas de negócio conhecidas.
- **Resultado de aprendizagem:** correlacionar volume esperado, `records_read`,
  shuffle, executor runtime, tasks, GC e spill por data sem confundir observação
  com prova de causa raiz.
- **Modo de condução:** demonstração principal do instrutor; batch completo ao
  vivo quando houver aproximadamente 10 minutos, evidência pré-computada como
  fallback e smoke test apenas como preflight opcional.

O argumento da aula é:

```text
known temporal volume
  -> one correlated Spark execution per date
  -> persisted stage-level evidence
  -> historical comparison
  -> read-only dashboard
```

## 0. Confirme somente o pré-requisito específico

Execute os comandos a partir da raiz do repositório:

```bash
cd workshop-spark-measures
```

O Lab 7 pressupõe que as dependências fixadas e as imagens do workshop já
existem. No primeiro uso do workshop, ou depois de remover as imagens, siga o
[guia do Lab 0: bootstrap e build](../lab_0/guide_lab0.md) antes de continuar.

O runner público do Lab 7 inicia Compose e cria ou valida sua própria fonte
temporal isolada. Ele não depende da regeneração de `sales`, `vendors`,
`products` ou `customers`, e não altera as fontes Bronze usadas pelos Labs 0–6.

## 1. Entenda o workflow público

O ponto público de orquestração é:

```text
src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Ele executa esta sequência:

```text
make compose
  -> create or validate the Lab 7 temporal source
  -> load the configured processing dates
  -> run one sequential spark-submit per date
  -> append one StageMetrics row per date
  -> print the dashboard command and metrics path
```

Os dois entry points Spark permanecem visíveis na raiz do lab:

```text
src/apps/labs/lab_7/lab_7_temporal_source_generator.py
src/apps/labs/lab_7/lab_7_daily_backfill_stage_metrics.py
```

O primeiro garante a fonte temporal; o segundo processa uma única
`processing_date`. Runners auxiliares, configurações e transformações ficam em
`lab_7_utils/`. Durante a aula, use o runner público, salvo quando estiver
depurando um componente isolado.

## 2. Leia primeiro o volume plan

Abra:

```text
src/apps/labs/lab_7/lab_7_utils/volume_plan.yaml
```

O plano cria 14 datas de negócio determinísticas:

| classe da data | linhas por data | multiplicador |
| --- | ---: | ---: |
| normal | 10,000 | 1x |
| medium spike | 100,000 | 10x |
| large spike | 1,000,000 | 100x |

Datas de spike configuradas:

```text
2026-01-04 -> 1,000,000 rows -> VOLUME_SPIKE
2026-01-07 ->   100,000 rows -> MEDIUM_SPIKE
2026-01-11 -> 1,000,000 rows -> VOLUME_SPIKE
```

Volume total planejado:

```text
2,210,000 rows
```

O gerador grava uma fonte Delta isolada, particionada por `event_date`:

```text
s3a://lakehouse/bronze/lab7/source_events_temporal
```

O volume esperado também é persistido em:

```text
s3a://observability/lab7/temporal_volume_plan
```

Comece pelo volume de negócio conhecido. Ele é a expectativa controlada contra
a qual `records_read`, shuffle, runtime e tasks serão interpretados.

## 3. Preflight opcional: smoke test com duas datas

Use este caminho somente para verificar rapidamente o ambiente:

```bash
LAB7_PROCESSING_DATES=2026-01-01,2026-01-04 \
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

Ele compara uma data normal com uma data `100x`:

```text
2026-01-01 -> 10,000 rows
2026-01-04 -> 1,000,000 rows
```

Na validação local com storage vazio, o comando levou:

```text
136.68 seconds = 2 minutes 16.68 seconds
```

Esse wall-clock incluiu a prontidão do Compose, a geração da fonte temporal
completa e os dois submits. É um exemplo do ambiente local, não uma duração
garantida.

O smoke possui seu próprio `run_id`, mas não é a evidência principal da aula.
Duas datas escondem a forma cronológica e podem tornar posições de spikes,
legendas e gráficos normalizados enganosos. Não valide o dashboard por esse
recorte.

## 4. Evidência principal: execute o batch completo de 14 datas

Não execute `make generate-lab7` imediatamente antes do fluxo normal. O runner
público já cria as datas ausentes ou valida a fonte isolada antes do backfill.

Execute sem definir `LAB7_PROCESSING_DATES`:

```bash
bash src/apps/labs/lab_7/run_temporal_backfill_observability.sh
```

O runner submete as 14 datas sequencialmente. Procure o início:

```text
LAB7_DAILY_BACKFILL_BATCH_STARTED ... dates=14 ...
```

E a conclusão:

```text
LAB7_DAILY_BACKFILL_BATCH_COMPLETED ... dates=14 ...
LAB7_TEMPORAL_BACKFILL_OBSERVABILITY_COMPLETED
```

Exemplo local validado em 2026-07-11:

```text
575.37 seconds = 9 minutes 35.37 seconds
```

Esse é o **batch wall-clock** medido externamente, iniciado com o storage do
projeto vazio. A fronteira incluiu:

- validação e prontidão do Compose;
- geração e validação das 2,210,000 linhas da fonte temporal;
- 14 aplicações Spark sequenciais;
- um append Delta de StageMetrics por data.

Bootstrap e build das imagens ficaram fora da medição. Reserve cerca de 10
minutos apenas como referência para esta stack local e use a janela para explicar
o volume plan e a escolha de um submit por data.

### Fallbacks para a aula

1. Se a tabela Delta já contém um batch completo, execute
   `make lab7-dashboard`, selecione o `run_id` com `Processed dates: 14` e use a
   evidência persistida.
2. Se não existe evidência persistida, use a tabela em [evidência local
   validada](docs/temporal_backfill_observability_class_notes.md#validated-local-evidence)
   como discussão pré-computada. Nesse caso, não apresente o dashboard como uma
   leitura ao vivo.

## 5. Leia um bloco diário antes do dashboard

Cada data imprime:

```text
## LAB 7 DAILY BACKFILL STAGE METRICS

### Processing date
processing_date: ...
source_rows_for_date: ...
volume_multiplier: ...
spike_label: ...

### StageMetrics
executor_run_time_ms: ...
records_read: ...
input_bytes: ...
shuffle_bytes_written: ...
shuffle_bytes_read: ...
num_stages: ...
num_tasks: ...
memory_bytes_spilled: ...
disk_bytes_spilled: ...
jvm_gc_time_ms: ...

### Normalized by expected source volume
runtime_per_million_rows: ...
shuffle_per_million_rows: ...
input_bytes_per_million_rows: ...
tasks_per_million_rows: ...
```

Markers esperados por data:

```text
LAB7_DAILY_BACKFILL_CONFIG_OK
LAB7_DAILY_BACKFILL_RUN_OK
LAB7_STAGE_METRICS_BY_DATE_WRITTEN_OK
LAB7_BACKFILL_VOLUME_SPIKE_SIGNAL_OK
LAB7_DAILY_BACKFILL_STAGE_METRICS_OK
```

Use `source_rows_for_date` e `records_read` como ponte entre volume de negócio e
execução Spark. `input_bytes` vem do contador `bytesRead` do StageMetrics e é
somente um sinal de apoio nesta stack Delta/S3A; ele não representa de forma
confiável o tamanho físico da tabela Delta.

`records_read` é um contador da execução capturada, não uma validação funcional
da quantidade de linhas. O objetivo é observar correlação com o volume esperado,
não exigir igualdade exata; a diferença local de 45 registros não deve ser
generalizada.

### Fronteiras de tempo

- `executor_run_time_ms` mapeia `executorRunTime` agregado pelo StageMetrics;
- application/submit wall-clock inclui a vida completa de uma aplicação, mas
  não é persistido nesta tabela do Lab 7;
- batch wall-clock inclui a orquestração das 14 aplicações e, na medição local,
  também Compose e geração/validação da fonte.

Custos de startup e encerramento pertencem ao wall-clock do submit ou do batch.
Eles não explicam diretamente `executor_run_time_ms`.

## 6. Inicie a lente read-only

Execute o comando específico do Lab 7:

```bash
make lab7-dashboard
```

O target confirma a stack principal, constrói a imagem fixada do dashboard,
inicia `wsm-lab7-dashboard` e expõe o Streamlit na porta `28501`.

Abra:

```text
http://127.0.0.1:28501
```

O caminho de leitura é:

```text
Streamlit -> DuckDB -> Delta table on MinIO
```

Streamlit e DuckDB somente leem a tabela Delta persistida. Eles não iniciam uma
query Spark, não coletam StageMetrics e não constituem a solução de
observabilidade: o dashboard é uma lente de apresentação sobre a evidência.

## 7. Apresente o batch completo de cima para baixo

Selecione o `run_id` completo. O dashboard escolhe inicialmente o batch mais
recente, mas antes de apresentar confirme `Processed dates: 14`.

### 7.1 Comece pelo plano conhecido

Exemplo validado do batch completo:

```text
Processed dates:      14
Expected source rows: 2,210,000
Spike days:           3
Max expected rows:    1,000,000
Max records read:     1,000,045
Max shuffle written:  about 20.6 MB
```

Esses números são evidência da calibração local, não thresholds universais.

### 7.2 Relacione volume, leitura e shuffle

As datas devem permanecer em ordem cronológica. As barras `100x` aparecem nos
dias 04 e 11; o sinal `10x`, no dia 07.

Intervalos locais validados de `shuffle_bytes_written`:

```text
normal:       487,357-522,139 B
medium spike: 2,484,042 B
large spike:  21,572,070-21,583,136 B
```

Use também o scatter de volume versus shuffle. O objetivo não é decorar os
valores, mas observar se `source_rows_for_date`, `records_read` e shuffle mudam
coerentemente entre as classes 1x, 10x e 100x.

### 7.3 Interprete runtime, GC e spill com limites

Use volume versus executor runtime como uma relação empírica. O runtime pode
crescer menos linearmente que o volume, mas essa forma não deve ser atribuída
automaticamente ao startup do `spark-submit`; esse custo pertence a outra
fronteira de medição.

No batch validado, `memory_bytes_spilled` e `disk_bytes_spilled` foram zero nas
14 datas. Isso demonstra apenas que nenhum spill foi observado nessas
execuções. Não demonstra ausência de pressão por GC nem de outros gargalos.

### 7.4 Use as visões normalizadas como comparação, não diagnóstico

O gráfico normalizado preserva os valores por milhão de linhas esperadas. O
índice complementar compara cada métrica com a mediana dos dias `NORMAL`, para
runtime, shuffle e tasks compartilharem uma escala legível. Esses gráficos
ajudam a localizar mudanças; não provam sozinhos sua causa.

A normalização por milhão é uma transformação aritmética, não uma extrapolação
de capacidade. Para um dia `NORMAL` com 10,000 linhas, os valores observados são
multiplicados por 100; custos fixos existentes dentro da execução, como o número
mínimo de stages, tasks e trabalho constante do workload, também são
amplificados. Um valor por milhão maior no dia pequeno não prova ineficiência
nem prevê o comportamento de uma execução real com um milhão de linhas.

### Checkpoint de raciocínio — comportamento ao longo do tempo

- **Pergunta:** os dias 1x, 10x e 100x alteraram os sinais de execução de forma
  coerente com o volume conhecido?
- **Hipótese:** `records_read` e shuffle devem acompanhar o volume conhecido.
  Executor runtime pode crescer de forma menos linear, sem que isso seja
  atribuído automaticamente ao startup do `spark-submit`.
- **Evidência:** volume plan, 14 datas do mesmo `run_id`, `records_read`, shuffle
  read/write, executor runtime, tasks, GC e o zero real de spill observado no
  batch validado.
- **Conclusão:** StageMetrics persistidas com data de negócio permitem localizar
  quais partições temporais mudaram o perfil operacional.
- **Limitação:** executor runtime, application/submit wall-clock e batch
  wall-clock têm fronteiras distintas. A fonte controlada, as relações e o
  dashboard localizam sinais, mas não provam causa raiz; spill zero também não
  exclui outros gargalos.

## 8. Caminhos opcionais depois da aula principal

### 8.1 Inspecione a evidência persistida

Outputs de negócio são separados por data:

```text
s3a://lakehouse/gold/lab7/daily_activity_dashboard/processing_date=<YYYY-MM-DD>/filter_strategy=early_partition_filter
```

As linhas de StageMetrics são adicionadas a:

```text
s3a://observability/lab7/daily_backfill_stage_metrics
```

As 14 linhas de um batch compartilham o mesmo `run_id`; cada data possui seu
próprio `date_run_id` e `application_id`.

Abra o MinIO Console somente se a turma precisar inspecionar o layout Delta:

```text
http://127.0.0.1:29011
```

### 8.2 Correlacione com as aplicações no History Server

Abra:

```text
http://127.0.0.1:28090
```

Uma data corresponde a uma aplicação Spark. Use `processing_date` nos logs e o
`application_id` persistido para correlacionar tempo de negócio, StageMetrics e
detalhe da Spark UI.

### 8.3 Controles específicos de preparação

Gere ou valide somente a fonte temporal isolada:

```bash
make generate-lab7
```

Gere primeiro as fontes retail compartilhadas e, ao final, a fonte do Lab 7:

```bash
make generate-all SCALE=xs
```

O runner público normal já garante sua fonte; use esses targets apenas quando a
preparação do ambiente ou a geração isolada fizer parte da explicação.

### 8.4 Cleanup opcional

```bash
make down
```

Esse comando para a plataforma e preserva a evidência gerada. Use o soft cleanup
do projeto somente quando o próximo ensaio precisar começar com storage vazio.

## Conclusão e ponte para produção

O Lab 7 muda a unidade de análise: uma linha de StageMetrics deixa de ser observada
isoladamente e passa a compor uma série associada ao tempo de negócio.

```text
known volume by business date
  -> correlated StageMetrics history
  -> explainable temporal signals
  -> deeper investigation when relationships change
```

Em produção, o próximo passo é transformar esse padrão em baselines por janela,
alertas e detecção de drift. Quando uma relação histórica muda, ela aponta onde
investigar; a confirmação da causa ainda exige contexto do workload, Spark UI,
planos, logs e, quando a pergunta depender da distribuição, métricas mais
granulares.
