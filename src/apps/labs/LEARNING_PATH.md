# Trilha de aprendizagem dos Labs 0–7

## 1. Propósito

### Público-alvo

Esta trilha foi escrita para engenheiros de dados e participantes que já
conhecem os fundamentos de Spark e querem usar observabilidade para investigar
execuções, avaliar mudanças e sustentar decisões de engenharia.

### Pergunta central do workshop

A pergunta central do workshop é:

> Como transformar evidências de uma execução Spark em diagnóstico e decisão
> explicáveis, sem ultrapassar o que as métricas realmente permitem concluir?

### Observar, diagnosticar e governar

**Observar** é localizar e coletar sinais. **Diagnosticar** é relacionar esses
sinais a uma hipótese e reconhecer seus limites. **Governar** é usar evidências,
critérios e contratos explícitos para decidir se a telemetria pode alimentar
automação e se uma mudança pode ser promovida. Ao adicionar histórico, essas
decisões deixam de olhar apenas uma execução e passam a considerar seu
comportamento ao longo do tempo de negócio.

> **Como usar este documento:** ele conecta os guides dos Labs 0–7 e mostra como
> uma conclusão cria a pergunta seguinte. Ele não substitui os guides
> individuais, que continuam sendo a fonte para execução, material de aula e
> detalhes operacionais.

---

## 2. Escada de aprendizagem

```text
evidência nativa
  -> medição agregada e distribuição
  -> experimentação controlada
  -> custo da observação
  -> fingerprint operacional
  -> decisão por policy
  -> contrato confiável de telemetria
  -> histórico temporal e drift
```

> **Leitura da escada:** ela representa uma evolução pedagógica, não uma
> dependência obrigatória de execução entre todos os labs.

Cada etapa amplia a pergunta anterior: primeiro localizamos evidências; depois
escolhemos sua granularidade, testamos hipóteses, medimos o custo da coleta,
organizamos interpretações, aplicamos critérios, questionamos a confiabilidade
da telemetria e, por fim, acompanhamos relações ao longo do tempo.

---

## 3. Mapa dos Labs 0–7

Leia cada linha da pergunta central até a nova pergunta que ela desbloqueia.

| Lab | Pergunta central | Evidência ou decisão produzida | Como prepara o próximo lab |
| --- | --- | --- | --- |
| [Lab 0](lab_0/guide_lab0.md) | O que o Spark oferece nativamente e o que o sparkMeasure condensa? | Inventário das fontes, `explain`, Spark UI e StageMetrics como superfícies complementares. | O agregado mostra a execução, mas ainda não revela como o trabalho se distribui entre tasks. |
| [Lab 1](lab_1/guide_lab1.md) | Quando StageMetrics basta e quando a distribuição exige TaskMetrics? | Diagnóstico agregado de global sort e drill-down de task outlier sem comparar performance entre coletores. | A granularidade passa a ser escolhida pela pergunta e pode ser aplicada a sintomas controlados. |
| [Lab 2](lab_2/guide_lab2.md) | Como testar relações entre métricas em experimentos controlados? | Quatro mini-experimentos que separam hipótese, evidência, compatibilidade funcional, mudança operacional e limitação. | Antes de confiar nas comparações, precisamos medir o efeito da própria observação. |
| [Lab 3](lab_3/guide_lab3.md) | Qual custo o mecanismo de coleta adiciona dentro da fronteira medida? | Comparação disciplinada entre modos, com warmup, repetições, ruído e fronteiras temporais explícitas. | Reconhecido o custo e o limite da coleta, os agregados podem formar uma hipótese operacional. |
| [Lab 4](lab_4/guide_lab4.md) | Como organizar StageMetrics em uma primeira leitura do workload? | Fingerprint explicável composto por métricas, ratios, flags, disponibilidade e precedência de regras. | Descrever o workload não decide se uma mudança deve ser aceita. |
| [Lab 5](lab_5/guide_lab5.md) | Uma candidate compatível segundo os invariantes definidos também respeita a policy operacional? | Compatibilidade segundo invariantes escolhidos e decisão `PASS`, `FAIL` ou `WARNING_LOW_SIGNAL` baseada em budgets. | Se a decisão depende da telemetria, precisamos saber se ela preserva contexto suficiente para automação. |
| [Lab 6](lab_6/guide_lab6.md) | A linha normalizada preserva contexto suficiente para o consumidor decidir se pode utilizá-la? | Contrato de schema, semântica e correlação, com decisão separada da telemetria raw. | Uma linha contratada descreve uma execução; várias linhas correlacionadas permitem observar mudanças temporais. |
| [Lab 7](lab_7/guide_lab7.md) | Quais datas de negócio alteraram o perfil de execução do backfill? | Histórico de StageMetrics por data e dashboard read-only para relacionar volume conhecido e sinais operacionais. | Abre o caminho para baselines por janela, alertas, drift e investigação aprofundada. |

---

## 4. Transições importantes

As transições abaixo registram a mudança de raciocínio entre uma aula e a
seguinte.

### Lab 0 → Lab 1: do agregado à distribuição

O Lab 0 estabelece onde procurar: planos, jobs, stages, tasks, executors e Spark
UI continuam sendo evidência nativa, enquanto StageMetrics resume a região
medida. O Lab 1 mostra o limite desse resumo. Perguntas sobre concentração,
outliers e variabilidade interna exigem TaskMetrics. Essa passagem muda a
granularidade diagnóstica; não mede qual coletor deixa o workload mais rápido.

### Lab 1 → Lab 2: da escolha do coletor ao experimento

Escolher a granularidade correta não basta. O Lab 2 transforma sintomas em
quatro mini-experimentos nos quais hipótese, evidência e conclusão precisam
permanecer alinhadas. Uma mudança operacional só pode ser chamada de melhoria
quando a compatibilidade funcional relevante foi verificada; uma execução
bem-sucedida, isoladamente, não demonstra equivalência entre outputs.

### Lab 2 → Lab 3: da comparação ao custo de observar

Depois de usar métricas para comparar workloads, o próprio mecanismo de coleta
vira objeto de estudo. O Lab 3 separa `workload_wall_ms`, tempo do collector,
agregação e wall-clock do submit. Warmup e repetição reduzem algumas ameaças,
mas não eliminam startup, estado do ambiente ou ruído. O resultado é evidência
local dentro de uma fronteira, nunca um percentual universal de overhead.

### Lab 3 → Lab 4: de contadores a hipótese operacional

Com custo e limitações reconhecidos, StageMetrics pode ser interpretado por
regras simples. O Lab 4 organiza contadores, ratios e flags em um fingerprint.
O YAML define thresholds; o código também define precedência quando várias
flags são verdadeiras. O profile orienta a próxima investigação, mas não lê o
plano, não localiza uma task e não comprova causa raiz.

### Lab 4 → Lab 5: de descrição a decisão

Um fingerprint descreve sinais do workload, mas não contém a política de
aceitação de uma mudança. O Lab 5 introduz baseline, candidate, invariantes
funcionais e budgets. Primeiro confirma compatibilidade dentro dos checks
escolhidos; depois aplica a policy. Na demonstração, `FAIL` pode terminar com
exit code zero porque a aplicação funcionou e a rejeição é uma decisão
didática. Em produção, o orquestrador deve transformar essa decisão em bloqueio
da promoção.

### Lab 5 → Lab 6: da policy à confiança na telemetria

O Lab 5 pressupõe que a telemetria normalizada pode alimentar seus budgets. O
Lab 6 questiona essa premissa. Ele não testa se o sparkMeasure funciona nem a
precisão física interna de seus contadores; valida a linha após normalização,
enriquecimento e correlação.

Telemetria raw é o registro produzido antes da decisão do contrato. Telemetria
trusted seria um produto promovido após essa decisão, mas o lab não materializa
essa tabela. A presença de uma coluna `*_available` também não torna a métrica
automaticamente disponível: o consumidor precisa ler a flag e decidir se o
contador é obrigatório para sua pergunta. Um contrato `FAIL` significa que a
telemetria não sustenta aquela automação; não significa regressão de performance
do workload nem bloqueia consumidores por conta própria.

### Lab 6 → Lab 7: de uma execução ao tempo de negócio

Uma linha correlacionável permite auditar uma execução. O Lab 7 persiste várias
linhas associadas a datas de negócio e compara volume conhecido, leitura,
shuffle, runtime, tasks, GC e spill. O dashboard somente lê essa evidência: ele
é uma lente, não o sistema que coleta ou valida métricas. A correlação temporal
localiza datas que merecem investigação, mas não explica sozinha a causa da
mudança.

---

## 5. Ordem pedagógica versus ordem operacional

### Por que o workshop apresenta policy antes de contrato

O workshop apresenta policy antes de contrato. No Lab 5, o participante vê como
uma evidência operacional pode produzir uma decisão. No Lab 6, a pergunta fica
mais madura:

> As métricas utilizadas nessa decisão são confiáveis o suficiente para
> alimentar automação?

Essa inversão torna visível primeiro o valor do guardrail e depois o risco de
confiar em telemetria sem contrato. Os labs, porém, são isolados: o código do
Lab 5 não chama o Lab 6, o Lab 6 não bloqueia tecnicamente o Lab 5 e a ordem é de
aprendizagem, não de integração entre aplicações.

### Uma ordem operacional plausível

Uma pipeline real poderia ordenar os conceitos assim:

```text
executar baseline e candidate e coletar evidência
  -> validar compatibilidade funcional
  -> normalizar e persistir telemetria raw
  -> validar o contrato da telemetria (conceito do Lab 6)
  -> aplicar a policy quando a telemetria for utilizável (conceito do Lab 5)
  -> persistir evidência e decisão no histórico
  -> promover, bloquear ou declarar decisão indeterminada
  -> acompanhar baselines e drift (conceito do Lab 7)
```

Persistir evidência e decisão antes da promoção mantém no histórico casos
aprovados, rejeitados e indeterminados, evitando uma visão enviesada composta
somente por mudanças promovidas.

### Limites da decisão

Quatro limites preservam a lógica dessa pipeline:

1. O workload precisa executar para produzir métricas; observabilidade não evita
   retroativamente a execução medida.
2. Se a compatibilidade funcional falhar, a comparação de performance não deve
   aprovar a candidate.
3. Se o contrato da telemetria falhar, a policy não deve produzir `PASS`
   automático. Estados como `TELEMETRY_INVALID` ou `INDETERMINATE` representam
   um modelo conceitual e não estados já implementados pelos labs.
4. O bloqueio atua sobre a decisão ou promoção seguinte, não sobre a execução
   que gerou a evidência.

---

## 6. Modelo mental final

```text
observar
  -> formular hipótese
  -> localizar evidência
  -> reconhecer limitações
  -> decidir com contrato
  -> acompanhar ao longo do tempo
```

### Papéis complementares

Spark UI e evidência nativa ajudam a investigar; StageMetrics resume a execução;
TaskMetrics revela distribuição quando a pergunta exige esse detalhe;
fingerprints organizam hipóteses; policies transformam critérios em decisão;
contratos permitem que consumidores avaliem a telemetria antes de automatizar;
histórico e dashboards tornam mudanças temporais visíveis.

> **Regra final:** esses componentes são complementares. Nenhum deles,
> isoladamente, prova causa raiz. O valor da trilha está em escolher a evidência
> adequada, declarar a fronteira da conclusão e tornar explícito o que ainda
> precisa ser investigado.
