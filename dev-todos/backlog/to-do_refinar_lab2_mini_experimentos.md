# To-do: organizar o Lab 2 como quatro mini-experimentos

## Contexto

O Lab 2 reúne quatro perguntas diferentes. O guia deve manter a conexão com as
questões selecionadas do exame Databricks Data Engineer Professional, mas a
história principal precisa ser a leitura disciplinada de evidência Spark.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_2/guide_lab2.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_2/README.md`
- `src/apps/labs/lab_2/docs/exam_questions.md`
- os quatro scripts `lab_2a` até `lab_2d`
- `src/apps/labs/lab_2/lab_2_utils/experiments.yaml`
- runtimes e transformations em `src/apps/labs/lab_2/lab_2_utils/`
- evidências locais já registradas no guia e no documento de questões

## Enquadramento da aula

- **Pergunta norteadora:** como transformar relações numéricas da Spark UI em
  perguntas diagnósticas reproduzíveis com sparkMeasure?
- **Por que esta aula aparece agora:** o Lab 1 ensinou a escolher a granularidade;
  o Lab 2 exercita essa escolha em quatro sintomas diferentes.
- **Resultado de aprendizagem:** distinguir movimento excessivo, pressão
  agregada, outlier no extremo superior e partitions vazias no extremo inferior.
- **Modo de condução:** quatro exercícios principais do aluno, executados ao vivo
  e discutidos pelo instrutor; questões de exame funcionam como contexto.

## Alterações pedagógicas

1. Manter uma única preparação compartilhada e apontar o bootstrap completo para
   o Lab 0.
2. Preservar todos os comandos e seletores de configuração dos quatro scripts.
3. Organizar cada exercício como uma mini-história independente.
4. Manter a tabela final que compara 2A–2D.
5. Rotular números `SCALE=xs` como exemplos locais validados.
6. Mover Spark History, MinIO, cleanup e apêndices depois dos quatro experimentos
   e da comparação final.
7. Não transformar preparação para certificação na tese principal da aula.

## Checkpoints de raciocínio

### 2A — distribuição desnecessária antes da agregação

- **Pergunta:** a repartição não alinhada aumenta shuffle e volume de tasks?
- **Hipótese:** a variante baseline movimenta linhas largas e cria trabalho sem
  necessidade antes de agregar.
- **Evidência:** baseline versus optimized em tasks, stages, shuffle read/write,
  executor runtime e compatibilidade do resultado.
- **Conclusão:** relações menores de movimento e tasks sustentam a otimização.
- **Limitação:** o experimento não afirma que toda repartição é ruim nem que o
  shuffle deve chegar a zero.

### 2B — pressão agregada de shuffle, spill e GC

- **Pergunta:** os agregados sustentam pressão de shuffle, spill ou GC?
- **Hipótese:** carregar payload largo e reparticionar cedo aumenta movimento e
  pode cruzar o limite de memória disponível.
- **Evidência:** shuffle, memory spill, disk spill e relação entre `jvmGCTime` e
  `executorRunTime` nas variantes pressure e default.
- **Conclusão:** spill positivo prova que houve spill nessa execução; GC deve ser
  interpretado como relação, não por um threshold universal.
- **Limitação:** StageMetrics não identifica qual task concentrou a pressão e a
  execução local não reproduz necessariamente o percentual da questão original.

### 2C — outlier no extremo superior

- **Pergunta:** uma ou poucas tasks processaram muito mais dados que a task
  típica?
- **Hipótese:** a chave dominante cria uma task com máximo muito acima de p75.
- **Evidência:** relações max/p75 de duration, shuffle records e shuffle bytes no
  TaskMetrics report.
- **Conclusão:** a distribuição sustenta um sinal de skew controlado.
- **Limitação:** a aula para no diagnóstico; não prova uma solução de produção.

### 2D — partitions vazias ou quase vazias

- **Pergunta:** o extremo inferior contém tasks sem trabalho útil?
- **Hipótese:** partitions em excesso produzem mínimos próximos de zero enquanto
  a mediana continua positiva.
- **Evidência:** mínimo, mediana, p75, máximo e registros lidos por task.
- **Conclusão:** TaskMetrics torna visível o desperdício que o total agregado
  esconderia.
- **Limitação:** tasks vazias são um sintoma; a métrica sozinha não prova qual
  escolha de partitioning originou o problema.

## Ponte para a próxima aula

Encerrar perguntando qual é o custo de coletar evidência mais detalhada. Essa
pergunta abre o benchmark de observabilidade do Lab 3.

## Critérios de aceite

- Há um setup compartilhado e quatro mini-experimentos claramente separados.
- 2A–2D possuem todos os cinco campos do checkpoint.
- StageMetrics permanece em 2A/2B e TaskMetrics em 2C/2D.
- A tabela comparativa final foi preservada.
- Questões de exame continuam referenciadas sem dominar a narrativa.
- Exemplos locais estão rotulados e não viraram thresholds universais.
- O guia termina com a ponte para o Lab 3.

## Validação e gate

1. Conferir comandos, configs, markers, collectors e outputs contra os quatro
   scripts e o YAML.
2. Conferir links para `exam_questions.md` e headings.
3. Executar `git diff --check`.
4. Mostrar o diff antes do commit.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab2): organizar diagnósticos como mini-experimentos
```
