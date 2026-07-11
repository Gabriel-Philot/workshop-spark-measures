# To-do: refinar o enquadramento e o bootstrap do Lab 0

## Contexto

O Lab 0 cumpre duas funções diferentes: preparar a plataforma local e introduzir
o argumento central do workshop. O guia precisa separar essas funções sem
remover nenhum comando necessário para uma instalação iniciada do zero.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_0/guide_lab0.md
```

Nenhum outro arquivo do lab deverá ser modificado durante esta tarefa.

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_0/README.md`
- `src/apps/labs/lab_0/docs/contract_rationale.md`
- `src/apps/labs/lab_0/docs/spark_ui_to_sparkmeasure_walkthrough.md`
- `src/apps/labs/lab_0/lab_0a_source_inventory.py`
- `src/apps/labs/lab_0/lab_0b_sparkmeasure_native_api.py`
- `src/apps/labs/lab_0/lab_0c_sparkmeasure_presentation.py`
- `src/apps/labs/lab_0/lab_0_utils/experiments.yaml`

## Enquadramento da aula

Adicionar no início um bloco conciso com:

- **Pergunta norteadora:** quais evidências o Spark oferece nativamente, o que o
  sparkMeasure condensa e por que o workshop usa um contrato de execução?
- **Por que esta aula aparece agora:** a plataforma e o vocabulário de evidência
  precisam estar estabelecidos antes dos labs de diagnóstico.
- **Resultado de aprendizagem:** distinguir preparação da plataforma, evidência
  nativa do Spark, API nativa do sparkMeasure e contrato do workshop.
- **Modo de condução:** preparação pré-aula obrigatória; aula principal,
  conduzida pelo instrutor e executada ao vivo.

## Alterações pedagógicas

1. Separar explicitamente:
   - **Preparação pré-aula da plataforma:** repository root, `make bootstrap`,
     `make build`, `make compose`, `make dry-test` e geração das fontes Bronze.
   - **Fluxo da aula:** 0A, 0B, 0C e jornada pela Spark UI.
2. Manter o Lab 0 como referência canônica para bootstrap dos outros labs.
3. Preservar todos os comandos, paths, markers, credenciais locais e resultados
   esperados exatamente como estão.
4. Não apresentar job ID, stage ID ou duração local como identificador estável.
5. Manter a jornada pela Spark UI ligada ao 0C, pois ela participa do argumento
   Spark nativo versus sparkMeasure.
6. Deixar MinIO e limpeza ao final como material operacional opcional.
7. Integrar o enquadramento e os checkpoints às seções existentes, reutilizando
   pergunta, evidência e limitação já documentadas em vez de criar um segundo
   resumo do mesmo experimento.
8. Manter toda a narrativa criada ou editada no guide em português, sem traduzir
   comandos, paths, markers, nomes de campos ou termos técnicos.

## Checkpoints de raciocínio

### 0A — inventário das fontes

- **Pergunta:** as fontes existem e possuem volume, layout e relacionamentos
  adequados para os próximos labs?
- **Hipótese:** o gerador produziu as quatro tabelas e manteve as chaves de
  relacionamento esperadas.
- **Evidência:** linhas, arquivos, bytes, tamanhos por arquivo e violações de
  chaves apresentados pelo bloco de inventário.
- **Conclusão:** as fontes estão prontas para os experimentos quando os markers e
  validações passam.
- **Limitação:** o inventário descreve os dados; não diagnostica performance.

### 0B — API nativa do sparkMeasure

- **Pergunta:** como o sparkMeasure delimita e agrega uma ação Spark?
- **Hipótese:** `StageMetrics.begin()` e `end()` ao redor da ação produzem um
  resumo compacto de métricas de stage.
- **Evidência:** API visível no código, relatório nativo e métricas agregadas.
- **Conclusão:** o collector mede uma região executada, não o script inteiro.
- **Limitação:** agregados de stage não explicam a distribuição entre tasks.

### 0C — Spark nativo e contrato do workshop

- **Pergunta:** o que fica mais simples com o contrato e o que ainda exige
  Spark UI ou `explain`?
- **Hipótese:** a execução observada condensa sinais úteis sem substituir plano,
  jobs, stages e executors.
- **Evidência:** mesma transformação nos modos native e observed, saída do
  `explain`, bloco do sparkMeasure e jornada documentada pela Spark UI.
- **Conclusão:** contrato, sparkMeasure e ferramentas nativas são complementares.
- **Limitação:** um único job da UI não representa toda a aplicação e a aula não
  prova uma causa raiz de performance.

## Ponte para a próxima aula

Encerrar mostrando que o Lab 0 apresentou as fontes e ferramentas; o Lab 1 usa
essa base para a primeira investigação real, começando em StageMetrics e
aprofundando com TaskMetrics somente quando necessário.

## Critérios de aceite

- Preparação pré-aula e aula estão visualmente separadas.
- O `Enquadramento da aula` contém os quatro campos obrigatórios.
- Os três experimentos possuem checkpoints completos.
- Spark UI permanece parte relevante da narrativa do 0C.
- MinIO e limpeza aparecem depois do conteúdo principal.
- O guia termina com `Ponte para a próxima aula`.
- Todo texto pedagógico está em português, sem trechos narrativos bilíngues;
  termos técnicos e interfaces permanecem intactos.
- Os checkpoints reutilizam o conteúdo existente, têm normalmente uma ou duas
  frases por campo e não duplicam métricas, outputs, comandos ou troubleshooting.
- O guide não cresce materialmente sem que conteúdo repetido seja removido,
  reorganizado ou direcionado para uma class note explicitamente linkada.
- Nenhum comando, path, marker ou comportamento foi alterado.

## Validação e gate

1. Conferir links relativos e headings.
2. Comparar comandos e markers com scripts e YAML do Lab 0.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário e aguardar aprovação.
5. Somente após aprovação, mover este todo para `done` e criar o commit:

```text
docs(lab0): refinar argumento pedagógico da introdução
```
