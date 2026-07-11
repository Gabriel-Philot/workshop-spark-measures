# To-do: refinar a evidência temporal do Lab 7

## Contexto

O Lab 7 muda a unidade de análise: deixa de observar uma única execução e passa
a relacionar várias aplicações com datas de negócio conhecidas. O dashboard é
uma lente sobre métricas persistidas, não a solução de observabilidade em si.

## Arquivo que poderá ser alterado

```text
src/apps/labs/lab_7/guide_lab7.md
```

## Leitura obrigatória antes da edição

- `src/apps/labs/lab_7/README.md`
- `src/apps/labs/lab_7/docs/temporal_backfill_observability_class_notes.md`
- os dois scripts Python do Lab 7
- `src/apps/labs/lab_7/run_temporal_backfill_observability.sh`
- runners, YAML, volume plan e transformations em `lab_7_utils/`
- `src/apps/labs/lab_7/dashboard/`
- evidência validada do batch completo de 14 datas

## Enquadramento da aula

- **Pergunta norteadora:** quais datas de negócio alteraram o perfil de execução
  do backfill e quais relações de StageMetrics tornam isso visível?
- **Por que esta aula aparece agora:** os labs anteriores diagnosticaram ou
  governaram uma execução; o Lab 7 adiciona contexto temporal persistido.
- **Resultado de aprendizagem:** correlacionar volume esperado, records read,
  shuffle, runtime, tasks, GC e spill por data.
- **Modo de condução:** demonstração principal do instrutor; batch completo ao
  vivo quando houver tempo e fallback com evidência pré-computada; smoke test
  opcional.

## Alterações pedagógicas

1. Manter o pré-requisito específico: o runner público inicia Compose e garante
   a fonte temporal isolada.
2. Preservar o smoke de duas datas como preflight opcional, mesmo aparecendo
   antes do batch por necessidade operacional.
3. Manter o batch completo de 14 datas como evidência principal.
4. Rotular `575.37 seconds` como exemplo local validado, não duração garantida.
5. Documentar dois fallbacks:
   - se já existir um batch Delta com 14 datas, executar `make lab7-dashboard` e
     selecionar o `run_id` completo;
   - se não existir evidência persistida, usar a tabela validada das class notes
     como discussão pré-computada, sem alegar dashboard ao vivo.
6. Explicar que Streamlit e DuckDB somente leem a tabela persistida.
7. Manter MinIO, 14 aplicações no History, Make controls e cleanup como caminhos
   opcionais depois da aula principal.

## Checkpoint de raciocínio — comportamento ao longo do tempo

- **Pergunta:** os dias 1x, 10x e 100x alteram os sinais de execução de forma
  coerente com o volume conhecido?
- **Hipótese:** records read acompanha o volume; shuffle separa os grupos com
  clareza; runtime cresce de forma menos linear por causa do custo fixo por
  submit.
- **Evidência:** volume plan, 14 datas do mesmo batch, records read,
  shuffle read/write, executor runtime, tasks, GC e zero real de spill quando
  disponível.
- **Conclusão:** StageMetrics persistidas com data de negócio permitem localizar
  quais partições temporais mudaram o perfil operacional.
- **Limitação:** a fonte é controlada, o dashboard não prova causa raiz e ausência
  de spill não significa ausência de outros gargalos.

## Ponte para a próxima aula

Como não há Lab 8, encerrar conectando o padrão a monitoramento histórico de
produção: baselines por janela, alertas, drift e investigação detalhada quando
as relações mudarem.

## Critérios de aceite

- A pergunta temporal aparece antes dos comandos.
- O batch completo é a evidência principal e o smoke continua opcional.
- O fallback pré-computado não depende de inventar novos arquivos ou serviços.
- O dashboard é apresentado como lente read-only.
- Números e duração local estão claramente rotulados.
- O guia termina com a ponte para uso em produção.

## Validação e gate

1. Conferir datas, volume plan, comandos, markers, paths, dashboard port e
   métricas contra código, runners e YAML.
2. Conferir links e headings.
3. Executar `git diff --check`.
4. Mostrar o diff ao usuário.
5. Após aprovação, mover para `done` e criar:

```text
docs(lab7): refinar narrativa de evidência temporal
```
