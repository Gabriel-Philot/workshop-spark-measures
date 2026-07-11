# To-do: revisar a consistência entre os oito guias

## Contexto

Esta tarefa só poderá começar depois que os todos dos Labs 0–7 estiverem em
`done`. Ela não cria uma estrutura genérica; apenas verifica se as decisões
pedagógicas ficaram consistentes entre documentos independentes.

## Arquivos que poderão ser alterados

Somente quando a auditoria encontrar uma inconsistência:

```text
src/apps/labs/lab_0/guide_lab0.md
...
src/apps/labs/lab_7/guide_lab7.md
```

## Auditoria obrigatória

1. Confirmar que cada guia possui `Enquadramento da aula` próximo ao início.
2. Confirmar os quatro campos:
   - Pergunta norteadora;
   - Por que esta aula aparece agora;
   - Resultado de aprendizagem;
   - Modo de condução.
3. Confirmar que cada experimento principal termina com:
   - Pergunta;
   - Hipótese;
   - Evidência;
   - Conclusão;
   - Limitação.
4. Confirmar que cada guia termina com `Ponte para a próxima aula`.
5. Verificar delivery modes: principal/opcional, demonstração/exercício e
   ao vivo/evidência pré-computada.
6. Verificar terminologia técnica consistente:
   - sparkMeasure;
   - StageMetrics;
   - TaskMetrics;
   - Spark UI e Spark History Server;
   - indisponibilidade versus zero real.
7. Confirmar que TaskMetrics aparece somente quando a pergunta depende da
   distribuição entre tasks.
8. Confirmar que números locais estão rotulados como exemplos validados.
9. Confirmar que IDs, durações e thresholds locais não foram convertidos em
   invariantes universais.
10. Confirmar que observação, interpretação e prova de causa raiz permanecem
    distintas.
11. Confirmar que Lab 0 é a referência de bootstrap e Labs 1–7 continuam
    utilizáveis isoladamente por meio de pré-requisitos curtos e links precisos.
12. Confirmar que comandos, paths, markers, schemas e runtime não mudaram.

## Validação técnica

- Verificar todos os links Markdown relativos.
- Verificar headings repetidos ou quebrados.
- Comparar comandos e markers dos guides com source, runners e YAML.
- Executar `git diff --check`.
- Executar `make tests`.
- Usar `git diff --name-only` para provar que nenhum README principal, código,
  runner ou YAML foi alterado.

## Relatório final esperado

Relatar para cada Lab 0–7:

- qual guia mudou;
- qual era o ruído pedagógico;
- qual argumento ficou explícito;
- quais materiais foram mantidos como opcionais;
- quais evidências e limitações foram preservadas.

## Gate e commit

Mostrar qualquer diff transversal ao usuário antes do commit. Após aprovação,
mover este todo para `done` e criar, somente se houver correções:

```text
docs(guides): alinhar consistência pedagógica entre labs
```
