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
13. Confirmar que, ao final de cada tarefa, toda a narrativa do guide, inclusive
    a prosa preexistente em inglês, está em português. Comandos, outputs
    literais, paths, markers, nomes de campos e termos técnicos permanecem
    intactos.
14. Confirmar que os checkpoints foram integrados às seções existentes e não
    criaram um segundo resumo do mesmo experimento.
15. Confirmar que cada campo de Pergunta, Hipótese, Evidência, Conclusão e
    Limitação possui normalmente uma ou duas frases.
16. Confirmar que tabelas, métricas, expected outputs, comandos e
    troubleshooting não foram duplicados.
17. Confirmar que detalhes condensados continuam disponíveis em uma class note
    explicitamente linkada e que nenhum conteúdo operacional útil foi removido
    sem preservação equivalente.
18. Confirmar que nenhum guide cresceu materialmente sem justificativa e sem
    compensação pela remoção ou reorganização de conteúdo repetido.
19. Confirmar que a padronização atingiu o raciocínio pedagógico, não a estrutura
    operacional específica de cada lab.

## Validação técnica

### Links internos e inbound

- Verificar todos os links Markdown relativos presentes nos guides.
- Preservar links para README, class notes, walkthroughs e post-mortems, salvo
  quando substituídos por referência equivalente e válida.
- Procurar no repositório inteiro referências que apontam para anchors dos
  guides:

```bash
rg -n 'guide_lab[0-7]\.md#[^ )]+' .
```

- Para cada referência encontrada, validar o fragmento contra o heading final do
  guide correspondente.
- Verificar headings repetidos, quebrados ou incompatíveis com links inbound.

### Escopo cumulativo da branch

Usar a base real da branch, não apenas o worktree atual:

```bash
BASE_SHA=$(git merge-base main HEAD)

git diff --name-status "$BASE_SHA"...HEAD
git diff "$BASE_SHA"...HEAD --check

git diff --check
git diff --cached --check
git status --short
```

O diff cumulativo pode conter somente:

```text
src/apps/labs/lab_0/guide_lab0.md
...
src/apps/labs/lab_7/guide_lab7.md
dev-todos/backlog/*
dev-todos/done/*
```

Qualquer README, código Python, runner, YAML, schema ou mudança de runtime no
diff cumulativo é falha do gate.

### Evidência adicional

- Comparar comandos e markers dos guides com source, runners e YAML.
- Executar `make tests`.

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
