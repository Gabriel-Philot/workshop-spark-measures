# sparkMeasure mechanics presentation

Apresentação estática em HTML/CSS/JS sobre como o sparkMeasure funciona por dentro.

Esta pasta complementa a apresentação de origem em `presentation/sparkmeasure_origin`.
Aqui o foco não é o contexto histórico do CERN, mas a mecânica técnica:

- rastros nativos do Spark;
- diferença entre plano físico e métricas de runtime;
- eventos do Spark listener bus;
- coleta por `StageMetrics`;
- coleta por `TaskMetrics`;
- tradeoff de overhead;
- uso das métricas como evidência operacional.

## Como abrir

1. Sirva esta pasta com um servidor HTTP estático.
2. Abra `theory.html` no navegador.
3. Para apresentar, coloque o navegador em tela cheia.

Exemplo local:

```bash
cd presentation/sparkmeasure_mecanix
python3 -m http.server 28502
```

Depois abra:

```text
http://127.0.0.1:28502/theory.html
```

## Páginas

```text
sparkmeasure_mecanix/
├── theory.html        # apresentação principal sobre mecânica interna
├── theory.css
├── theory.js
├── index.html         # material herdado do pacote original
├── styles.css
├── script.js
├── docs/
│   ├── sparkmeasure_theory_source.md
│   └── sparkmeasure_theory_storyboard.md
└── assets/
    ├── generated/
    ├── svg/
    └── video/
```

## Observação

O arquivo `index.html` está preservado porque veio no pacote original, mas a entrada
principal desta pasta é `theory.html`.

Os arquivos em `docs/` registram a fonte narrativa e o storyboard da seção teórica.
