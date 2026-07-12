# sparkMeasure mechanics presentation

Static HTML/CSS/JavaScript presentation about Spark runtime traces, listeners,
StageMetrics, TaskMetrics, and observability overhead.

## Run locally

From the repository root:

```bash
python3 -m http.server 28503 --bind 0.0.0.0 --directory presentation/sparkmeasure_mecanix
```

Open [http://127.0.0.1:28503/theory.html](http://127.0.0.1:28503/theory.html).

The main entry point is `theory.html`; `index.html` is retained from the original
package.
