# Workshop presentation index

This directory contains three independent static presentations for the
sparkMeasure workshop. Each folder provides its own HTML, CSS, JavaScript, and
assets.

## Teaching sequence

| Order | Presentation | Main entry point | Dedicated port | Role in the narrative |
| ---: | --- | --- | ---: | --- |
| 1 | `sparkmeasure_origin/` | `index.html` | `28502` | Introduces the origin of sparkMeasure, CERN, data scale, and the need for observability. |
| 2 | `sparkmeasure_mecanix/` | `theory.html` | `28503` | Explains the internal mechanics: Spark listeners, StageMetrics, TaskMetrics, and overhead. |
| 3 | `sparkmeasure_recap/` | `index.html` | `28504` | Recaps the path from fundamentals to policies, telemetry contracts, and responsible automation. |

## Serve all three presentations

Run each command in a separate terminal from the repository root.

### Terminal 1 — origin

```bash
python3 -m http.server 28502 --bind 0.0.0.0 --directory presentation/sparkmeasure_origin
```

### Terminal 2 — mechanics

```bash
python3 -m http.server 28503 --bind 0.0.0.0 --directory presentation/sparkmeasure_mecanix
```

### Terminal 3 — recap

```bash
python3 -m http.server 28504 --bind 0.0.0.0 --directory presentation/sparkmeasure_recap
```

Classroom links:

- origin: [http://127.0.0.1:28502/](http://127.0.0.1:28502/);
- mechanics: [http://127.0.0.1:28503/theory.html](http://127.0.0.1:28503/theory.html);
- recap: [http://127.0.0.1:28504/](http://127.0.0.1:28504/).

## Port contract

The presentations use a dedicated port block that does not overlap with the
public workshop services:

| Service | Port |
| --- | ---: |
| Spark History Server | `28090` |
| Spark Master UI | `28091` |
| Lab 7 dashboard | `28501` |
| Origin presentation | `28502` |
| Mechanics presentation | `28503` |
| Recap presentation | `28504` |
| MinIO API | `29010` |
| MinIO Console | `29011` |

There is no overlap between the three presentation ports and the services
defined in `.env.example` and `build/docker-compose.yml`.

## Workshop serving drill

When asked to expose the presentations:

1. start all three servers on the documented ports;
2. verify that each entry point responds over HTTP;
3. return the three links separately and identify the presentation opened by
   each link;
4. keep the processes running until asked to stop them.

These servers only publish static files locally. They do not start Spark,
MinIO, or any other workshop platform service.
