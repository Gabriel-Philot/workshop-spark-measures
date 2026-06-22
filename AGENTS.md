# AGENTS.md

## Scope
- Applies to this entire repository.
- Keep `../dataship/spark-plat-v0` read-only; it is reference material only.

## Stack
- Apache Spark 4.1.2, Scala 2.13, Delta Lake 4.2.0, sparkMeasure 0.28.0, and MinIO.
- Use `make` as the public operational interface and `uv` for local Python tests.
- Keep external artifacts pinned and downloaded by `make bootstrap`.

## Validation
- Run `make tests` for Python changes.
- Run `make validate` for infrastructure changes.
- Run `make dry-test` for changes affecting Spark, Delta, MinIO, or sparkMeasure.

## Conventions
- Prefer small, workshop-oriented examples over framework abstractions.
- Never commit `.env`, downloaded JARs/wheels, image contexts, or runtime data.
- Record durable decisions in `MEMORY.md` using the repository-wide memory format.
