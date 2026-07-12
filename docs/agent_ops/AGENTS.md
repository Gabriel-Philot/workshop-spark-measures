# AGENTS.md

## Scope
- Applies to this entire repository.

## Stack
- Apache Spark 4.1.2, Scala 2.13, Delta Lake 4.2.0, sparkMeasure 0.28.0, and MinIO.
- Use `make` as the public operational interface and `uv` for local Python tests.
- Keep external artifacts pinned and downloaded by `make bootstrap`.

## Validation
- Run `make tests` for Python changes.
- Run `make validate` for infrastructure changes.
- Run `make dry-test` for changes affecting Spark, Delta, MinIO, or sparkMeasure.

## Git delivery drills
- A **push drill** means: run the scope-appropriate validation, review the diff,
  create scoped commit(s) when needed, push the current feature branch, and
  return a PR link, title, and English description. Never merge the PR
  automatically.
- After the user confirms the PR was merged, fetch and prune the remote, verify
  the feature commits are reachable from `origin/main`, fast-forward the local
  `main`, and only then delete the feature branch locally and remotely. If the
  commits are not on `origin/main`, stop and report the mismatch; never delete
  the branch first.
- Keep commits separated by requested scope and leave the final worktree clean.

## Soft project cleanup drill
- A **soft cleanup drill** is project-scoped: run `make down`,
  `make clean-data`, and `make removeimage`, then verify that this Compose
  project's containers and volumes, `workshop-spark-measures-*` images, and
  MinIO runtime data are gone.
- Never use global Docker cleanup such as `docker system prune`, and never
  remove unrelated containers, volumes, images, source files, or configuration.

## Conventions
- Prefer small, workshop-oriented examples over framework abstractions.
- Never commit `.env`, downloaded JARs/wheels, image contexts, or runtime data.
- Record durable decisions in `MEMORY.md` using the repository-wide memory format.
