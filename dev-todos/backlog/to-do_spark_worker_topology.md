# TODO: define local Spark worker topology for WSL

Date: 2026-06-24.

## Context

The workshop platform currently runs one fixed Spark worker. For Spark diagnostics and sparkMeasure labs, multiple workers are useful because they make executor/task distribution easier to observe in Spark UI, History Server, and sparkMeasure task metrics.

We do not expect to run more than three local workers.

## Local machine validation

Measured from inside WSL2:

```text
Kernel: Linux 6.6.87.2-microsoft-standard-WSL2
CPU: AMD Ryzen 7 3800X 8-Core Processor
Logical CPUs visible: 16
Physical cores: 8
Threads per core: 2
RAM visible to WSL: 7.8 GiB
RAM available during check: 6.5 GiB
Swap: 4.0 GiB
Docker CPUs: 16
Docker memory: 8,325,898,240 bytes (~7.76 GiB)
Workspace disk: 1007 GiB total, 878 GiB available
```

Important interpretation: CPU is not the limiting factor. RAM is. The host has many CPU threads, but WSL/Docker is currently capped around 8 GiB. Spark workers plus driver, master, history server, MinIO, Python processes, and OS cache must fit inside that.

## Recommendation

Use a small fixed-worker topology instead of dynamic Compose scaling for now.

Preferred workshop defaults:

```env
SPARK_WORKER_REPLICAS=2
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
```

This gives Spark roughly:

```text
2 workers x 2 cores = 4 Spark worker cores
2 workers x 2g = 4 GiB worker memory
```

That leaves about 3-4 GiB for driver/master/history/MinIO/WSL overhead. This is the safest default for a small local WSL cluster.

Maximum local topology for demos:

```env
SPARK_WORKER_REPLICAS=3
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=1536m
```

This gives:

```text
3 workers x 2 cores = 6 Spark worker cores
3 workers x 1536m = 4.5 GiB worker memory
```

This is useful for showing multiple executors without consuming too much memory. Prefer this over `3 x 2g` on the current WSL memory cap.

Avoid as default:

```env
SPARK_WORKER_REPLICAS=3
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
```

Reason: 6 GiB just for worker memory is too close to the ~7.8 GiB WSL/Docker total once driver, master, History Server, MinIO, logs, shuffle, and Python overhead are included.

## Design direction

Use explicit worker services like the reference `spark-frm-mec` compose rather than `docker compose --scale`.

Reasoning:

- maximum worker count is small: 1-3;
- fixed service names are easier to explain in a workshop;
- Spark UI executor names/topology are easier to reason about;
- no need to fight Compose `container_name` scaling constraints;
- static services make History Server dependencies and readiness checks explicit.

Proposed services:

```text
spark-worker-1
spark-worker-2
spark-worker-3
```

Each worker should share the same image, env, volumes, cores, and memory variables.

## Proposed default implementation

- Add `spark-worker-1` and `spark-worker-2` as default services.
- Add `spark-worker-3` behind a Compose profile, e.g. `profiles: [three-workers]`, or keep it commented/disabled until needed.
- Keep worker memory/cores controlled by env vars.
- Update `.env.example` with conservative WSL defaults.
- Update `wait-ready.sh` to validate registered workers in Spark Master or at least known worker container readiness.
- Update docs with local sizing guidance.

## Acceptance criteria

- `make compose` starts a stable 2-worker local Spark cluster by default, or a documented 1-worker fallback if we decide to stay lighter.
- A documented command/profile can start 3 workers.
- Spark Master UI shows the expected number of alive workers.
- `make dry-test` still passes.
- `make generate SCALE=demo` still passes after images are rebuilt.
- Docs warn that 3 workers with 2g each is risky under the current ~8 GiB WSL memory cap.

## Notes for future scale presets

Given current disk availability, datasets up to hundreds of GB are possible from a storage perspective. RAM remains the main limit. Large local generation should prefer controlled data-shape pathologies over raw volume.
