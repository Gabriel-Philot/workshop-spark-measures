# Runtime Pattern

Each workshop workload is executed by the runtime package. The platform owns session lifecycle, configuration, artifact IO, metric collection, metadata, and persistence.

## Execution boundary

```text
load config
  -> create/reuse SparkSession singleton
  -> workload.prepare(context)
  -> sparkMeasure.begin()
  -> workload.workload(context)
  -> sparkMeasure.end()
  -> workload.validate(result, context)
  -> persist metrics artifact
  -> workload.cleanup(context)
  -> stop SparkSession
```

Only `workload()` is measured. Setup, validation, metrics persistence, and cleanup do not contaminate workload measurements.

## Runtime package

The runtime code lives under:

```text
src/spark_workshop/runtime/
```

It intentionally remains separate from:

```text
jobs/      -> readable lab contract: extract, transform, load, validate
metrics/   -> sparkMeasure StageMetrics/TaskMetrics adapters
config/    -> YAML-to-typed-config loading
artifacts/ -> named read/write/stats helpers
```

## Workload implementation

```python
class ExampleExperiment(SparkExperiment):
    def prepare(self, context):
        # Optional unmeasured setup.
        pass

    def workload(self, context):
        source = context.read("source")
        return source.groupBy("key").count().collect()

    def validate(self, result, context):
        assert result

    def cleanup(self, context):
        # Optional unmeasured cleanup.
        pass
```

The context exposes:

- `context.spark`: singleton SparkSession.
- `context.config`: typed experiment configuration.
- `context.logger`: workshop logger.
- `context.read(name)` and `context.write(name, dataframe)`: named artifacts.
- `context.artifact_path(name)`: named storage roots.

## Configuration

Workloads are registered as named experiments in `src/config/experiments.yaml` or a lab-local `experiments.yaml`. Defaults are deeply merged into each named experiment and `${ENV_VAR:-default}` expressions are expanded at load time.

Observability can be disabled or configured with `stage` or `task` collection. Metrics persistence targets a named output artifact, so workload code does not own storage paths or write options.
