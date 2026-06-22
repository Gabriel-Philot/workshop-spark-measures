# Experiment Pattern

Each workshop experiment combines one workload with optional observability. The platform owns session lifecycle, configuration, artifact IO, metric collection, metadata, and persistence.

## Execution boundary

```text
load config
  -> create/reuse SparkSession singleton
  -> experiment.prepare(context)
  -> sparkMeasure.begin()
  -> experiment.workload(context)
  -> sparkMeasure.end()
  -> experiment.validate(result, context)
  -> persist metrics artifact
  -> experiment.cleanup(context)
  -> stop SparkSession
```

Only `workload()` is measured. Setup, validation, metrics persistence, and cleanup do not contaminate workload measurements.

## Experiment implementation

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

Experiments are registered in `src/config/experiments.yaml`. Defaults are deeply merged into each named experiment and `${ENV_VAR:-default}` expressions are expanded at load time.

Observability can be disabled or configured with `stage` or `task` collection. Metrics persistence targets a named output artifact, so experiment code does not own storage paths or write options.
