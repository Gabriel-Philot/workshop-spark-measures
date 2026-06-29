# TODO: annotate Spark jobs for readable History Server demos

Date: 2026-06-26.

## Context

Spark History Server shows jobs inside each application mostly by Spark/Delta callsite when no explicit job description is set. For workshop demos this makes the Jobs tab hard to read because descriptions such as `first at ...`, `Delta: Filtering files for query`, and Scala anonymous functions do not explain the lab step.

Application names are already controlled by experiment config, but the individual jobs within an application should be easier to identify during a live walkthrough.

## Goal

Add a small pattern for annotating Spark actions with readable job descriptions, starting with Lab 0 and the app template.

The History Server Jobs tab should make it clear which action belongs to which lab step, for example:

```text
LAB0 | source_profile | table=sales | count_rows
LAB0 | source_profile | table=sales | count_files
LAB0 | sales_skew | top_vendors
LAB0 | relationship_check | sales_to_products
```

## Proposed approach

Use Spark's native APIs before actions:

```python
context.spark.sparkContext.setJobDescription(
    "LAB0 | source_profile | table=sales | count_rows"
)
sales.count()
```

Optionally use `setJobGroup()` around related sections:

```python
context.spark.sparkContext.setJobGroup(
    "lab0-source-profile",
    "LAB0 | source profiling generated bronze sources",
)
```

Clear or replace descriptions/groups between sections so one label does not leak into unrelated Spark actions.

## Scope

- Add the pattern to `src/apps/template.md`.
- Apply the pattern to Lab 0 Spark actions.
- Keep app-level naming in `src/config/experiments.yaml` as-is.
- Do not introduce a larger framework abstraction unless repeated lab usage proves it necessary.

## Acceptance criteria

- Lab 0 still runs successfully in native and sparkMeasure modes.
- Spark History Jobs tab shows meaningful descriptions for Lab 0 actions instead of only callsites.
- Descriptions are concise and consistent enough to read during the workshop.
- `make tests` and `make validate` still pass.

## Notes

- This improves Spark UI/History readability; it does not change sparkMeasure metrics.
- `setJobDescription()` is the main user-facing improvement for the Jobs table.
- `setJobGroup()` may be useful for broader grouping, but the implementation should stay minimal.
