# TODO: expose generator volume metrics in CLI logs

## Context

The first validated generator run worked, but after `make clean-data` the MinIO manifest was removed with the generated data. The remaining local log preserved logical volume only:

```text
sales_rows=200000
hot_vendor_share=0.7000
sales_files=22
```

For workshop iteration, the generator should print physical volume metrics directly to stdout/log so we can recover them even after MinIO cleanup.

## Goal

Make `make generate` print enough volumetry to understand the generated dataset without opening the manifest.

Expected terminal/log output should include at least:

```text
GENERATOR_VOLUME table=sales rows=200000 files=22 total_bytes=... min_file_bytes=... avg_file_bytes=... max_file_bytes=...
GENERATOR_VOLUME table=vendors rows=20 files=... total_bytes=...
GENERATOR_VOLUME table=products rows=200 files=... total_bytes=...
GENERATOR_VOLUME table=customers rows=10000 files=... total_bytes=...
```

## Tasks

- Extend generator validation to collect active Delta file stats for every generated table, not only `sales`.
- Print table-level volume metrics from the CLI after generation.
- Keep the manifest as the durable source in `s3a://observability/generator-runs/<run_id>/manifest.json`.
- Add unit coverage for volume metric formatting if the logic is factored into a pure helper.
- Run `make tests`.
- Rebuild and run `make generate SCALE=demo` when Docker images are available.

## Acceptance criteria

- `make generate SCALE=demo` prints one `GENERATOR_VOLUME` line per generated table.
- The log contains physical byte totals and file stats.
- Existing `GENERATOR_VALIDATION_OK` and `GENERATOR_OK` markers remain unchanged.
