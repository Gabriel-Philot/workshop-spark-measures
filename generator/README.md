# Workshop data generator

Schema-first data generator for Spark Measures labs.

The generator creates related Delta tables in the `lakehouse` bucket and writes a manifest to the `observability` bucket. It is intentionally a lab setup tool: sparkMeasure should measure the workload that reads these tables, not the generation step itself.

## First scenario

`retail_sales_skew` creates:

- `vendors`
- `products`
- `customers`
- `sales`

The fact table has valid foreign keys and a configurable hot vendor so join skew and uneven file sizes can be demonstrated later.

## Local run

With the platform built and Compose services available:

```bash
make generate SCALE=demo
```

Useful overrides:

```bash
make generate SCALE=xs
make generate SCALE=demo GENERATOR_VALIDATE=0
make generate SCALE=demo GENERATOR_RUN_ID=my-run
```

The default contract is `generator/configs/retail_sales_skew.yaml`.
