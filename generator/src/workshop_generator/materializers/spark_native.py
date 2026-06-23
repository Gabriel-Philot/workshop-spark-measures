"""Spark-native Delta materializer for relational workshop datasets."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from spark_workshop.session import SparkSessionSingleton
from spark_workshop.utils import logger
from workshop_generator.contract import GeneratorContract


class SparkNativeMaterializer:
    def __init__(self, contract: GeneratorContract, run_id: str, validate: bool = True):
        self.contract = contract
        self.run_id = run_id
        self.validate = validate

    def run(self) -> dict[str, Any]:
        from pyspark.sql import functions as F

        logger.info(
            f"Starting generator scenario={self.contract.name} scale={self.contract.scale.name} run_id={self.run_id}"
        )
        spark = SparkSessionSingleton.get_or_create(self.contract.app_name)
        spark.sparkContext.setLogLevel("WARN")
        try:
            vendors = self._vendors(spark, F)
            products = self._products(spark, F)
            customers = self._customers(spark, F)
            sales = self._sales(spark, F)

            self._write(vendors, "vendors", partition_by=())
            self._write(products, "products", partition_by=())
            self._write(customers, "customers", partition_by=())
            self._write(sales, "sales", partition_by=self.contract.write.sales_partition_by)

            validation = self._validate_outputs(spark, F) if self.validate else {"skipped": True}
            manifest = self._manifest(spark, validation)
            self._write_manifest(spark, manifest)
            logger.info(
                f"Completed generator scenario={self.contract.name} run_id={self.run_id}"
            )
            return manifest
        finally:
            SparkSessionSingleton.stop()

    def _vendors(self, spark: Any, F: Any) -> Any:
        scale = self.contract.scale
        return (
            spark.range(1, scale.vendor_rows + 1, 1, numPartitions=min(scale.vendor_rows, scale.partitions))
            .withColumnRenamed("id", "vendor_id")
            .withColumn("vendor_name", F.concat(F.lit("vendor_"), F.lpad(F.col("vendor_id").cast("string"), 6, "0")))
            .withColumn(
                "region",
                F.when((F.col("vendor_id") % 10) < 5, F.lit("US"))
                .when((F.col("vendor_id") % 10) < 7, F.lit("BR"))
                .when((F.col("vendor_id") % 10) < 9, F.lit("EU"))
                .otherwise(F.lit("APAC")),
            )
        )

    def _products(self, spark: Any, F: Any) -> Any:
        scale = self.contract.scale
        return (
            spark.range(0, scale.product_rows, 1, numPartitions=scale.partitions)
            .withColumn("product_id", F.col("id") + F.lit(1))
            .withColumn("vendor_id", (F.col("id") % F.lit(scale.vendor_rows)) + F.lit(1))
            .withColumn("product_name", F.concat(F.lit("product_"), F.lpad(F.col("product_id").cast("string"), 8, "0")))
            .withColumn("category_id", (F.col("product_id") % F.lit(50)) + F.lit(1))
            .drop("id")
        )

    def _customers(self, spark: Any, F: Any) -> Any:
        scale = self.contract.scale
        return (
            spark.range(1, scale.customer_rows + 1, 1, numPartitions=scale.partitions)
            .withColumnRenamed("id", "customer_id")
            .withColumn("customer_name", F.concat(F.lit("customer_"), F.lpad(F.col("customer_id").cast("string"), 9, "0")))
            .withColumn(
                "region",
                F.when((F.col("customer_id") % 10) < 5, F.lit("US"))
                .when((F.col("customer_id") % 10) < 7, F.lit("BR"))
                .when((F.col("customer_id") % 10) < 9, F.lit("EU"))
                .otherwise(F.lit("APAC")),
            )
        )

    def _sales(self, spark: Any, F: Any) -> Any:
        scale = self.contract.scale
        skew = self.contract.vendor_skew
        hot_threshold = int(skew.hot_vendor_share * 10_000)
        hot_bucket = _hash_mod(F, F.col("sale_id"), F.lit(self.contract.seed), modulo=10_000)
        raw_non_hot_vendor = _hash_mod(
            F, F.col("sale_id"), F.lit(self.contract.seed + 1), modulo=scale.vendor_rows - 1
        ) + F.lit(1)
        non_hot_vendor = F.when(
            raw_non_hot_vendor >= F.lit(skew.hot_vendor_id), raw_non_hot_vendor + F.lit(1)
        ).otherwise(raw_non_hot_vendor)
        vendor_id = F.when(hot_bucket < F.lit(hot_threshold), F.lit(skew.hot_vendor_id)).otherwise(non_hot_vendor)
        product_slot = _hash_mod(
            F, F.col("sale_id"), F.lit(self.contract.seed + 2), modulo=scale.products_per_vendor
        )
        day_offset = _hash_mod(
            F, F.col("sale_id"), F.lit(self.contract.seed + 4), modulo=self.contract.time.days
        )

        dataframe = (
            spark.range(1, scale.sales_rows + 1, 1, numPartitions=scale.partitions)
            .withColumnRenamed("id", "sale_id")
            .withColumn("vendor_id", vendor_id.cast("long"))
            .withColumn("product_id", (product_slot * F.lit(scale.vendor_rows) + F.col("vendor_id")).cast("long"))
            .withColumn("customer_id", (_hash_mod(F, F.col("sale_id"), F.lit(self.contract.seed + 3), modulo=scale.customer_rows) + F.lit(1)).cast("long"))
            .withColumn("sale_date", F.date_add(F.to_date(F.lit(self.contract.time.start_date)), day_offset.cast("int")))
            .withColumn("quantity", (_hash_mod(F, F.col("sale_id"), F.lit(self.contract.seed + 5), modulo=5) + F.lit(1)).cast("int"))
            .withColumn("unit_price", F.round(F.rand(self.contract.seed + 6) * F.lit(500.0) + F.lit(1.0), 2))
            .withColumn("sale_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
        )
        for index in range(scale.payload_columns):
            dataframe = dataframe.withColumn(
                f"payload_{index + 1}",
                F.substring(
                    F.sha2(F.concat_ws(":", F.col("sale_id").cast("string"), F.lit(index), F.lit(self.contract.seed)), 256),
                    1,
                    max(1, min(scale.payload_width, 256)),
                ),
            )
        return dataframe.repartition(scale.partitions, "vendor_id")

    def _write(self, dataframe: Any, table: str, partition_by: tuple[str, ...]) -> None:
        path = self.contract.paths.table_path(table)
        writer = (
            dataframe.write.format("delta")
            .mode(self.contract.write.mode)
            .option("overwriteSchema", "true")
            .option("maxRecordsPerFile", str(self.contract.scale.max_records_per_file))
        )
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        if self.contract.write.mode.lower() == "overwrite":
            _delete_path(spark=dataframe.sparkSession, path=path)
        logger.info(f"Writing generated table={table} path={path} partition_by={partition_by}")
        writer.save(path)

    def _validate_outputs(self, spark: Any, F: Any) -> dict[str, Any]:
        paths = self.contract.paths
        vendors = spark.read.format("delta").load(paths.table_path("vendors"))
        products = spark.read.format("delta").load(paths.table_path("products"))
        customers = spark.read.format("delta").load(paths.table_path("customers"))
        sales = spark.read.format("delta").load(paths.table_path("sales"))

        counts = {
            "vendors": vendors.count(),
            "products": products.count(),
            "customers": customers.count(),
            "sales": sales.count(),
        }
        expected_counts = {
            "vendors": self.contract.scale.vendor_rows,
            "products": self.contract.scale.product_rows,
            "customers": self.contract.scale.customer_rows,
            "sales": self.contract.scale.sales_rows,
        }
        if counts != expected_counts:
            raise RuntimeError(f"Generated table counts differ from contract: actual={counts} expected={expected_counts}")

        invalid_vendor_fk = sales.join(vendors.select("vendor_id"), "vendor_id", "left_anti").count()
        invalid_product_fk = sales.join(products.select("product_id", "vendor_id"), ["product_id", "vendor_id"], "left_anti").count()
        invalid_customer_fk = sales.join(customers.select("customer_id"), "customer_id", "left_anti").count()
        if invalid_vendor_fk or invalid_product_fk or invalid_customer_fk:
            raise RuntimeError(
                "Generated FK validation failed: "
                f"vendor={invalid_vendor_fk} product={invalid_product_fk} customer={invalid_customer_fk}"
            )

        hot_vendor_id = self.contract.vendor_skew.hot_vendor_id
        vendor_counts = sales.groupBy("vendor_id").count()
        hot_count = vendor_counts.where(F.col("vendor_id") == hot_vendor_id).select("count").first()[0]
        hot_share = hot_count / counts["sales"]
        expected_share = self.contract.vendor_skew.hot_vendor_share
        tolerance = self.contract.vendor_skew.tolerance
        if abs(hot_share - expected_share) > tolerance:
            raise RuntimeError(
                f"Generated hot vendor share {hot_share:.4f} differs from expected {expected_share:.4f} by more than {tolerance:.4f}"
            )

        file_stats = _data_file_stats_for_files(spark, sales.inputFiles())
        if file_stats["file_count"] < 1:
            raise RuntimeError("Generated sales table has no Delta data files")

        logger.info(
            "GENERATOR_VALIDATION_OK "
            f"sales_rows={counts['sales']} hot_vendor_share={hot_share:.4f} "
            f"sales_files={file_stats['file_count']}"
        )
        return {
            "counts": counts,
            "foreign_key_violations": {
                "vendor": invalid_vendor_fk,
                "product": invalid_product_fk,
                "customer": invalid_customer_fk,
            },
            "hot_vendor_id": hot_vendor_id,
            "hot_vendor_count": hot_count,
            "hot_vendor_share": hot_share,
            "sales_file_stats": file_stats,
        }

    def _manifest(self, spark: Any, validation: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "scenario": self.contract.name,
            "scale": self.contract.scale.name,
            "engine": self.contract.engine,
            "application_id": spark.sparkContext.applicationId,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "paths": {
                "vendors": self.contract.paths.table_path("vendors"),
                "products": self.contract.paths.table_path("products"),
                "customers": self.contract.paths.table_path("customers"),
                "sales": self.contract.paths.table_path("sales"),
                "manifest": self.contract.paths.manifest_path(self.run_id),
            },
            "contract": {
                "seed": self.contract.seed,
                "scale": asdict(self.contract.scale),
                "vendor_skew": asdict(self.contract.vendor_skew),
                "write": asdict(self.contract.write),
                "time": asdict(self.contract.time),
            },
            "validation": validation,
        }

    def _write_manifest(self, spark: Any, manifest: dict[str, Any]) -> None:
        path = self.contract.paths.manifest_path(self.run_id)
        text = json.dumps(manifest, sort_keys=True, indent=2)
        _write_text(spark, path, text)
        logger.info(f"Wrote generator manifest path={path}")


def _hash_mod(F: Any, *columns: Any, modulo: int) -> Any:
    if modulo <= 0:
        raise ValueError("modulo must be positive")
    return F.pmod(F.xxhash64(*columns), F.lit(modulo)).cast("long")


def _data_file_stats_for_files(spark: Any, files: list[str]) -> dict[str, int | float]:
    sizes = _file_sizes(spark, files)
    if not sizes:
        return {
            "file_count": 0,
            "total_bytes": 0,
            "min_file_bytes": 0,
            "max_file_bytes": 0,
            "avg_file_bytes": 0.0,
        }
    return {
        "file_count": len(sizes),
        "total_bytes": int(sum(sizes)),
        "min_file_bytes": int(min(sizes)),
        "max_file_bytes": int(max(sizes)),
        "avg_file_bytes": float(sum(sizes) / len(sizes)),
    }



def _file_sizes(spark: Any, files: list[str]) -> list[int]:
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    sizes: list[int] = []
    for file_path in files:
        path = jvm.org.apache.hadoop.fs.Path(file_path)
        fs = path.getFileSystem(hconf)
        if fs.exists(path):
            sizes.append(int(fs.getFileStatus(path).getLen()))
    return sizes


def _delete_path(spark: Any, path: str) -> None:
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    target = jvm.org.apache.hadoop.fs.Path(path)
    fs = target.getFileSystem(hconf)
    if fs.exists(target):
        logger.info(f"Deleting existing generated path before overwrite path={path}")
        fs.delete(target, True)

def _write_text(spark: Any, path: str, text: str) -> None:
    jvm = spark.sparkContext._jvm
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    manifest_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = manifest_path.getFileSystem(hconf)
    parent = manifest_path.getParent()
    if parent is not None and not fs.exists(parent):
        fs.mkdirs(parent)
    stream = fs.create(manifest_path, True)
    try:
        stream.write(bytearray(text.encode("utf-8")))
    finally:
        stream.close()
