"""Bronze, Silver (Apache Hudi upsert), and Gold writers for Project IL-07."""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta, timezone
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from lakehouse.spark_hudi_session import get_lakehouse_paths

STATUS_MAP: Dict[str, str] = {
    "ouvert": "OPEN",
    "open": "OPEN",
    "en_cours": "IN_PROGRESS",
    "in_progress": "IN_PROGRESS",
    "resolu": "RESOLVED",
    "résolu": "RESOLVED",
    "resolved": "RESOLVED",
    "ferme": "CLOSED",
    "fermé": "CLOSED",
    "clos": "CLOSED",
    "closed": "CLOSED",
}

PRIORITY_MAP: Dict[str, str] = {
    "basse": "LOW",
    "faible": "LOW",
    "low": "LOW",
    "moyenne": "MEDIUM",
    "medium": "MEDIUM",
    "haute": "HIGH",
    "high": "HIGH",
    "critique": "CRITICAL",
    "critical": "CRITICAL",
}

SLA_TARGET_BY_PRIORITY: Dict[str, float] = {
    "CRITICAL": 4.0,
    "HIGH": 8.0,
    "MEDIUM": 24.0,
    "LOW": 48.0,
}

SILVER_SCHEMA = StructType(
    [
        StructField("ticket_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("product", StringType(), True),
        StructField("subject", StringType(), True),
        StructField("description", StringType(), True),
        StructField("intent", StringType(), True),
        StructField("status", StringType(), True),
        StructField("assigned_agent", StringType(), True),
        StructField("resolution_notes", StringType(), True),
        StructField("predicted_category", StringType(), True),
        StructField("predicted_priority", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("resolved_at", TimestampType(), True),
        StructField("sla_target_hours", DoubleType(), True),
        StructField("resolution_hours", DoubleType(), True),
    ]
)


def _map_column(col_expr: F.Column, mapping: Dict[str, str]) -> F.Column:
    """Normalize string values using a case-insensitive lookup dictionary."""
    lowered = F.lower(F.trim(col_expr.cast(StringType())))
    result = F.upper(F.trim(col_expr.cast(StringType())))
    for src, dst in mapping.items():
        result = F.when(lowered == src, F.lit(dst)).otherwise(result)
    return result


def _coalesce_cols(df: DataFrame, *candidates: str, target_type=StringType()) -> F.Column:
    """Pick the first existing column among aliases, or typed NULL if none exist."""
    present = [F.col(c).cast(target_type) for c in candidates if c in df.columns]
    if not present:
        return F.lit(None).cast(target_type)
    if len(present) == 1:
        return present[0]
    return F.coalesce(*present)


def hudi_table_exists(spark: SparkSession, path: str) -> bool:
    """Check whether a Hudi table (.hoodie metadata folder) exists at the given path."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs_path = jvm.org.apache.hadoop.fs.Path(f"{path.rstrip('/')}/.hoodie")
    fs = fs_path.getFileSystem(conf)
    return bool(fs.exists(fs_path))


def normalize_ticket_records(df: DataFrame) -> DataFrame:
    """Align French (Kafka) and English (Master Guide) ticket schemas to Silver columns."""
    status_raw = _coalesce_cols(df, "status", "nouveau_statut", "statut")
    status_col = F.when(status_raw.isNotNull(), _map_column(status_raw, STATUS_MAP))

    prio_raw = _coalesce_cols(df, "predicted_priority", "priority", "priorite")
    prio_col = F.when(prio_raw.isNotNull(), _map_column(prio_raw, PRIORITY_MAP))

    cat_raw = _coalesce_cols(df, "predicted_category", "category", "categorie")
    cat_col = F.when(cat_raw.isNotNull(), F.upper(F.trim(cat_raw)))

    created_col = _coalesce_cols(df, "created_at", "date_creation", target_type=TimestampType())
    updated_raw = _coalesce_cols(df, "updated_at", "date_mise_a_jour", "created_at", "date_creation", target_type=TimestampType())
    updated_col = F.coalesce(updated_raw, F.current_timestamp())

    resolved_raw = _coalesce_cols(df, "resolved_at", target_type=TimestampType())
    resolved_col = F.when(
        resolved_raw.isNotNull(),
        resolved_raw,
    ).when(
        status_col.isin("RESOLVED", "CLOSED"),
        updated_col,
    ).otherwise(F.lit(None).cast(TimestampType()))

    sla_raw = _coalesce_cols(df, "sla_target_hours", target_type=DoubleType())
    sla_from_prio = F.lit(None).cast(DoubleType())
    for prio_label, hours in SLA_TARGET_BY_PRIORITY.items():
        sla_from_prio = F.when(prio_col == prio_label, F.lit(hours)).otherwise(sla_from_prio)
    sla_col = F.coalesce(sla_raw, sla_from_prio)

    res_hours_raw = _coalesce_cols(df, "resolution_hours", target_type=DoubleType())
    res_hours_calc = F.when(
        created_col.isNotNull() & resolved_col.isNotNull(),
        F.round((F.unix_timestamp(resolved_col) - F.unix_timestamp(created_col)) / 3600.0, 2),
    )
    res_hours_col = F.coalesce(res_hours_raw, res_hours_calc)

    normalized = df.select(
        F.col("ticket_id").cast(StringType()).alias("ticket_id"),
        _coalesce_cols(df, "customer_id").alias("customer_id"),
        _coalesce_cols(df, "channel").alias("channel"),
        _coalesce_cols(df, "product").alias("product"),
        _coalesce_cols(df, "subject").alias("subject"),
        _coalesce_cols(df, "description", "texte").alias("description"),
        _coalesce_cols(df, "intent").alias("intent"),
        status_col.alias("status"),
        _coalesce_cols(df, "assigned_agent").alias("assigned_agent"),
        _coalesce_cols(df, "resolution_notes").alias("resolution_notes"),
        cat_col.alias("predicted_category"),
        prio_col.alias("predicted_priority"),
        created_col.alias("created_at"),
        updated_col.alias("updated_at"),
        resolved_col.alias("resolved_at"),
        sla_col.alias("sla_target_hours"),
        res_hours_col.alias("resolution_hours"),
    )
    return normalized.filter(F.col("ticket_id").isNotNull())


def write_bronze_raw(
    df: DataFrame,
    source: str = "tickets",
    fmt: str = "parquet",
    path: str | None = None,
) -> int:
    """Archive raw incoming events immutably in MinIO Bronze zone partitioned by ingestion_date."""
    if df.rdd.isEmpty():
        return 0

    paths = get_lakehouse_paths()
    default_key = "bronze_updates" if source == "updates" else "bronze_tickets"
    target_path = path or paths[default_key]

    enriched = (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.to_date(F.current_timestamp()))
        .withColumn("source_topic", F.lit(source))
    )

    writer = enriched.write.mode("append").partitionBy("ingestion_date")
    if fmt.lower() == "json":
        writer.json(target_path)
    else:
        writer.parquet(target_path)
    return enriched.count()


def _enrich_partial_updates(spark: SparkSession, df: DataFrame, target_path: str) -> DataFrame:
    """Preserve partition path and compute resolution_hours when partial updates lack created_at/category."""
    needs_enrichment = df.filter(
        F.col("predicted_category").isNull() | F.col("created_at").isNull()
    )
    if needs_enrichment.rdd.isEmpty() or not hudi_table_exists(spark, target_path):
        return df.withColumn(
            "predicted_category",
            F.coalesce(F.col("predicted_category"), F.lit("UNCATEGORIZED")),
        )

    existing = spark.read.format("hudi").load(target_path).select(
        F.col("ticket_id").alias("_ex_ticket_id"),
        F.col("predicted_category").alias("_ex_category"),
        F.col("created_at").alias("_ex_created_at"),
    )

    joined = df.join(existing, df["ticket_id"] == existing["_ex_ticket_id"], "left")
    resolved_cat = F.coalesce(F.col("predicted_category"), F.col("_ex_category"), F.lit("UNCATEGORIZED"))
    resolved_created = F.coalesce(F.col("created_at"), F.col("_ex_created_at"))
    resolved_hours = F.when(
        F.col("resolution_hours").isNotNull(),
        F.col("resolution_hours"),
    ).when(
        resolved_created.isNotNull() & F.col("resolved_at").isNotNull(),
        F.round((F.unix_timestamp(F.col("resolved_at")) - F.unix_timestamp(resolved_created)) / 3600.0, 2),
    ).otherwise(F.lit(None).cast(DoubleType()))

    return (
        joined.withColumn("predicted_category", resolved_cat)
        .withColumn("created_at", resolved_created)
        .withColumn("resolution_hours", resolved_hours)
        .drop("_ex_ticket_id", "_ex_category", "_ex_created_at")
    )


def upsert_silver_hudi(
    df: DataFrame,
    table_type: str | None = None,
    path: str | None = None,
    normalize: bool = True,
) -> int:
    """Upsert normalized tickets or partial status updates into Silver Apache Hudi table."""
    if df.rdd.isEmpty():
        return 0

    spark = df.sparkSession
    target_path = path or get_lakehouse_paths()["silver_tickets"]
    resolved_table_type = table_type or os.getenv("HUDI_TABLE_TYPE", "MERGE_ON_READ")

    prepared = normalize_ticket_records(df) if normalize else df
    prepared = _enrich_partial_updates(spark, prepared, target_path)

    hudi_options = {
        "hoodie.table.name": "tickets_hudi",
        "hoodie.datasource.write.table.type": resolved_table_type,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "ticket_id",
        "hoodie.datasource.write.precombine.field": "updated_at",
        "hoodie.datasource.write.partitionpath.field": "predicted_category",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.PartialUpdateAvroPayload",
        "hoodie.index.type": "GLOBAL_SIMPLE",
        "hoodie.simple.index.update.partition.path": "false",
        "hoodie.combine.before.upsert": "true",
        "hoodie.upsert.shuffle.parallelism": "4",
        "hoodie.insert.shuffle.parallelism": "4",
        "hoodie.compact.inline": "false",
        "hoodie.write.concurrency.mode": "single_writer",
        "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    }

    prepared.write.format("hudi").options(**hudi_options).mode("append").save(target_path)
    return prepared.count()


def write_gold_hudi(
    df: DataFrame,
    table_name: str,
    recordkey: str,
    precombine: str = "computed_at",
    partitionpath: str | None = None,
    path: str | None = None,
) -> int:
    """Write aggregated SLA or volume metrics into a Gold Copy-on-Write Apache Hudi table."""
    if df.rdd.isEmpty():
        return 0

    paths = get_lakehouse_paths()
    default_path = paths.get(f"gold_{table_name}", f"s3a://{os.getenv('MINIO_BUCKET', 'lakehouse')}/gold/{table_name}")
    target_path = path or default_path

    enriched = df if precombine in df.columns else df.withColumn(precombine, F.current_timestamp())

    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": recordkey,
        "hoodie.datasource.write.precombine.field": precombine,
        "hoodie.upsert.shuffle.parallelism": "2",
        "hoodie.insert.shuffle.parallelism": "2",
    }
    if partitionpath:
        hudi_options["hoodie.datasource.write.partitionpath.field"] = partitionpath
        hudi_options["hoodie.datasource.write.hive_style_partitioning"] = "true"
    else:
        hudi_options["hoodie.datasource.write.keygenerator.class"] = "org.apache.hudi.keygen.NonpartitionedKeyGenerator"

    enriched.write.format("hudi").options(**hudi_options).mode("append").save(target_path)
    return enriched.count()


def seed_gold_tables(spark: SparkSession, num_tickets: int = 500) -> Dict[str, int]:
    """Generate a 500-ticket Day-2 seed dataset and publish initial Gold Hudi tables."""
    rng = random.Random(42)
    categories = ["BILLING", "TECHNICAL_SUPPORT", "ACCOUNT", "DELIVERY", "CANCELLATION"]
    priorities = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
    statuses = ["OPEN", "IN_PROGRESS", "RESOLVED", "CLOSED"]
    now = datetime.now(timezone.utc)

    rows = []
    for idx in range(1, num_tickets + 1):
        category = rng.choice(categories)
        priority = rng.choice(priorities)
        status = rng.choice(statuses)
        sla_hours = SLA_TARGET_BY_PRIORITY[priority]
        created = now - timedelta(days=rng.randint(0, 14), hours=rng.randint(0, 23))

        if status in {"RESOLVED", "CLOSED"}:
            # ~20% simulated SLA breaches
            is_breach = rng.random() < 0.20
            res_hours = round(sla_hours * rng.uniform(1.1, 2.2) if is_breach else sla_hours * rng.uniform(0.15, 0.95), 2)
            resolved = created + timedelta(hours=res_hours)
            updated = resolved
        else:
            res_hours = None
            resolved = None
            updated = created + timedelta(minutes=rng.randint(5, 120))

        rows.append(
            {
                "ticket_id": f"TCK-SEED-{idx:05d}",
                "predicted_category": category,
                "predicted_priority": priority,
                "status": status,
                "created_at": created,
                "updated_at": updated,
                "resolved_at": resolved,
                "sla_target_hours": sla_hours,
                "resolution_hours": res_hours,
            }
        )

    seed_df = spark.createDataFrame(rows)
    sla_by_category = (
        seed_df.groupBy("predicted_category", "predicted_priority")
        .agg(
            F.count("*").alias("total_tickets"),
            F.sum(F.when(F.col("status").isin("OPEN", "IN_PROGRESS"), 1).otherwise(0)).alias("open_tickets_count"),
            F.sum(F.when(F.col("status").isin("RESOLVED", "CLOSED"), 1).otherwise(0)).alias("resolved_tickets_count"),
            F.round(F.avg("resolution_hours"), 2).alias("avg_resolution_hours"),
            F.sum(
                F.when(
                    F.col("resolution_hours").isNotNull() & (F.col("resolution_hours") > F.col("sla_target_hours")),
                    1,
                ).otherwise(0)
            ).alias("sla_breach_count"),
        )
        .withColumn(
            "sla_compliance_pct",
            F.when(
                F.col("resolved_tickets_count") > 0,
                F.round(
                    100.0 * (F.col("resolved_tickets_count") - F.col("sla_breach_count")) / F.col("resolved_tickets_count"),
                    2,
                ),
            ).otherwise(F.lit(100.0)),
        )
        .withColumn("metric_key", F.concat_ws("_", F.col("predicted_category"), F.col("predicted_priority")))
        .withColumn("computed_at", F.current_timestamp())
    )

    daily_volumes = (
        seed_df.withColumn("ticket_date", F.date_format(F.col("created_at"), "yyyy-MM-dd"))
        .groupBy("ticket_date", "predicted_category")
        .agg(
            F.count("*").alias("ticket_count"),
            F.sum(F.when(F.col("status").isin("RESOLVED", "CLOSED"), 1).otherwise(0)).alias("resolved_count"),
        )
        .withColumn("volume_key", F.concat_ws("_", F.col("ticket_date"), F.col("predicted_category")))
        .withColumn("computed_at", F.current_timestamp())
    )

    sla_rows = write_gold_hudi(sla_by_category, table_name="sla_by_category", recordkey="metric_key")
    vol_rows = write_gold_hudi(
        daily_volumes,
        table_name="daily_volumes",
        recordkey="volume_key",
        partitionpath="ticket_date",
    )
    return {"sla_by_category": sla_rows, "daily_volumes": vol_rows}
