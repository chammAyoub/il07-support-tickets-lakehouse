# Lakehouse Module (`lakehouse/`) — MinIO & Apache Hudi

**Owner:** Mohamed BAILLA (*Étudiant 2 — Data Lake MinIO & Lakehouse Apache Hudi*)

## Storage Layout (`s3a://lakehouse/`)

| Zone | S3A URI | Format | Purpose |
| :--- | :--- | :--- | :--- |
| **Bronze (Tickets)** | `s3a://lakehouse/bronze/tickets` | Parquet / JSON (append) | Immutable raw arrivals from Kafka topic `tickets` |
| **Bronze (Updates)** | `s3a://lakehouse/bronze/updates` | Parquet / JSON (append) | Immutable raw status transitions from `mises_a_jour_tickets` |
| **Silver** | `s3a://lakehouse/silver/tickets_hudi` | Apache Hudi (`MERGE_ON_READ`) | Deduplicated & NLP-classified tickets (`recordkey=ticket_id`, `precombine=updated_at`, `partitionpath=predicted_category`, `PartialUpdateAvroPayload` + `GLOBAL_SIMPLE`) |
| **Gold (SLA)** | `s3a://lakehouse/gold/sla_by_category` | Apache Hudi (`COPY_ON_WRITE`) | Aggregated MTTR & SLA compliance by category and priority |
| **Gold (Volumes)** | `s3a://lakehouse/gold/daily_volumes` | Apache Hudi (`COPY_ON_WRITE`) | Daily ticket volume & resolution counts |
| **Models** | `s3a://lakehouse/models/nlp_pipeline` | Spark MLlib | Saved NLP classification pipeline |

## Integration API for Teammates

```python
from lakehouse.spark_hudi_session import get_spark_session, get_lakehouse_paths
from lakehouse.hudi_tables import write_bronze_raw, upsert_silver_hudi, write_gold_hudi

spark = get_spark_session(app_name="IL07-Streaming", with_kafka=True)
write_bronze_raw(raw_df, source="tickets")
upsert_silver_hudi(enriched_df)
```

> **Note for `ingestion/` and `spark_jobs/`:** `normalize_ticket_records()` inside `hudi_tables.py` automatically normalizes both Imane's French Kafka fields (`texte`, `categorie`, `priorite`, `statut`, `nouveau_statut`, `date_creation`, `date_mise_a_jour`) and the English Master Guide schema (`description`, `predicted_category`, `predicted_priority`, `status`, `created_at`, `updated_at`, `resolved_at`).

## Common Commands

Start MinIO and initialize the `lakehouse` bucket:

```bash
docker compose up -d minio minio-init
```

Generate the 500-ticket Day-2 Gold seed tables in MinIO:

```bash
python -m lakehouse.hudi_maintenance --action seed-gold --seed-rows 500
```

Run Hudi compaction, cleaner, and commit inspection:

```bash
python -m lakehouse.hudi_maintenance --action all --retain-commits 5
```
