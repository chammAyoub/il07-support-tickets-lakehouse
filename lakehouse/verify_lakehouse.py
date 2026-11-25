"""End-to-end verification of MinIO S3A, Bronze append, Silver Hudi partial upsert, Gold seed, and maintenance."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from lakehouse.hudi_maintenance import query_incremental, run_clean, run_compaction, show_commits
from lakehouse.hudi_tables import seed_gold_tables, upsert_silver_hudi, write_bronze_raw
from lakehouse.spark_hudi_session import get_lakehouse_paths, get_spark_session


def run_verification() -> None:
    """Execute end-to-end Lakehouse checks against MinIO using Spark and Apache Hudi."""
    paths = get_lakehouse_paths()
    spark = get_spark_session(app_name="IL07-Lakehouse-Verify")

    try:
        now = datetime.now(timezone.utc)
        t0 = (now - timedelta(hours=3)).isoformat()
        t1 = now.isoformat()

        # 1. Simulate Imane's French-schema Kafka tickets
        initial_tickets = spark.createDataFrame(
            [
                {
                    "ticket_id": "T-VERIFY-001",
                    "texte": "Je ne peux pas me connecter a mon compte",
                    "categorie": "account",
                    "priorite": "haute",
                    "statut": "ouvert",
                    "date_creation": t0,
                },
                {
                    "ticket_id": "T-VERIFY-002",
                    "texte": "Je souhaite demander un remboursement",
                    "categorie": "refund",
                    "priorite": "moyenne",
                    "statut": "ouvert",
                    "date_creation": t0,
                },
                {
                    "ticket_id": "T-VERIFY-003",
                    "texte": "Mon paiement a ete refuse",
                    "categorie": "payment",
                    "priorite": "haute",
                    "statut": "ouvert",
                    "date_creation": t0,
                },
            ]
        )

        bronze_count = write_bronze_raw(initial_tickets, source="tickets")
        assert bronze_count == 3, f"Expected 3 Bronze rows, got {bronze_count}"
        print(f"[PASS] Bronze tickets written: {bronze_count}")

        silver_insert_count = upsert_silver_hudi(initial_tickets)
        assert silver_insert_count == 3, f"Expected 3 Silver rows upserted, got {silver_insert_count}"

        # 2. Simulate partial update from mises_a_jour_tickets (missing text, category, priority, created_at)
        partial_updates = spark.createDataFrame(
            [
                {
                    "ticket_id": "T-VERIFY-002",
                    "ancien_statut": "ouvert",
                    "nouveau_statut": "resolu",
                    "assigned_agent": "agent_04",
                    "resolution_notes": "Refund confirmed",
                    "date_mise_a_jour": t1,
                }
            ]
        )
        write_bronze_raw(partial_updates, source="updates")
        upsert_silver_hudi(partial_updates)

        # 3. Verify Silver Hudi state: 3 rows total, partial update preserved original columns
        silver_df = spark.read.format("hudi").load(paths["silver_tickets"])
        verify_rows = silver_df.filter(silver_df["ticket_id"].startswith("T-VERIFY-")).collect()
        assert len(verify_rows) == 3, f"Expected 3 unique verify rows (no duplicates), got {len(verify_rows)}"

        t2 = [r for r in verify_rows if r["ticket_id"] == "T-VERIFY-002"][0]
        assert t2["status"] == "RESOLVED", f"Expected RESOLVED, got {t2['status']}"
        assert t2["predicted_category"] == "REFUND", f"Lost category: {t2['predicted_category']}"
        assert t2["predicted_priority"] == "MEDIUM", f"Lost priority: {t2['predicted_priority']}"
        assert t2["description"] == "Je souhaite demander un remboursement", f"Lost description: {t2['description']}"
        assert t2["sla_target_hours"] == 24.0, f"Lost sla_target_hours: {t2['sla_target_hours']}"
        assert t2["assigned_agent"] == "agent_04", f"Missing assigned_agent: {t2['assigned_agent']}"
        assert t2["resolution_hours"] is not None and t2["resolution_hours"] >= 2.5, (
            f"Unexpected resolution_hours: {t2['resolution_hours']}"
        )
        print(
            "[PASS] Silver Hudi partial-update verified: "
            f"status={t2['status']}, category={t2['predicted_category']}, "
            f"priority={t2['predicted_priority']}, resolution_hours={t2['resolution_hours']}"
        )

        # 4. Seed Gold tables (500 rows -> sla_by_category + daily_volumes)
        gold_counts = seed_gold_tables(spark, num_tickets=500)
        sla_df = spark.read.format("hudi").load(paths["gold_sla_by_category"])
        vol_df = spark.read.format("hudi").load(paths["gold_daily_volumes"])
        assert sla_df.count() == gold_counts["sla_by_category"]
        assert vol_df.count() == gold_counts["daily_volumes"]
        print(f"[PASS] Gold Hudi tables seeded and readable: {gold_counts}")

        # 5. Maintenance: compaction, cleaner, commits, incremental query
        run_compaction(spark, paths["silver_tickets"])
        run_clean(spark, paths["silver_tickets"], retain_commits=5)
        show_commits(spark, paths["silver_tickets"], limit=5)
        inc_df = query_incremental(spark, paths["silver_tickets"], begin_instant="000")
        assert inc_df.count() >= 3
        print("[PASS] Hudi compaction, cleaner, commits, and incremental query verified.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_verification()
