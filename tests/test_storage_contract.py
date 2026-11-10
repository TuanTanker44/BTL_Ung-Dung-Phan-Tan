import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class StorageContractTests(unittest.TestCase):
    def test_fact_table_replaces_concurrent_versions_by_event_category(self):
        schema = (ROOT / "clickhouse" / "schema.sql").read_text(encoding="utf-8")
        fact = schema.split("CREATE TABLE IF NOT EXISTS fact_traffic_flow", 1)[1]
        fact = fact.split("-- Populate static dimension data", 1)[0]
        self.assertIn("ENGINE = ReplacingMergeTree()", fact)
        self.assertRegex(
            fact,
            r"ORDER BY\s*\(event_id,\s*vehicle_type_id\)",
        )

    def test_fact_reads_use_final(self):
        sources = [
            ROOT / "spark" / "spark_job.py",
            ROOT / "dashboard.py",
            ROOT / "query_clickhouse.py",
        ]
        unsafe = []
        pattern = re.compile(r"FROM\s+fact_traffic_flow(?!\s+FINAL)", re.IGNORECASE)
        for path in sources:
            for match in pattern.finditer(path.read_text(encoding="utf-8")):
                unsafe.append(f"{path.name}:{match.start()}")
        self.assertEqual([], unsafe)

    def test_existing_installation_has_rebuild_migration(self):
        migration = (
            ROOT / "clickhouse" / "migrations" / "002_make_traffic_facts_idempotent.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("ReplacingMergeTree()", migration)
        self.assertIn("EXCHANGE TABLES", migration)
        self.assertIn("GROUP BY event_id, vehicle_type_id", migration)


if __name__ == "__main__":
    unittest.main()
