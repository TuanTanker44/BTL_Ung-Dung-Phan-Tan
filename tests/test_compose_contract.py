import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class ComposeContractTests(unittest.TestCase):
    def test_airflow_services_wait_for_successful_initialization(self):
        compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        for service in ("airflow-webserver", "airflow-scheduler"):
            block = re.search(
                rf"(?ms)^  {re.escape(service)}:\n(.*?)(?=^  \S|\Z)", compose
            ).group(1)
            self.assertIn("airflow-init:", block)
            self.assertIn("condition: service_completed_successfully", block)

        init = compose.split("  airflow-init:", 1)[1]
        init = init.split("\n  airflow-webserver:", 1)[0]
        self.assertIn("condition: service_healthy", init)
        self.assertIn("airflow db migrate", init)
        self.assertIn("airflow users reset-password", init)

    def test_dashboard_has_an_actual_periodic_rerun(self):
        dashboard = (ROOT / "dashboard.py").read_text(encoding="utf-8")
        requirements = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        self.assertIn("streamlit-autorefresh", requirements)
        self.assertIn("st_autorefresh(", dashboard)
        self.assertIn("DASHBOARD_REFRESH_SECONDS", dashboard)
        self.assertIn("st.sidebar.toggle(", dashboard)

    def test_start_scripts_verify_services_and_apply_migrations(self):
        for name in ("start_pipeline.ps1", "start_pipeline.sh"):
            script = (ROOT / name).read_text(encoding="utf-8")
            self.assertIn("verify_services.py infrastructure", script)
            self.assertIn("verify_services.py applications", script)
            self.assertIn("verify_dependencies.py", script)
            self.assertIn("001_add_detection_mode.sql", script)
            self.assertIn("002_make_traffic_facts_idempotent.sql", script)
            self.assertNotIn("pip install -r /opt/spark-jobs/requirements.txt", script)


if __name__ == "__main__":
    unittest.main()
