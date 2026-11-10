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


if __name__ == "__main__":
    unittest.main()
