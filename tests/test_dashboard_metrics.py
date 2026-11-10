import unittest

import pandas as pd

from dashboard_metrics import calculate_kpis


class DashboardMetricTests(unittest.TestCase):
    def test_inference_average_is_weighted_by_processed_event(self):
        summary = pd.DataFrame(
            [
                {"camera_id": "A", "vehicle_type_name": "Car", "total_count": 7},
                {"camera_id": "A", "vehicle_type_name": "Pedestrian", "total_count": 4},
                {"camera_id": "B", "vehicle_type_name": "Truck", "total_count": 5},
            ]
        )
        time_series = pd.DataFrame(
            [
                {"processed_frames": 1, "inference_time_sum_ms": 50},
                {"processed_frames": 9, "inference_time_sum_ms": 900},
            ]
        )

        cameras, frames, detections, inference_ms = calculate_kpis(
            summary, time_series
        )

        self.assertEqual(cameras, 2)
        self.assertEqual(frames, 10)
        self.assertEqual(detections, 12)
        self.assertEqual(inference_ms, 95.0)

    def test_empty_time_series_has_zero_frame_and_inference_kpis(self):
        summary = pd.DataFrame(
            columns=["camera_id", "vehicle_type_name", "total_count"]
        )
        time_series = pd.DataFrame(
            columns=["processed_frames", "inference_time_sum_ms"]
        )
        self.assertEqual(calculate_kpis(summary, time_series), (0, 0, 0, 0))


if __name__ == "__main__":
    unittest.main()
