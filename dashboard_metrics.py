"""Pure KPI calculations shared by the Streamlit dashboard and tests."""


VEHICLE_TYPE_NAMES = ["Car", "Motorcycle", "Bus", "Truck"]


def calculate_kpis(traffic_summary, time_series):
    """Return camera, frame, detection and event-weighted inference KPIs.

    Vehicle counts are detection occurrences across frames. They are not unique
    physical vehicles because the pipeline does not assign tracking IDs.
    """
    vehicle_rows = traffic_summary[
        traffic_summary["vehicle_type_name"].isin(VEHICLE_TYPE_NAMES)
    ]
    cameras = int(traffic_summary["camera_id"].nunique())
    frames = int(time_series["processed_frames"].sum()) if not time_series.empty else 0
    detections = int(vehicle_rows["total_count"].sum())
    inference_ms = (
        round(float(time_series["inference_time_sum_ms"].sum()) / frames, 1)
        if frames
        else 0
    )
    return cameras, frames, detections, inference_ms
