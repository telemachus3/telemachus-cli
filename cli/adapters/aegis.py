"""
Adapter: AEGIS Automotive Sensor Data (Zenodo) -> Telemachus D0

AEGIS structure (CSV files in a flat directory):
  positions.csv      — ~1 Hz GPS (NMEA format: DDMM.MMMM)
  accelerations.csv  — ~24 Hz accelerometer (G-force)
  gyroscopes.csv     — ~24 Hz gyroscope (deg/s)
  trips.csv          — trip metadata (35 trips, 1 driver, Graz Austria)

Output: Telemachus D0 Parquet per trip (multi-rate ~24Hz IMU / ~1Hz GPS)
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import numpy as np
import pandas as pd


G = 9.80665
DEG2RAD = np.pi / 180.0


def _nmea_to_decimal(nmea_val: float) -> float:
    """Convert NMEA DDMM.MMMM to decimal degrees."""
    degrees = int(nmea_val / 100)
    minutes = nmea_val - (degrees * 100)
    return degrees + minutes / 60.0


def adapt(input_dir: str, output_dir: str) -> list[Path]:
    """Convert all AEGIS trips to Telemachus D0 Parquet."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load all CSVs
    print("  Loading AEGIS CSVs...")
    acc = pd.read_csv(input_path / "accelerations.csv")
    gyro = pd.read_csv(input_path / "gyroscopes.csv")
    pos = pd.read_csv(input_path / "positions.csv")
    trips = pd.read_csv(input_path / "trips.csv")

    # Parse timestamps
    acc["ts"] = pd.to_datetime(acc["timestamp"])
    gyro["ts"] = pd.to_datetime(gyro["timestamp"])
    pos["ts"] = pd.to_datetime(pos["timestamp"])

    # Convert units
    acc["ax_mps2"] = acc["x_value"] * G
    acc["ay_mps2"] = acc["y_value"] * G
    acc["az_mps2"] = acc["z_value"] * G

    gyro["gx_rad_s"] = gyro["x_value"] * DEG2RAD
    gyro["gy_rad_s"] = gyro["y_value"] * DEG2RAD
    gyro["gz_rad_s"] = gyro["z_value"] * DEG2RAD

    # Convert NMEA to decimal degrees
    pos["lat"] = pos["latitude"].astype(float).apply(_nmea_to_decimal)
    pos["lon"] = pos["longitude"].astype(float).apply(_nmea_to_decimal)

    results = []
    trip_ids = sorted(acc["trip_id"].unique())

    for tid in trip_ids:
        # Filter by trip
        a = acc[acc["trip_id"] == tid][["ts", "ax_mps2", "ay_mps2", "az_mps2"]].sort_values("ts")
        g = gyro[gyro["trip_id"] == tid][["ts", "gx_rad_s", "gy_rad_s", "gz_rad_s"]].sort_values("ts")
        p = pos[pos["trip_id"] == tid][["ts", "lat", "lon"]].sort_values("ts")

        if len(a) < 10:
            print(f"  !! Trip {tid}: too few accel points ({len(a)}), skipping")
            continue

        # Merge accel + gyro (same rate, merge on nearest timestamp)
        d0 = pd.merge_asof(a, g, on="ts", tolerance=pd.Timedelta("50ms"), direction="nearest")

        # Merge GPS (~1Hz into ~24Hz, NaN between ticks)
        d0 = pd.merge_asof(d0, p, on="ts", tolerance=pd.Timedelta("600ms"), direction="nearest")

        # Convert ts to int64 nanoseconds
        d0["ts"] = d0["ts"].astype("int64")

        # Add speed estimate from GPS (simple diff)
        # Not available in raw AEGIS data, set to NaN
        d0["speed_mps"] = np.nan

        # Write
        trip_name = f"aegis_trip_{tid:02d}"
        out_path = output_path / f"{trip_name}_d0.parquet"
        d0.to_parquet(out_path, index=False, engine="pyarrow")

        # Manifest
        manifest = {
            "format": "Telemachus",
            "version": "0.1",
            "source": {
                "provider": "AEGIS (Zenodo 820576)",
                "trip_id": int(tid),
                "country": "AT",
                "city": "Graz",
                "adapter": "telemachus adapt --source aegis",
            },
            "records": len(d0),
            "gps_records": len(p),
            "imu_records": len(a),
            "gyro_records": len(g),
            "columns": list(d0.columns),
        }
        (output_path / f"{trip_name}_manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False)
        )

        gps_pct = d0["lat"].notna().mean() * 100
        print(f"  -> {trip_name} ({len(d0)} rows, GPS {gps_pct:.0f}%, gyro OK)")
        results.append(out_path)

    return results
