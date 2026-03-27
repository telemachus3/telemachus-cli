"""
Adapter: PVS (Passive Vehicular Sensors) -> Telemachus D0

PVS structure (CSV files in a folder):
  dataset_gps_mpu_left.csv  — 100 Hz, merged GPS+IMU from dashboard sensor
    Columns: timestamp (Unix), acc_x/y/z_dashboard (m/s2), gyro_x/y/z_dashboard (deg/s),
             mag_x/y/z, temp, timestamp_gps (Unix), latitude, longitude, speed (m/s)
  dataset_labels.csv        — one-hot labels per sample
    paved/unpaved/dirt/cobblestone/asphalt, speed_bump_asphalt/cobblestone,
    good/regular/bad_road_left/right

Output: Telemachus D0 Parquet (100Hz IMU / ~1Hz GPS, downsampled to 10Hz)
"""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd


DEG2RAD = np.pi / 180.0


def adapt(input_dir: str, output_dir: str) -> list[Path]:
    """Convert PVS dataset to Telemachus D0 Parquet."""
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # Load main data (dashboard sensor + GPS)
    data_file = input_path / "dataset_gps_mpu_left.csv"
    if not data_file.exists():
        raise FileNotFoundError(f"Missing {data_file}")

    print("  Loading PVS CSV (~144K rows)...")
    df = pd.read_csv(data_file)

    # Load labels
    labels_file = input_path / "dataset_labels.csv"
    labels = pd.read_csv(labels_file) if labels_file.exists() else None

    # Build D0 at 100Hz from dashboard sensor
    # Use dashboard sensor (most relevant for vehicle-level analysis)
    d0 = pd.DataFrame()

    # Timestamps: Unix epoch -> nanoseconds
    d0["ts"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).astype("int64")

    # Accelerometer (already in m/s2)
    d0["ax_mps2"] = df["acc_x_dashboard"].values
    d0["ay_mps2"] = df["acc_y_dashboard"].values
    d0["az_mps2"] = df["acc_z_dashboard"].values

    # Gyroscope (deg/s -> rad/s)
    d0["gx_rad_s"] = df["gyro_x_dashboard"].values * DEG2RAD
    d0["gy_rad_s"] = df["gyro_y_dashboard"].values * DEG2RAD
    d0["gz_rad_s"] = df["gyro_z_dashboard"].values * DEG2RAD

    # GPS (multi-rate: GPS timestamp differs from IMU timestamp)
    d0["lat"] = df["latitude"].values
    d0["lon"] = df["longitude"].values
    d0["speed_mps"] = df["speed"].values

    # GPS is forward-filled in the raw data (~100 updates per GPS tick).
    # Keep speed as-is (forward-filled is valid for Telemachus D0).
    # Set lat/lon to NaN between GPS ticks to signal multi-rate,
    # but keep speed_mps filled (it's the best available estimate).
    gps_ts = df["timestamp_gps"].values
    gps_changed = np.concatenate([[True], np.diff(gps_ts) != 0])
    d0.loc[~gps_changed, ["lat", "lon"]] = np.nan

    # Keep full 100Hz resolution — high frequency data is valuable for:
    # - Testing algorithms at 100Hz then validating at 10Hz
    # - Better event detection (speed bumps, potholes need high freq)
    # - IMU calibration with more data points
    d0_full = d0
    labels_full = labels

    # Add road surface label (most useful for validation)
    if labels_full is not None:
        surface = []
        for _, row in labels_full.iterrows():
            if row.get("speed_bump_asphalt", 0) == 1:
                surface.append("speed_bump")
            elif row.get("speed_bump_cobblestone", 0) == 1:
                surface.append("speed_bump_cobble")
            elif row.get("cobblestone_road", 0) == 1:
                surface.append("cobblestone")
            elif row.get("dirt_road", 0) == 1:
                surface.append("dirt")
            elif row.get("unpaved_road", 0) == 1:
                surface.append("unpaved")
            elif row.get("asphalt_road", 0) == 1:
                surface.append("asphalt")
            else:
                surface.append("unknown")
        d0_full["road_surface"] = surface

    # Write Parquet
    trip_name = input_path.name.replace(" ", "_")
    out_path = output_path / f"pvs_{trip_name}_d0.parquet"
    d0_full.to_parquet(out_path, index=False, engine="pyarrow")

    # Manifest
    manifest = {
        "format": "Telemachus",
        "version": "0.1",
        "source": {
            "provider": "PVS (Passive Vehicular Sensors)",
            "trip": trip_name,
            "country": "BR",
            "city": "Curitiba",
            "sensor": "dashboard",
            "hz": 100,
            "adapter": "telemachus adapt --source pvs",
        },
        "records": len(d0_full),
        "columns": list(d0_full.columns),
        "has_labels": labels is not None,
    }
    (output_path / f"pvs_{trip_name}_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False)
    )

    print(f"  -> {out_path.name} ({len(d0_full)} rows, {len(d0_full.columns)} cols, 100Hz)")
    if labels_full is not None:
        road_counts = pd.Series(surface).value_counts()
        print(f"  Road surfaces: {road_counts.to_dict()}")

    return [out_path]
