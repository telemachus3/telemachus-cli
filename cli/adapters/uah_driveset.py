"""
Adapter: UAH-DriveSet -> Telemachus D0

UAH-DriveSet structure (per trip folder):
  RAW_ACCELEROMETERS.txt  — 10 Hz, space-delimited
    columns: activation_flag, acc_x(G), acc_y(G), acc_z(G),
             acc_x_KF, acc_y_KF, acc_z_KF, roll(deg), pitch(deg), yaw(deg)
  RAW_GPS.txt             — 1 Hz, space-delimited
    columns: speed(m/s), lat, lon, altitude, vert_acc, horiz_acc,
             course, course_var, position_state, lane_dist_state, lane_history

Output: Telemachus D0 Parquet (multi-rate 10Hz IMU / 1Hz GPS)
"""
from __future__ import annotations

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


G = 9.80665  # m/s^2

# UAH column names (space-delimited, no header in files)
ACC_COLS = [
    "activation", "acc_x_g", "acc_y_g", "acc_z_g",
    "acc_x_kf_g", "acc_y_kf_g", "acc_z_kf_g",
    "roll_deg", "pitch_deg", "yaw_deg",
]
GPS_COLS = [
    "speed_mps", "lat", "lon", "altitude_m",
    "vert_accuracy", "horiz_accuracy",
    "course_deg", "course_var",
    "position_state", "lane_dist_state", "lane_history",
]


def _read_uah_accel(path: Path) -> pd.DataFrame:
    """Read RAW_ACCELEROMETERS.txt and convert to SI units."""
    df = pd.read_csv(path, sep=r"\s+", header=None, names=ACC_COLS)
    # Convert Gs to m/s^2
    df["ax_mps2"] = df["acc_x_g"] * G
    df["ay_mps2"] = df["acc_y_g"] * G
    df["az_mps2"] = df["acc_z_g"] * G
    # Keep Kalman-filtered versions as optional enrichment
    df["ax_kf_mps2"] = df["acc_x_kf_g"] * G
    df["ay_kf_mps2"] = df["acc_y_kf_g"] * G
    df["az_kf_mps2"] = df["acc_z_kf_g"] * G
    # Orientation (degrees -> radians)
    df["roll_rad"] = np.radians(df["roll_deg"])
    df["pitch_rad"] = np.radians(df["pitch_deg"])
    df["yaw_rad"] = np.radians(df["yaw_deg"])
    return df


def _read_uah_gps(path: Path) -> pd.DataFrame:
    """Read RAW_GPS.txt."""
    df = pd.read_csv(path, sep=r"\s+", header=None, names=GPS_COLS)
    return df


def convert_trip(trip_dir: Path, output_dir: Path, freq_hz: int = 10) -> Path:
    """Convert a single UAH-DriveSet trip folder to Telemachus D0 Parquet.

    Returns path to the output parquet file.
    """
    accel_file = trip_dir / "RAW_ACCELEROMETERS.txt"
    gps_file = trip_dir / "RAW_GPS.txt"

    if not accel_file.exists():
        raise FileNotFoundError(f"Missing {accel_file}")
    if not gps_file.exists():
        raise FileNotFoundError(f"Missing {gps_file}")

    acc = _read_uah_accel(accel_file)
    gps = _read_uah_gps(gps_file)

    # Generate timestamps: IMU at freq_hz, GPS at 1 Hz
    n_acc = len(acc)
    dt_ns = int(1e9 / freq_hz)
    # Use a synthetic epoch (UAH doesn't provide absolute timestamps)
    t0 = pd.Timestamp("2024-01-01T00:00:00Z")
    acc["ts"] = t0 + pd.to_timedelta(np.arange(n_acc) * dt_ns, unit="ns")

    n_gps = len(gps)
    gps["ts"] = t0 + pd.to_timedelta(np.arange(n_gps) * int(1e9), unit="ns")

    # Build D0 DataFrame at IMU rate with NaN for GPS between ticks
    d0 = pd.DataFrame({
        "ts": acc["ts"],
        "ax_mps2": acc["ax_mps2"].values,
        "ay_mps2": acc["ay_mps2"].values,
        "az_mps2": acc["az_mps2"].values,
    })

    # Merge GPS at 1 Hz (every freq_hz rows)
    gps_at_imu = pd.DataFrame(index=d0.index)
    gps_at_imu["lat"] = np.nan
    gps_at_imu["lon"] = np.nan
    gps_at_imu["speed_mps"] = np.nan
    gps_at_imu["altitude_m"] = np.nan

    # Align GPS rows to IMU timeline
    gps_indices = np.arange(0, min(n_gps * freq_hz, n_acc), freq_hz)
    for col in ["lat", "lon", "speed_mps", "altitude_m"]:
        if col in gps.columns:
            vals = gps[col].values[:len(gps_indices)]
            gps_at_imu.loc[gps_indices[:len(vals)], col] = vals

    d0["lat"] = gps_at_imu["lat"].values
    d0["lon"] = gps_at_imu["lon"].values
    d0["speed_mps"] = gps_at_imu["speed_mps"].values

    # Convert timestamps to int64 nanoseconds (Telemachus convention)
    d0["ts"] = d0["ts"].astype("int64")

    # Extract trip metadata from folder name
    trip_name = trip_dir.name

    # Write Parquet
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{trip_name}_d0.parquet"
    d0.to_parquet(out_path, index=False, engine="pyarrow")

    # Write manifest
    manifest = {
        "format": "Telemachus",
        "version": "0.1",
        "source": {
            "provider": "UAH-DriveSet",
            "trip": trip_name,
            "adapter": "telemachus-cli adapt --source uah-driveset",
        },
        "records": len(d0),
        "sampling_rate_hz": freq_hz,
        "columns": list(d0.columns),
    }
    import json
    manifest_path = output_dir / f"{trip_name}_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))

    return out_path


def adapt(input_dir: str, output_dir: str) -> list[Path]:
    """Convert all UAH-DriveSet trips in input_dir to Telemachus D0.

    Expects input_dir to contain trip subfolders (e.g., D1/20151111123456/).
    """
    input_path = Path(input_dir)
    output_path = Path(output_dir)
    results = []

    # Find trip folders (contain RAW_ACCELEROMETERS.txt)
    trip_dirs = sorted(
        p.parent for p in input_path.rglob("RAW_ACCELEROMETERS.txt")
    )

    if not trip_dirs:
        raise FileNotFoundError(
            f"No UAH-DriveSet trip folders found in {input_dir}. "
            "Expected subfolders containing RAW_ACCELEROMETERS.txt."
        )

    for trip_dir in trip_dirs:
        try:
            out = convert_trip(trip_dir, output_path)
            results.append(out)
            print(f"  -> {out.name} ({pd.read_parquet(out).shape[0]} rows)")
        except Exception as e:
            print(f"  !! {trip_dir.name}: {e}")

    return results
