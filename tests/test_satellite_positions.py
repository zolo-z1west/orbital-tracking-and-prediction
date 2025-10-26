# tests/test_satellite_positions.py
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

def main():
    # 1️⃣ Check CSV existence
    csv_path = Path("data/processed/satellite_positions.csv")
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} does not exist.")
    print(f"✅ CSV exists: {csv_path}")

    # 2️⃣ Load CSV
    df = pd.read_csv(csv_path)
    print(f"✅ CSV loaded successfully, {len(df)} rows, {len(df.columns)} columns")
    print(df.head())

    # 3️⃣ Verify required columns
    required_cols = [
        "satellite_id", "satellite_name", "timestamp_utc",
        "temex", "temey", "temez", "velocity_mag_kms",
        "alt_deg", "az_deg", "range_km"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(f"❌ Missing columns: {missing}")
    else:
        print("✅ All key columns present")

    # 4️⃣ Sanity check: positions and velocity
    if df[["temex", "temey", "temez", "velocity_mag_kms"]].isna().all().all():
        print("❌ All position/velocity data is NaN")
    else:
        print("✅ Position/velocity data looks good")

    # 5️⃣ Sanity check: TLE epoch
    if "tle_epoch" in df.columns and df["tle_epoch"].isna().all():
        print("❌ All TLE epochs missing")
    else:
        print("✅ TLE epoch column has data")

    # 6️⃣ Optional: plot trajectory of first satellite
    sample_sat = df["satellite_id"].iloc[0]
    sat_df = df[df["satellite_id"] == sample_sat]
    plt.plot(sat_df["temex"], sat_df["temey"], marker="o", linestyle="-")
    plt.xlabel("X (km)")
    plt.ylabel("Y (km)")
    plt.title(f"Trajectory of {sample_sat}")
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    main()
