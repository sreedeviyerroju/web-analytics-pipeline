import pandas as pd
import sys
import os

def main():
    if len(sys.argv) < 2:
        print("❌ Please provide the raw data path as argument")
        sys.exit(1)

    raw_path = sys.argv[1]
    print(f"📥 Reading {raw_path}")
    df = pd.read_csv(raw_path)

    print("🧹 Cleaning data...")
    # Rename event_time to timestamp for consistency
    if "event_time" in df.columns:
        df = df.rename(columns={"event_time": "timestamp"})

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    # Example cleaning: remove rows missing user_id or event_type
    df = df.dropna(subset=["user_id", "event_type"])

    # Create output folder
    os.makedirs("data/processed", exist_ok=True)
    output_path = "data/processed/web_events.parquet"
    df.to_parquet(output_path, index=False)
    print(f"✅ Cleaned data saved to {output_path}")

if __name__ == "__main__":
    main()
