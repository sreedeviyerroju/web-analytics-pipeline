import pandas as pd
import matplotlib.pyplot as plt
import os

def main():
    fact_path = "data/processed/fact_events.parquet"
    df = pd.read_parquet(fact_path)

    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # --- 1️⃣ Daily Events Trend ---
    df["date"] = df["timestamp"].dt.date
    daily = df.groupby("date").size().reset_index(name="event_count")

    plt.figure(figsize=(10,4))
    plt.plot(daily["date"], daily["event_count"])
    plt.title("📈 Daily Web Event Trend")
    plt.xlabel("Date")
    plt.ylabel("Events")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # --- 2️⃣ Revenue by Device ---
    revenue_by_device = df.groupby("device")["revenue"].sum().reset_index()

    plt.figure(figsize=(6,4))
    plt.bar(revenue_by_device["device"], revenue_by_device["revenue"])
    plt.title("💰 Revenue by Device")
    plt.xlabel("Device")
    plt.ylabel("Total Revenue")
    plt.tight_layout()
    plt.show()

    # --- 3️⃣ Top Pages ---
    top_pages = df["page_url"].value_counts().head(10)
    print("🔝 Top Pages by Views:")
    print(top_pages)

if __name__ == "__main__":
    main()
