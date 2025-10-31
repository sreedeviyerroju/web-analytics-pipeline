import duckdb
import sys
import os

def main():
    if len(sys.argv) < 2:
        print("❌ Please provide the processed parquet path as argument")
        sys.exit(1)

    processed_path = sys.argv[1]
    print(f"🚀 Loading {processed_path}")

    con = duckdb.connect()

    # Create temporary view from parquet
    con.execute(f"""
        CREATE OR REPLACE VIEW web_events AS
        SELECT * FROM read_parquet('{processed_path}');
    """)

    # Create dim_users
    con.execute("""
        CREATE TABLE dim_users AS
        SELECT DISTINCT
            user_id,
            COUNT(*) AS total_events,
            COUNT(DISTINCT device) AS unique_devices
        FROM web_events
        GROUP BY user_id;
    """)

    # Create dim_pages
    con.execute("""
        CREATE TABLE dim_pages AS
        SELECT
            page_url,
            COUNT(*) AS total_views,
            COUNT(DISTINCT user_id) AS unique_visitors
        FROM web_events
        GROUP BY page_url;
    """)

    # Create fact_events
    con.execute("""
        CREATE TABLE fact_events AS
        SELECT
            user_id,
            page_url,
            event_type,
            device,
            timestamp,
            revenue
        FROM web_events;
    """)

    # Save outputs
    os.makedirs("data/processed", exist_ok=True)
    con.execute("COPY dim_users TO 'data/processed/dim_users.parquet' (FORMAT 'parquet');")
    con.execute("COPY dim_pages TO 'data/processed/dim_pages.parquet' (FORMAT 'parquet');")
    con.execute("COPY fact_events TO 'data/processed/fact_events.parquet' (FORMAT 'parquet');")

    print("✅ Dimension and fact tables created and saved!")

if __name__ == "__main__":
    main()
