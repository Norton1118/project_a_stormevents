import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PARQUET_DIR = os.environ.get("PARQUET_DIR", "./data/parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)


def main():
    # Placeholder small dataset — replace with NOAA StormEvents pipeline
    df = pd.DataFrame(
        {
            "event_id": [1, 2, 3],
            "type": ["Hail", "Tornado", "Flood"],
            "magnitude": [1.25, 2.7, 0.8],
            "lon": [-83.75, -83.7, -83.8],
            "lat": [42.28, 42.3, 42.25],
            "date": ["2023-07-01", "2023-07-02", "2023-07-03"],
        }
    )
    table = pa.Table.from_pandas(df)
    out = os.path.join(PARQUET_DIR, "stormevents_sample.parquet")
    pq.write_table(table, out)
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
