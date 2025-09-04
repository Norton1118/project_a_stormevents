# etl/noaa_etl.py
import re
import gzip
import shutil
import requests
import pandas as pd
from pathlib import Path

BASE_URL = "https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles"
YEARS = [2023]  # add more e.g. [2021, 2022, 2023, 2024]

OUT_DIR = Path("data/parquet/stormevents")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def latest_filename_for_year(year: int) -> str:
    """Fetch directory listing and return the latest details CSV.gz for the year."""
    r = requests.get(BASE_URL, timeout=60)
    r.raise_for_status()
    # Example: StormEvents_details-ftp_v1.0_d2023_c20250110.csv.gz
    pattern = re.compile(rf"StormEvents_details-ftp_v1\.0_d{year}_c\d{{8}}\.csv\.gz")
    matches = pattern.findall(r.text)
    if not matches:
        raise FileNotFoundError(f"No NOAA details file found for {year} at {BASE_URL}")
    # pick the max cYYYYMMDD
    return sorted(set(matches))[-1]


def dl(url: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)


def normalize_and_write(csv_path: Path, year: int):
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    # Many NOAA files use these names (BEGIN_* for start of event)
    def pick(*cands):
        for c in cands:
            if c in df.columns:
                return c
        return None

    c_event_id = pick("EVENT_ID", "event_id")
    c_type = pick("EVENT_TYPE", "event_type")
    c_mag = pick("MAGNITUDE", "magnitude")
    c_lat = pick("BEGIN_LAT", "BEGIN_LATITUDE", "LATITUDE", "lat")
    c_lon = pick("BEGIN_LON", "BEGIN_LONGITUDE", "LONGITUDE", "lon")
    c_date = pick("BEGIN_DATE_TIME", "BEGIN_DATE", "BEGIN_YEARMONTH", "date")

    if not all([c_event_id, c_type, c_lat, c_lon, c_date]):
        raise ValueError(
            "Missing required columns in NOAA CSV; got columns: "
            f"{', '.join(df.columns[:20])} ..."
        )

    out = pd.DataFrame(
        {
            "event_id": pd.to_numeric(df[c_event_id], errors="coerce"),
            "type": df[c_type].astype(str),
            "magnitude": (
                pd.to_numeric(df[c_mag], errors="coerce")
                if c_mag
                else pd.Series([None] * len(df))
            ),
            "lat": pd.to_numeric(df[c_lat], errors="coerce"),
            "lon": pd.to_numeric(df[c_lon], errors="coerce"),
            "date": pd.to_datetime(df[c_date], errors="coerce").dt.date,
        }
    ).dropna(subset=["event_id", "lat", "lon", "date"])

    out_path = OUT_DIR / f"stormevents_{year}.parquet"
    out.to_parquet(out_path, index=False)
    print(f"Wrote {out_path} rows={len(out)}")


def process_year(year: int):
    fname = latest_filename_for_year(year)
    url = f"{BASE_URL}/{fname}"
    gz_path = OUT_DIR / f"{year}.csv.gz"
    csv_path = OUT_DIR / f"{year}.csv"

    if not gz_path.exists():
        print(f"Downloading {url}")
        dl(url, gz_path)
    else:
        print(f"Using cached {gz_path}")

    if not csv_path.exists():
        print(f"Unzipping {gz_path}")
        with gzip.open(gz_path, "rb") as g, open(csv_path, "wb") as o:
            shutil.copyfileobj(g, o)

    normalize_and_write(csv_path, year)


if __name__ == "__main__":
    for y in YEARS:
        process_year(y)
