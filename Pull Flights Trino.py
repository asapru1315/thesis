import os
from datetime import datetime, timedelta

from pyopensky.trino import Trino

USERNAME = "as1315"

AIRLINES = ("AAL", "ASA", "DAL", "EDV", "FFT", "HAL", "JBU", "NKS", "QXE", "RPA", "SKW", "SWA", "UAL", "JIA", "ENY", "PDT", "ASH", "UCA", "GJS")

DOWNLOADS_DIR = os.path.join(os.path.expanduser("~"), "Downloads")
OUTPUT_DIR = os.path.join(DOWNLOADS_DIR, "opensky_daily_exports")
os.makedirs(OUTPUT_DIR, exist_ok=True)

trino = Trino()

START_DATE = datetime(2025, 10, 6)
END_DATE   = datetime(2025, 10, 28)  # loop runs up to but not including this date

date = START_DATE
while date < END_DATE:
    start_str = date.strftime("%Y-%m-%d")
    stop_str  = (date + timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"Fetching flights for {start_str}...")
    df = trino.flightlist(
        start=start_str,
        stop=stop_str,
        airport="K%"
    )

    if df.empty:
        print(f"No flights for {start_str}")
        date += timedelta(days=1)
        continue

    # Normalize callsign and filter by prefix
    df["callsign"] = df["callsign"].astype(str).str.strip()
    df = df[df["callsign"].str.startswith(AIRLINES, na=False)]

    out_path = os.path.join(OUTPUT_DIR, f"opensky_flights_{start_str}_airlines.csv")
    df.to_csv(out_path, index=False)

    print(f"Saved {len(df):,} rows -> {out_path}")
    date += timedelta(days=1)