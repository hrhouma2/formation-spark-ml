#!/usr/bin/env python3
"""
Remplace le jeu synthétique par les VRAIES données ouvertes NYC TLC (Yellow Taxi) pour un mois donné.

    pip install pandas pyarrow requests
    python scripts/download_official.py --year 2024 --month 1 --rows 300000

Télécharge yellow_tripdata_<AAAA>-<MM>.parquet et taxi_zone_lookup.csv depuis le CDN officiel de la TLC
(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), garde `--rows` courses (échantillon),
et écrit data/trips.parquet, data/trips.csv, data/taxi_zone_lookup.csv au même format que le jeu synthétique.
"""
import argparse
import os
import requests
import pandas as pd

CDN = "https://d37ci6vzurychx.cloudfront.net"
COLUMNS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
           "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
           "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
           "congestion_surcharge", "Airport_fee"]


def fetch(url, dest):
    print("téléchargement", url)
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(1 << 20):
                f.write(chunk)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--month", type=int, default=1)
    ap.add_argument("--rows", type=int, default=300_000, help="taille de l'échantillon (0 = tout le mois, ~3 M lignes)")
    ap.add_argument("--out", default="data")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    fetch(f"{CDN}/misc/taxi_zone_lookup.csv", os.path.join(a.out, "taxi_zone_lookup.csv"))
    tmp = os.path.join(a.out, "_yellow_full.parquet")
    fetch(f"{CDN}/trip-data/yellow_tripdata_{a.year}-{a.month:02d}.parquet", tmp)

    df = pd.read_parquet(tmp)
    df = df[[c for c in COLUMNS if c in df.columns]]
    df = df.dropna(subset=["PULocationID", "DOLocationID", "total_amount"])
    if a.rows:
        df = df.sample(n=min(a.rows, len(df)), random_state=42).sort_values("tpep_pickup_datetime")
    for c in ["VendorID", "passenger_count", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"]:
        if c in df.columns:
            df[c] = df[c].fillna(0).astype("int32")
    df.to_parquet(os.path.join(a.out, "trips.parquet"), index=False)
    df.to_csv(os.path.join(a.out, "trips.csv"), index=False, date_format="%Y-%m-%d %H:%M:%S")
    os.remove(tmp)
    print(f"{len(df):,} courses écrites dans {a.out}/ (trips.parquet, trips.csv, taxi_zone_lookup.csv)")


if __name__ == "__main__":
    main()
