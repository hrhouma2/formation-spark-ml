#!/usr/bin/env python3
"""
Génère un jeu de données SYNTHÉTIQUE au format NYC TLC « Yellow Taxi Trip Records »
(mêmes 19 colonnes, mêmes types, mêmes ordres de grandeur) pour les TP de la formation.

    python scripts/generate_data.py --rows 120000 --out data

Produit :
    data/trips.csv              courses au format CSV (en-tête + lignes)
    data/trips.parquet          le même contenu en Parquet (via pandas / pyarrow)
    data/taxi_zone_lookup.csv   référentiel des zones (LocationID, Borough, Zone, service_zone)

Pour utiliser les VRAIES données TLC à la place : scripts/download_official.py
"""
import argparse
import csv
import math
import os
import random
from datetime import datetime, timedelta

# --------------------------------------------------------------------------- zones
# Référentiel de 265 zones. Les identifiants et noms marqués ci-dessous correspondent au
# référentiel officiel TLC pour les zones les plus courantes ; les autres identifiants
# reçoivent un nom générique « Zone <id> » et un arrondissement plausible.
KNOWN = {
    1: ("EWR", "Newark Airport"), 3: ("Bronx", "Allerton/Pelham Gardens"), 4: ("Manhattan", "Alphabet City"),
    7: ("Queens", "Astoria"), 12: ("Manhattan", "Battery Park"), 13: ("Manhattan", "Battery Park City"),
    17: ("Brooklyn", "Bedford"), 18: ("Bronx", "Bedford Park"), 24: ("Manhattan", "Bloomingdale"),
    25: ("Brooklyn", "Boerum Hill"), 33: ("Brooklyn", "Brooklyn Heights"), 36: ("Brooklyn", "Bushwick North"),
    37: ("Brooklyn", "Bushwick South"), 40: ("Brooklyn", "Carroll Gardens"), 41: ("Manhattan", "Central Harlem"),
    42: ("Manhattan", "Central Harlem North"), 43: ("Manhattan", "Central Park"), 45: ("Manhattan", "Chinatown"),
    48: ("Manhattan", "Clinton East"), 49: ("Brooklyn", "Clinton Hill"), 50: ("Manhattan", "Clinton West"),
    61: ("Brooklyn", "Crown Heights North"), 65: ("Brooklyn", "Downtown Brooklyn/MetroTech"),
    68: ("Manhattan", "East Chelsea"), 74: ("Manhattan", "East Harlem North"), 75: ("Manhattan", "East Harlem South"),
    79: ("Manhattan", "East Village"), 82: ("Queens", "Elmhurst"), 87: ("Manhattan", "Financial District North"),
    88: ("Manhattan", "Financial District South"), 90: ("Manhattan", "Flatiron"), 92: ("Queens", "Flushing"),
    97: ("Brooklyn", "Fort Greene"), 100: ("Manhattan", "Garment District"), 107: ("Manhattan", "Gramercy"),
    112: ("Brooklyn", "Greenpoint"), 113: ("Manhattan", "Greenwich Village North"),
    114: ("Manhattan", "Greenwich Village South"), 116: ("Manhattan", "Hamilton Heights"),
    125: ("Manhattan", "Hudson Sq"), 129: ("Queens", "Jackson Heights"), 130: ("Queens", "Jamaica"),
    132: ("Queens", "JFK Airport"), 137: ("Manhattan", "Kips Bay"), 138: ("Queens", "LaGuardia Airport"),
    140: ("Manhattan", "Lenox Hill East"), 141: ("Manhattan", "Lenox Hill West"), 142: ("Manhattan", "Lincoln Square East"),
    143: ("Manhattan", "Lincoln Square West"), 144: ("Manhattan", "Little Italy/NoLiTa"), 145: ("Queens", "Long Island City/Hunters Point"),
    148: ("Manhattan", "Lower East Side"), 151: ("Manhattan", "Manhattan Valley"), 152: ("Manhattan", "Manhattanville"),
    158: ("Manhattan", "Meatpacking/West Village West"), 161: ("Manhattan", "Midtown Center"), 162: ("Manhattan", "Midtown East"),
    163: ("Manhattan", "Midtown North"), 164: ("Manhattan", "Midtown South"), 166: ("Manhattan", "Morningside Heights"),
    170: ("Manhattan", "Murray Hill"), 181: ("Brooklyn", "Park Slope"), 186: ("Manhattan", "Penn Station/Madison Sq West"),
    193: ("Queens", "Queensbridge/Ravenswood"), 209: ("Manhattan", "Seaport"), 211: ("Manhattan", "SoHo"),
    224: ("Manhattan", "Stuy Town/PCV"), 226: ("Queens", "Sunnyside"), 229: ("Manhattan", "Sutton Place/Turtle Bay North"),
    230: ("Manhattan", "Times Sq/Theatre District"), 231: ("Manhattan", "TriBeCa/Civic Center"),
    232: ("Manhattan", "Two Bridges/Seward Park"), 233: ("Manhattan", "UN/Turtle Bay South"), 234: ("Manhattan", "Union Sq"),
    236: ("Manhattan", "Upper East Side North"), 237: ("Manhattan", "Upper East Side South"),
    238: ("Manhattan", "Upper West Side North"), 239: ("Manhattan", "Upper West Side South"),
    243: ("Manhattan", "Washington Heights North"), 244: ("Manhattan", "Washington Heights South"),
    246: ("Manhattan", "West Chelsea/Hudson Yards"), 249: ("Manhattan", "West Village"), 255: ("Brooklyn", "Williamsburg (North Side)"),
    256: ("Brooklyn", "Williamsburg (South Side)"), 260: ("Queens", "Woodside"), 261: ("Manhattan", "World Trade Center"),
    262: ("Manhattan", "Yorkville East"), 263: ("Manhattan", "Yorkville West"), 264: ("Unknown", "NV"), 265: ("N/A", "Outside of NYC"),
}
BOROUGHS = ["Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"]
AIRPORTS = {132, 138}


def zones():
    rows = []
    rng = random.Random(42)
    for i in range(1, 266):
        if i in KNOWN:
            b, z = KNOWN[i]
        else:
            b = rng.choices(BOROUGHS, weights=[25, 25, 25, 15, 10])[0]
            z = f"Zone {i}"
        sz = "Airports" if i in AIRPORTS else ("EWR" if i == 1 else ("Yellow Zone" if b == "Manhattan" else "Boro Zone"))
        if i in (264, 265):
            sz = "N/A"
        rows.append((i, b, z, sz))
    return rows


# --------------------------------------------------------------------------- courses
COLUMNS = ["VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
           "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
           "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
           "congestion_surcharge", "Airport_fee"]

# Poids de prise en charge : Manhattan dominant, aéroports très présents
HOT = {132: 60, 138: 45, 161: 70, 237: 65, 236: 60, 162: 55, 230: 55, 186: 50, 170: 40, 48: 45, 68: 40, 79: 40,
       100: 40, 107: 35, 113: 30, 114: 35, 125: 25, 137: 35, 140: 40, 141: 45, 142: 40, 143: 40, 148: 30, 151: 20,
       158: 25, 163: 45, 164: 45, 166: 15, 209: 15, 211: 30, 224: 20, 229: 30, 231: 35, 232: 15, 233: 30, 234: 40,
       238: 40, 239: 45, 246: 35, 249: 35, 261: 20, 262: 30, 263: 35, 43: 20, 50: 35, 87: 25, 88: 20, 90: 35,
       45: 15, 4: 15, 13: 15, 24: 15, 41: 10, 42: 8, 74: 10, 75: 12, 116: 8, 144: 20, 152: 5, 243: 8, 244: 8,
       7: 12, 145: 15, 226: 8, 255: 12, 256: 8, 33: 10, 65: 12, 97: 8, 181: 8, 112: 8, 25: 6, 49: 6, 40: 5}
HOURS_W = [2, 1, 1, 1, 1, 2, 4, 7, 9, 8, 7, 7, 8, 8, 8, 8, 9, 10, 11, 10, 9, 8, 6, 4]


def gen_trips(n, seed, zone_rows):
    rng = random.Random(seed)
    ids = [z[0] for z in zone_rows if z[0] not in (264, 265)]
    w = [HOT.get(i, 2) for i in ids]
    start = datetime(2024, 1, 1)
    for _ in range(n):
        day = rng.randrange(31)
        hour = rng.choices(range(24), weights=HOURS_W)[0]
        pickup = start + timedelta(days=day, hours=hour, minutes=rng.randrange(60), seconds=rng.randrange(60))
        pu = rng.choices(ids, weights=w)[0]
        do = rng.choices(ids, weights=w)[0]
        airport = pu in AIRPORTS or do in AIRPORTS
        if airport:
            dist = round(max(0.5, rng.gauss(14, 5)), 2)
        else:
            dist = round(min(40.0, max(0.0, rng.lognormvariate(0.6, 0.7))), 2)
        if rng.random() < 0.004:
            dist = 0.0  # courses annulées / erreurs GPS, comme dans les vraies données
        minutes = max(1, int(dist * rng.uniform(2.5, 5.0) + rng.gauss(4, 3)))
        dropoff = pickup + timedelta(minutes=minutes)
        rate = 2 if pu == 132 or do == 132 else 1
        fare = 70.0 if rate == 2 else round(3.0 + 3.5 * dist + 0.7 * minutes * 0.5, 2)
        extra = rng.choice([0.0, 0.0, 1.0, 2.5]) if not (6 <= hour < 20) else rng.choice([0.0, 1.0, 2.5, 2.5])
        mta = 0.5
        improvement = 1.0
        congestion = 2.5 if (pu in KNOWN and KNOWN[pu][0] == "Manhattan") else 0.0
        airport_fee = 1.75 if pu in AIRPORTS else 0.0
        tolls = round(rng.choice([0.0] * 8 + [6.94, 10.17]), 2) if airport else 0.0
        payment = rng.choices([1, 2, 3, 4], weights=[75, 22, 2, 1])[0]
        passengers = rng.choices([1, 2, 3, 4, 5, 6], weights=[70, 15, 6, 4, 3, 2])[0]
        subtotal = fare + extra + mta + improvement + congestion + airport_fee + tolls
        # Pourboire : seuls les pourboires par carte sont enregistrés (comme dans les vraies données TLC).
        # La probabilité dépend de la course (signal apprenable pour le TP de classification du jour 2).
        tip = 0.0
        if payment == 1:
            manhattan = pu in KNOWN and KNOWN[pu][0] == "Manhattan"
            night = hour >= 20 or hour < 5
            score = (-1.2 + 0.18 * min(dist, 20) + 0.9 * night + 1.0 * manhattan
                     - 0.35 * (passengers - 1) + 0.5 * airport + rng.gauss(0, 0.6))
            if rng.random() < 1 / (1 + math.exp(-score)):
                tip = round(subtotal * rng.choice([0.15, 0.18, 0.2, 0.25]), 2)
        total = round(subtotal + tip, 2)
        yield [rng.choice([1, 2, 2]), pickup.strftime("%Y-%m-%d %H:%M:%S"), dropoff.strftime("%Y-%m-%d %H:%M:%S"),
               passengers, dist, rate, rng.choice(["N"] * 99 + ["Y"]), pu, do, payment, fare, extra, mta, tip,
               tolls, improvement, total, congestion, airport_fee]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=120_000)
    ap.add_argument("--seed", type=int, default=2024)
    ap.add_argument("--out", default="data")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)

    zr = zones()
    with open(os.path.join(a.out, "taxi_zone_lookup.csv"), "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f, quoting=csv.QUOTE_ALL)  # tous les champs entre guillemets, comme le fichier officiel
        wr.writerow(["LocationID", "Borough", "Zone", "service_zone"])
        wr.writerows(zr)

    csv_path = os.path.join(a.out, "trips.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        wr = csv.writer(f)
        wr.writerow(COLUMNS)
        wr.writerows(gen_trips(a.rows, a.seed, zr))
    print(f"{csv_path} : {a.rows} courses")

    try:
        import pandas as pd
        df = pd.read_csv(csv_path, parse_dates=["tpep_pickup_datetime", "tpep_dropoff_datetime"])
        # Entiers sur 32 bits (comme les fichiers officiels TLC) : un schéma Spark IntegerType
        # doit correspondre au type physique du Parquet, sinon PARQUET_COLUMN_DATA_TYPE_MISMATCH.
        for c in ["VendorID", "passenger_count", "RatecodeID", "PULocationID", "DOLocationID", "payment_type"]:
            df[c] = df[c].astype("int32")
        pq = os.path.join(a.out, "trips.parquet")
        df.to_parquet(pq, index=False)
        print(f"{pq} : écrit ({os.path.getsize(pq) // 1024} Ko)")
    except ImportError:
        print("pandas / pyarrow absents : trips.parquet non généré (pip install pandas pyarrow)")


if __name__ == "__main__":
    main()
