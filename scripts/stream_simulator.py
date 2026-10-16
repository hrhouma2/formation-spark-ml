#!/usr/bin/env python3
"""
Simulateur de flux pour le jour 3 : fait « arriver » des données dans un dossier, fichier par fichier,
comme le ferait une application en production. Spark Structured Streaming lit ce dossier en continu.

    # Terminal 1 : les courses arrivent (1 fichier de 500 courses toutes les 2 secondes)
    python scripts/stream_simulator.py courses --rows 500 --interval 2

    # Terminal 1 bis : les logs de l'application de réservation arrivent
    python scripts/stream_simulator.py logs --lines 400 --interval 2

    # Tout générer d'un coup (corrigés, mode dégradé) :
    python scripts/stream_simulator.py courses --all
    python scripts/stream_simulator.py logs --all

Les fichiers sont écrits dans un dossier temporaire puis DÉPLACÉS (os.replace) : Spark ne voit jamais
un fichier à moitié écrit. C'est la bonne pratique avec la source « fichier ».
"""
import argparse
import json
import os
import random
import shutil
import time
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def atomic_write(dossier, nom, lignes):
    tmp = os.path.join(dossier, "_tmp")
    os.makedirs(tmp, exist_ok=True)
    chemin_tmp = os.path.join(tmp, nom)
    with open(chemin_tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(lignes) + "\n")
    os.replace(chemin_tmp, os.path.join(dossier, nom))


# --------------------------------------------------------------------------- courses
def courses(a):
    import pandas as pd
    df = pd.read_parquet(os.path.join(ROOT, "data", "trips.parquet"))
    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)
    debut = df.tpep_pickup_datetime.searchsorted(pd.Timestamp("2024-01-15 06:00:00"))
    df = df.iloc[debut:debut + a.rows * a.files].copy()
    for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S")
    dossier = os.path.join(ROOT, "stream", "courses")
    os.makedirs(dossier, exist_ok=True)
    rng = random.Random(1)
    lignes_retard = []
    for i in range(a.files):
        bloc = df.iloc[i * a.rows:(i + 1) * a.rows].to_dict("records")
        rng.shuffle(bloc)                                   # l'ordre d'arrivée n'est pas l'ordre des événements
        # 2 % des courses arrivent en retard (réseau, téléphone hors ligne) : elles partent dans un fichier ultérieur
        garde, retard = [], []
        for r in bloc:
            (retard if rng.random() < 0.02 else garde).append(r)
        bloc = garde + lignes_retard
        lignes_retard = retard
        atomic_write(dossier, f"courses-{i:05d}.json", [json.dumps(r) for r in bloc])
        print(f"courses-{i:05d}.json : {len(bloc)} courses (jusqu'à {max(r['tpep_pickup_datetime'] for r in bloc)})")
        if not a.all:
            time.sleep(a.interval)


# --------------------------------------------------------------------------- logs
ENDPOINTS = ["/estimate", "/book", "/cancel", "/payment", "/rating"]


def logs(a):
    dossier = os.path.join(ROOT, "stream", "logs")
    os.makedirs(dossier, exist_ok=True)
    rng = random.Random(7)
    t = datetime(2024, 1, 15, 8, 0, 0)
    for i in range(a.files):
        lignes = []
        for _ in range(a.lines):
            t += timedelta(milliseconds=rng.randint(50, 250))
            minute = (t - datetime(2024, 1, 15, 8, 0, 0)).seconds // 60
            incident = 22 <= minute < 26                    # panne du service de paiement entre 8h22 et 8h26
            ep = rng.choices(ENDPOINTS, weights=[40, 25, 5, 20, 10])[0]
            erreur = rng.random() < (0.35 if incident and ep == "/payment" else 0.01)
            statut = rng.choice([500, 502, 503]) if erreur else rng.choices([200, 201, 404], weights=[90, 8, 2])[0]
            niveau = "ERROR" if statut >= 500 else ("WARN" if statut == 404 else "INFO")
            latence = int(rng.lognormvariate(4.6, 0.5) * (8 if erreur else 1))
            ts = t
            if rng.random() < 0.03:                         # 3 % des lignes arrivent avec du retard
                ts = t - timedelta(minutes=rng.choice([1, 2, 5, 8]))
            lignes.append(f"{ts:%Y-%m-%dT%H:%M:%S}.{ts.microsecond // 1000:03d} {niveau:5s} "
                          f"endpoint={ep} status={statut} latency_ms={latence} zone={rng.randint(1, 263)}")
        atomic_write(dossier, f"app-{i:05d}.log", lignes)
        print(f"app-{i:05d}.log : {len(lignes)} lignes (horloge {t:%H:%M:%S})")
        if not a.all:
            time.sleep(a.interval)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("flux", choices=["courses", "logs"])
    ap.add_argument("--rows", type=int, default=500, help="courses par fichier")
    ap.add_argument("--lines", type=int, default=400, help="lignes de log par fichier")
    ap.add_argument("--files", type=int, default=30, help="nombre de fichiers à produire")
    ap.add_argument("--interval", type=float, default=2.0, help="secondes entre deux fichiers")
    ap.add_argument("--all", action="store_true", help="tout écrire immédiatement")
    ap.add_argument("--reset", action="store_true", help="vider le dossier avant de commencer")
    a = ap.parse_args()
    if a.reset:
        shutil.rmtree(os.path.join(ROOT, "stream", a.flux), ignore_errors=True)
        # les checkpoints des requêtes qui lisaient ce flux ne sont plus valables : on les supprime aussi
        for tp in (["tp9", "tp11"] if a.flux == "courses" else ["tp10"]):
            shutil.rmtree(os.path.join(ROOT, "out", "checkpoints", tp), ignore_errors=True)
        print(f"stream/{a.flux} et les checkpoints associés ont été vidés")
    courses(a) if a.flux == "courses" else logs(a)


if __name__ == "__main__":
    main()
