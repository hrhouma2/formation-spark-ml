# Données du fil rouge

| Fichier | Contenu |
|---|---|
| `trips.csv` | 120 000 courses, 19 colonnes au format NYC TLC « Yellow Taxi » (en-tête + lignes, séparateur `,`) |
| `trips.parquet` | le même contenu en Parquet (entiers 32 bits, horodatages sans fuseau) |
| `taxi_zone_lookup.csv` | 265 zones : `LocationID, Borough, Zone, service_zone` – tous les champs entre guillemets |

Jeu **synthétique** généré par `scripts/generate_data.py` (reproductible, graine 2024).
Les identifiants et noms des zones principales (aéroports, Manhattan) correspondent au référentiel officiel ;
les autres zones portent un nom générique `Zone <id>`.

Données réelles : `python scripts/download_official.py` (source : NYC Taxi & Limousine Commission, données ouvertes).
