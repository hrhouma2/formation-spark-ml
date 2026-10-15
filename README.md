# Formation Spark 4 & Machine Learning – dépôt des travaux pratiques

Support des TP de la formation **Spark 4 & Machine Learning** (3 jours, Ambient IT).
Un seul projet **fil rouge** sur les 3 jours : les courses de taxis jaunes de New York.

> **Jour 1 – Comprendre et utiliser Spark 4** : *quelles zones de New York rapportent le plus, et à quelle heure ?*
> Résolu 3 fois, pour comparer : avec les RDD, sur un cluster + HDFS, puis en Spark SQL.

## Démarrage rapide (participants)

```bash
git clone <URL_DU_DEPOT> formation-spark-ml
cd formation-spark-ml
python -m venv .venv && source .venv/bin/activate      # Windows : .venv\Scripts\activate
pip install -r requirements.txt
python scripts/check_env.py                              # Java 17+, PySpark 4, Jupyter, Git, SparkSession
```

Puis ouvrez `jour1/TP1_premier_contact.ipynb` dans VS Code (extension Jupyter), **depuis la racine du dépôt**.

Prérequis détaillés : [docs/prerequis.md](docs/prerequis.md).

## Contenu

```
data/                      fil rouge : trips.csv (120 000 courses), trips.parquet, taxi_zone_lookup.csv
jour1/
  TP1_premier_contact.ipynb        30 min   SparkSession, chargement, Spark UI
  TP2_rdd_top10_zones.ipynb        30 min   RDD, PairRDD, reduceByKey, join, stages
  TP3_cluster/                     50 min   cluster Standalone (local ou Docker) + HDFS
  TP4_spark_sql.ipynb              45 min   DataFrames, schéma, SQL, partitionBy, explain, ANSI
  corriges/                        corrigés (notebooks + scripts .py exécutables)
quiz/quiz_jour1.md         quiz express de fin de matinée (avec réponses)
formateur/                 checklist du jour 1, mode dégradé, mail de prérequis
scripts/                   generate_data.py, download_official.py, check_env.py, build_notebooks.py
```

## Les données

`data/` contient un jeu **synthétique** au format exact des « Yellow Taxi Trip Records » de la NYC Taxi & Limousine
Commission (19 colonnes, mêmes types, mêmes ordres de grandeur, 1 mois, 120 000 courses) : assez petit pour un
portable et pour Git, assez réaliste pour que les résultats aient du sens (JFK et LaGuardia en tête du chiffre d'affaires).

Les 5 colonnes utilisées au jour 1 :

| Colonne | Signification | Index CSV |
|---|---|---|
| `tpep_pickup_datetime` | date et heure de prise en charge (heure locale, sans fuseau) | 1 |
| `PULocationID` | zone de départ (1 à 265) | 7 |
| `trip_distance` | distance en miles | 4 |
| `total_amount` | montant total payé ($) | 16 |
| `taxi_zone_lookup.csv` | LocationID → Borough, Zone | – |

Pour utiliser les **données réelles** (un mois complet ≈ 3 millions de courses) :
`python scripts/download_official.py --year 2024 --month 1 --rows 300000`

## Versions

Testé avec PySpark **4.2.0**, Java 21, Python 3.13. Fonctionne avec toute version 4.x de PySpark et Java 17+.
Pour le TP 3 en conteneurs, aligner la version de l'image `apache/spark` sur celle de `pip show pyspark`.

## Licence

Contenu pédagogique © Ambient IT. Données synthétiques générées par `scripts/generate_data.py`.
Les noms de zones proviennent du référentiel public de la NYC TLC.
