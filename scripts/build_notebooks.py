#!/usr/bin/env python3
"""
Construit les notebooks du jour 1 :
  - jour1/TPx_*.ipynb          : énoncés (cellules à compléter)
  - jour1/corriges/TPx_*.ipynb : corrigés, générés depuis les scripts .py (cellules séparées par `# %%`)

    python scripts/build_notebooks.py
"""
import re
import nbformat as nbf
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
J1 = ROOT / "jour1"


def md(t):
    return nbf.v4.new_markdown_cell(t.strip())


def code(t):
    return nbf.v4.new_code_cell(t.strip("\n"))


def write(path, cells):
    nb = nbf.v4.new_notebook()
    nb["cells"] = cells
    nb["metadata"] = {"kernelspec": {"name": "python3", "display_name": "Python 3", "language": "python"},
                      "language_info": {"name": "python"}}
    nbf.write(nb, path)
    print("écrit", path.relative_to(ROOT))


# --------------------------------------------------------------- corrigés depuis les .py
def corrige_from_py(py_path, titre):
    src = py_path.read_text(encoding="utf-8")
    head, *parts = re.split(r"^# %% ?", src, flags=re.M)
    cells = [md(f"# {titre} – corrigé"), code(RACINE), code(head)]
    for p in parts:
        first, _, body = p.partition("\n")
        cells.append(md("### " + first.strip()))
        cells.append(code(body))
    return cells


RACINE = '''
# Se placer à la racine du dépôt : les chemins data/... sont relatifs à la racine
import os
while not os.path.isdir("data") and os.path.dirname(os.getcwd()) != os.getcwd():
    os.chdir("..")
print("Répertoire de travail :", os.getcwd())
'''

SESSION = '''
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("TP")
         .master("local[*]")
         .getOrCreate())
sc = spark.sparkContext
print("Spark", spark.version, "—", spark.sparkContext.uiWebUrl)
'''

# --------------------------------------------------------------- TP 1
tp1 = [
    md("""
# TP 1 – Premier contact avec Spark 4 (30 min)

**Fil rouge** : un mois de courses de taxis jaunes de New York (`data/trips.csv`, ~120 000 lignes, 19 colonnes).

**Objectif** : avoir une SparkSession qui fonctionne, charger les données et lire la Spark UI.

> Ouvrez ce notebook depuis la **racine du dépôt** (les chemins sont relatifs à `data/`).
"""),
    code(RACINE),
    md("### 1. Créer une SparkSession en local, avec tous les cœurs (`local[*]`)"),
    code(SESSION),
    md("### 2. Charger `data/trips.csv` (avec en-tête) et compter les lignes\n\nIndice : `spark.read.csv(chemin, header=True, inferSchema=True)` puis `.count()`."),
    code("df = ...\nprint(\"Nombre de courses :\", ...)"),
    md("### 3. Afficher le schéma et 5 lignes\n\nIndice : `printSchema()`, `select(...)`, `show(5)`."),
    code("..."),
    md("### 4. Ouvrir la Spark UI (lien affiché à l'étape 1) : combien de **jobs** ont été lancés ? Combien de **tâches** pour le `count()` ?\n\nIndice : `df.rdd.getNumPartitions()`"),
    code("..."),
    md("### Bonus – CSV ou Parquet ?\n\nChronométrez un `count()` sur `data/trips.csv` puis sur `data/trips.parquet`. Lequel est le plus rapide, et pourquoi ?"),
    code("import time\n..."),
]

# --------------------------------------------------------------- TP 2
tp2 = [
    md("""
# TP 2 – Top 10 des zones avec les RDD (30 min)

**Objectif** : trouver les 10 zones de départ (`PULocationID`) qui rapportent le plus (`total_amount`), avec l'API RDD.

Colonnes de `trips.csv` (index à partir de 0) : `PULocationID` = colonne **7**, `total_amount` = colonne **16**.

Livrable : ce notebook, poussé sur votre branche Git.
"""),
    code(RACINE),
    code(SESSION),
    md("### 1. Charger `data/trips.csv` en RDD de lignes (`sc.textFile`) et retirer l'en-tête\n\nIndice : `first()` renvoie la première ligne ; `filter(lambda l: l != entete)`."),
    code("lignes = ...\nentete = ...\ncourses = ...\nprint(courses.count())"),
    md("### 2. Construire un PairRDD `(PULocationID, total_amount)`\n\nIndice : `map(lambda l: l.split(\",\"))` puis `map(lambda c: (int(c[7]), float(c[16])))`."),
    code("paires = ...\npaires.take(3)"),
    md("### 3. Chiffre d'affaires par zone avec `reduceByKey`"),
    code("ca_par_zone = ..."),
    md("### 4. Garder le top 10 avec `takeOrdered`\n\nIndice : `takeOrdered(10, key=lambda kv: -kv[1])`."),
    code("top10 = ...\ntop10"),
    md("""
### 5. Afficher le nom des zones : `join` avec `data/taxi_zone_lookup.csv`

Le fichier met tous les champs entre guillemets : préférez `csv.reader` à un `split(",")`.

```python
import csv
champs = next(csv.reader([ligne]))   # ['132', 'Queens', 'JFK Airport', 'Airports']
```
Indice : construire un PairRDD `(LocationID, Zone)`, puis `sc.parallelize(top10).join(zones)`.
"""),
    code("import csv\nzones = ...\nresultat = ...\nfor zid, (ca, nom) in resultat.collect():\n    print(nom, round(ca, 2))"),
    md("### 6. Dans la Spark UI : combien de **stages** pour le calcul du top 10 ? Où est le **shuffle** ?\n\nÉcrivez votre réponse ici :"),
    md("_Votre réponse…_"),
    md("### Bonus – `groupByKey` contre `reduceByKey`\n\nRefaites l'étape 3 avec `groupByKey().mapValues(sum)` et comparez les temps. Pourquoi cette différence ?"),
    code("import time\n..."),
]

# --------------------------------------------------------------- TP 4
tp4 = [
    md("""
# TP 4 – Le fil rouge en DataFrames et Spark SQL (45 min)

**Objectif** : refaire le TP 2 avec l'API structurée, aller plus loin (heure, arrondissement), et comparer avec les RDD.

Données : `data/trips.parquet` et `data/taxi_zone_lookup.csv`.
"""),
    code(RACINE),
    code(SESSION),
    md("""
### 1. Lire `trips.parquet` avec un **schéma explicite**

Gardez seulement `tpep_pickup_datetime`, `PULocationID`, `DOLocationID`, `trip_distance`, `total_amount`.

Indices : `StructType([StructField(nom, type), ...])`, `spark.read.schema(schema).parquet(...)`.
Les identifiants sont des entiers 32 bits (`IntegerType`), les montants des `DoubleType`,
l'horodatage est en heure locale sans fuseau (`TimestampNTZType`).
"""),
    code("from pyspark.sql.types import *\nschema = StructType([\n    ...\n])\ntrips = ...\ntrips.printSchema()\nzones = spark.read.csv(\"data/taxi_zone_lookup.csv\", header=True, inferSchema=True)"),
    md("### 2. Créer les vues `trips` et `zones` (`createOrReplaceTempView`)"),
    code("..."),
    md("### 3a. En SQL : le top 10 des zones de départ par chiffre d'affaires, avec le nom de la zone et l'arrondissement\n\nIndice : `JOIN zones z ON t.PULocationID = z.LocationID`, `GROUP BY`, `ORDER BY ... DESC LIMIT 10`."),
    code('top10 = spark.sql("""\n    SELECT ...\n""")\ntop10.show(truncate=False)'),
    md("### 3b. En SQL : montant moyen par heure de la journée\n\nIndice : `HOUR(tpep_pickup_datetime)`."),
    code('spark.sql("""\n    SELECT ...\n""").show(24)'),
    md("### 4. Avec l'API DataFrame : classement des arrondissements (`Borough`) par chiffre d'affaires\n\nIndice : `join`, `groupBy`, `agg(F.sum(...))`, `orderBy(F.desc(...))`."),
    code("from pyspark.sql import functions as F\npar_borough = ...\npar_borough.show()"),
    md("### 5. Écrire les courses en Parquet, **partitionnées par jour**, dans `out/trips_par_jour`\n\nIndice : `withColumn(\"jour\", F.to_date(...))`, `write.partitionBy(\"jour\").parquet(...)`. Combien de dossiers obtenez-vous ?"),
    code("..."),
    md("### Bonus 1 – `explain()`\n\nAffichez le plan physique du top 10. Que fait Catalyst avec la jointure (indice : *Broadcast*) ? Comparez la lisibilité et le temps avec le TP 2."),
    code("top10.explain()"),
    md("### Bonus 2 – ANSI SQL dans Spark 4\n\nCalculez `total_amount / trip_distance`. Que se passe-t-il pour les courses dont la distance vaut 0 ? Corrigez avec `try_divide`."),
    code("..."),
]

write(J1 / "TP1_premier_contact.ipynb", tp1)
write(J1 / "TP2_rdd_top10_zones.ipynb", tp2)
write(J1 / "TP4_spark_sql.ipynb", tp4)
write(J1 / "corriges" / "TP1_premier_contact.ipynb", corrige_from_py(J1 / "corriges" / "tp1_premier_contact.py", "TP 1 – Premier contact"))
write(J1 / "corriges" / "TP2_rdd_top10_zones.ipynb", corrige_from_py(J1 / "corriges" / "tp2_rdd.py", "TP 2 – Top 10 des zones avec les RDD"))
write(J1 / "corriges" / "TP4_spark_sql.ipynb", corrige_from_py(J1 / "corriges" / "tp4_sql.py", "TP 4 – DataFrames et Spark SQL"))
