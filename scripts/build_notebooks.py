#!/usr/bin/env python3
"""
Construit les notebooks du jour 1 :
  - jour1/TPx_*.ipynb, jour2/TPx_*.ipynb          : énoncés (cellules à compléter)
  - jour1/corriges/*.ipynb, jour2/corriges/*.ipynb : corrigés, générés depuis les scripts .py (cellules séparées par `# %%`)

    python scripts/build_notebooks.py
"""
import re
import nbformat as nbf
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
J1 = ROOT / "jour1"
J2 = ROOT / "jour2"


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


# =============================================================== JOUR 2
SESSION_ML = '''
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
spark = SparkSession.builder.appName("TP").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
'''

tp5 = [
    md("""
# TP 5 – Explorer avant de prédire (35 min)

**Objectif** : trouver 3 faits dans les données qui guideront nos modèles de l'après-midi.

**Règle d'or** : Spark ne dessine pas. On **agrège** (`groupBy`) ou on **échantillonne** (`sample`) dans Spark,
puis `toPandas()` sur un petit résultat, puis Matplotlib / Seaborn.

**Réussi si** : 4 graphiques affichés et 3 observations écrites.
"""),
    code(RACINE),
    code(SESSION_ML + '''
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_theme(style="whitegrid")
trips = (spark.read.parquet("data/trips.parquet")
         .withColumn("heure", F.hour("tpep_pickup_datetime"))
         .withColumn("jour_semaine", F.dayofweek("tpep_pickup_datetime"))
         .join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID"))
trips.cache().count()'''),
    md("### 1. Distribution des montants : histogramme\n\nIndice : créer une colonne `tranche` = `floor(total_amount / 5) * 5`, puis `groupBy(\"tranche\").count()`, puis `sns.barplot`."),
    code("tranches = ...\n# sns.barplot(data=tranches, x=\"tranche\", y=\"count\")"),
    md("### 2. Montant selon la distance : nuage de points sur un **échantillon**\n\nIndice : `trips.sample(fraction=0.03, seed=1)`, puis `plt.scatter`. Limiter les axes : 0–30 miles, 0–120 $."),
    code("ech = ...\n# plt.scatter(ech.trip_distance, ech.fare_amount, s=5, alpha=0.3)"),
    md("### 3. Courses par jour de semaine et par heure : heatmap\n\nIndice : `groupBy(\"jour_semaine\", \"heure\").count()`, `toPandas().pivot(index=..., columns=..., values=...)`, `sns.heatmap`."),
    code("..."),
    md("### 4. Montant moyen par arrondissement de départ : barres"),
    code("..."),
    md("### 5. Vos 3 observations utiles pour **prédire le montant d'une course**\n\nRegardez bien le nuage de points : que signifie la ligne horizontale ?"),
    md("1. …\n2. …\n3. …"),
    md("### Bonus – le même nuage de points en Plotly (interactif)\n\n`pip install plotly` puis `plotly.express.scatter(ech, x=..., y=...)`."),
    code("..."),
]

tp6 = [
    md("""
# TP 6 – Combien va coûter cette course ? (30 min)

**Objectif** : prédire `fare_amount` à partir de la distance, de l'heure, du nombre de passagers et de l'arrondissement
de départ. Comparer une régression linéaire et un arbre de régression.

**Réussi si** : RMSE de l'arbre < 5 $ sur le test.
"""),
    code(RACINE),
    code(SESSION_ML + '''
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor
from pyspark.ml.evaluation import RegressionEvaluator'''),
    md("### 1. Les données : ajouter `heure` et `Borough` (jointure avec les zones), garder les courses de distance > 0\n\nColonnes à garder : `trip_distance`, `heure`, `passenger_count`, `Borough`, `fare_amount`."),
    code("df = (spark.read.parquet(\"data/trips.parquet\")\n      .filter(...)\n      .withColumn(\"heure\", ...)\n      .join(...)\n      .select(...))"),
    md("### 2. Train / test : 80 / 20, `seed=42`"),
    code("train, test = ...\nprint(train.count(), test.count())"),
    md("### 3. La préparation : `StringIndexer` (Borough → index, `handleInvalid=\"keep\"`) → `OneHotEncoder` → `VectorAssembler`"),
    code("preparation = [\n    ...\n]"),
    md("### 4. Un pipeline par modèle : `LinearRegression`, puis `DecisionTreeRegressor(maxDepth=6)`\n\nIndice : `Pipeline(stages=preparation + [modele]).fit(train)`, puis `.transform(test)`."),
    code("..."),
    md("### 5. Comparer RMSE et R² sur le test\n\nIndice : `RegressionEvaluator(labelCol=\"fare_amount\", metricName=\"rmse\")`. Lequel est le meilleur ? Pourquoi ? (indice : 70 $)"),
    code("..."),
    md("### 6. Sauvegarder le meilleur pipeline, puis le recharger\n\nIndice : `model.write().overwrite().save(\"out/modele_montant\")`, `PipelineModel.load(...)`."),
    code("from pyspark.ml import PipelineModel\n..."),
    md("### Bonus 1 – `GBTRegressor` fait-il mieux ?"),
    code("..."),
    md("### Bonus 2 – « Tricher » : ajoutez `total_amount` aux features. Quel R² ? Pourquoi ce modèle est-il inutilisable ?"),
    code("..."),
]

tp7 = [
    md("""
# TP 7 – Ce client laissera-t-il un pourboire ? (50 min)

**Objectif** : classer les courses payées **par carte** (les pourboires en espèces ne sont pas enregistrés) :
pourboire ou pas. Comparer Random Forest et SVM linéaire, tracer la courbe ROC.

**Réussi si** : AUC > 0,65 et courbe ROC tracée.
"""),
    code(RACINE),
    code(SESSION_ML + '''
from pyspark.ml.classification import RandomForestClassifier, LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
import matplotlib.pyplot as plt'''),
    md("""
### 1. Les données

- garder `trip_distance > 0` **et** `payment_type = 1` (carte)
- `label` = 1.0 si `tip_amount > 0`, sinon 0.0 (`.cast("double")`)
- features : `trip_distance`, `heure`, `passenger_count`, `manhattan` (Borough == "Manhattan"), `nuit` (heure ≥ 20 ou < 5)

Quel est le taux de pourboire ? Les classes sont-elles équilibrées ?
"""),
    code("df = ...\nprint(df.agg(F.avg(\"label\")).first()[0])\ntrain, test = df.randomSplit([0.8, 0.2], seed=7)"),
    md("### 2. Pipeline : `VectorAssembler` → `StandardScaler` → `RandomForestClassifier(numTrees=50, maxDepth=6)`, puis la même chose avec `LinearSVC`"),
    code("..."),
    md("### 3. AUC des deux modèles\n\nIndice : `BinaryClassificationEvaluator(metricName=\"areaUnderROC\")`."),
    code("..."),
    md("### 4. Matrice de confusion (Random Forest, seuil 0,5)\n\nIndice : `pred.groupBy(\"label\").pivot(\"prediction\").count()`. Calculez précision et rappel à la main."),
    code("..."),
    md("""
### 5. Courbe ROC en Matplotlib

Récupérer `label` et la probabilité de la classe 1 : `vector_to_array("probability")[1]`, puis `toPandas()` (le test est petit).
Pour 20 seuils entre 0 et 1, calculer le taux de vrais positifs et le taux de faux positifs, puis `plt.plot(fpr, tpr)`.
"""),
    code("scores = ...\n..."),
    md("### 6. Sauvegarder le meilleur pipeline"),
    code("..."),
    md("### Bonus – `CrossValidator` + `ParamGridBuilder` sur `numTrees` et `maxDepth` (3 plis). C'est plus long !"),
    code("from pyspark.ml.tuning import CrossValidator, ParamGridBuilder\n..."),
    md("### Bonus 2 – Les vraies données\n\n`python scripts/download_official.py` puis relancez ce notebook. Les résultats sont-ils aussi nets ?"),
]

tp8 = [
    md("""
# TP 8 – Les familles de courses (45 min)

**Objectif** : regrouper les courses en familles **sans étiquette**, avec K-means, à partir de la distance, du montant et de l'heure.

**Réussi si** : k justifié par la silhouette et 3 familles nommées avec des mots métier.
"""),
    code(RACINE),
    code(SESSION_ML + '''
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator'''),
    md("### 1. `VectorAssembler` (distance, montant, heure) puis `StandardScaler(withMean=True, withStd=True)`"),
    code("df = ...\nassembleur = ...\nscaler = ..."),
    md("### 2. `KMeans(k=k, seed=1)` pour k = 2 à 6 ; silhouette de chaque k\n\nIndice : `ClusteringEvaluator(featuresCol=\"features\", metricName=\"silhouette\")`."),
    code("for k in range(2, 7):\n    ..."),
    md("### 3. Choisir k, décrire chaque famille\n\nIndice : `groupBy(\"prediction\").agg(count, avg distance, avg montant, avg heure)`."),
    code("..."),
    md("### 4. Nommer les familles avec des mots métier"),
    md("- Famille 0 : …\n- Famille 1 : …\n- Famille 2 : …"),
    md("### 5. Sans `StandardScaler` : que change-t-il ? Pourquoi ?\n\nIndice : regardez l'heure moyenne de chaque centre (`model.clusterCenters()`)."),
    code("..."),
    md("### Bonus – GraphFrames : les zones comme un graphe\n\nSommets = zones (`id`), arêtes = courses (`src` = PULocationID, `dst` = DOLocationID). Top 5 PageRank. Si le package manque : degrés avec `groupBy(\"dst\").count()`."),
    code("# from graphframes import GraphFrame\n..."),
]

write(J2 / "TP5_explorer.ipynb", tp5)
write(J2 / "TP6_regression.ipynb", tp6)
write(J2 / "TP7_classification.ipynb", tp7)
write(J2 / "TP8_clustering.ipynb", tp8)
write(J2 / "corriges" / "TP5_explorer.ipynb", corrige_from_py(J2 / "corriges" / "tp5_explorer.py", "TP 5 – Explorer avant de prédire"))
write(J2 / "corriges" / "TP6_regression.ipynb", corrige_from_py(J2 / "corriges" / "tp6_regression.py", "TP 6 – Prédire le montant"))
write(J2 / "corriges" / "TP7_classification.ipynb", corrige_from_py(J2 / "corriges" / "tp7_classification.py", "TP 7 – Prédire le pourboire"))
write(J2 / "corriges" / "TP8_clustering.ipynb", corrige_from_py(J2 / "corriges" / "tp8_clustering.py", "TP 8 – Les familles de courses"))


# =============================================================== JOUR 3
J3 = ROOT / "jour3"
SESSION_STREAM = '''
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName("TP").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")     # petit volume : 200 partitions par défaut, c'est trop
'''
LANCEMENT = """
**Avant de commencer**, dans un terminal à la racine du dépôt :

```bash
python scripts/stream_simulator.py {flux} --reset          # en direct : un fichier toutes les 2 secondes
# ou, si vous êtes en retard / en mode dégradé :
python scripts/stream_simulator.py {flux} --all --reset    # tout d'un coup
```
"""

tp9 = [
    md("# TP 9 – Votre premier stream (35 min)\n\n**Objectif** : suivre en direct le chiffre d'affaires et le nombre de courses par arrondissement.\n\n**Réussi si** : le tableau se met à jour à chaque micro-batch.\n" + LANCEMENT.format(flux="courses")),
    code(RACINE),
    code(SESSION_STREAM),
    md("### 1. Lire le dossier `stream/courses` en continu\n\nUn stream exige un **schéma explicite** : prenez celui de `data/trips.parquet` (`spark.read.parquet(...).schema`).\nIndices : `spark.readStream.schema(schema).option(\"maxFilesPerTrigger\", 2).json(\"stream/courses\")`, puis `isStreaming`."),
    code("schema = ...\ncourses = ...\nprint(courses.isStreaming)"),
    md("### 2. Jointure stream-statique avec les zones\n\nLe référentiel `data/taxi_zone_lookup.csv` est un DataFrame **statique** ordinaire. Ajoutez la colonne `Borough`."),
    code("zones = ...\nenrichies = ..."),
    md("### 3. Chiffre d'affaires et nombre de courses par `Borough`"),
    code("par_borough = ..."),
    md("### 4. Démarrer la requête : `outputMode(\"complete\")`, `format(\"console\")`, `trigger(processingTime=\"5 seconds\")`, un `checkpointLocation`\n\nLaissez tourner 60 secondes : `q.awaitTermination(60)`, puis `q.stop()`. Le résultat s'affiche dans le terminal de Jupyter / VS Code."),
    code("q = (par_borough.writeStream\n     ...\n     .start())\nq.awaitTermination(60)\nq.stop()"),
    md("### 5. Le suivi : `q.lastProgress`, `q.recentProgress`\n\nCombien de micro-batches ? Combien de lignes par micro-batch (`numInputRows`) ?"),
    code("..."),
    md("### Bonus – Le même calcul en **batch** (`spark.read`) sur les mêmes fichiers : obtenez-vous les mêmes comptes ?"),
    code("..."),
]

tp10 = [
    md("# TP 10 – Les logs en temps réel (35 min)\n\nL'application de réservation écrit ses logs dans `stream/logs/`. Une panne survient quelque part dans la matinée.\n\n**Objectif** : la détecter en temps réel.\n\n**Réussi si** : une alerte (taux d'erreur > 10 %) apparaît sur le bon endpoint, aux bonnes minutes.\n" + LANCEMENT.format(flux="logs")),
    code(RACINE),
    code(SESSION_STREAM),
    md("### 1. Lire les logs en continu (`readStream.text`) : une colonne `value` par ligne\n\nExemple de ligne :\n```\n2024-01-15T08:22:03.120 ERROR endpoint=/payment status=503 latency_ms=812 zone=161\n```"),
    code("brut = ..."),
    md("### 2. Parser avec `regexp_extract`\n\nMotif fourni :\n```python\nmotif = r\"^(\\S+) +(\\w+) +endpoint=(\\S+) status=(\\d+) latency_ms=(\\d+) zone=(\\d+)$\"\n```\nColonnes : `ts` (`to_timestamp(..., \"yyyy-MM-dd'T'HH:mm:ss.SSS\")`), `niveau`, `endpoint`, `status` (int), `latence_ms` (int)."),
    code("motif = r\"^(\\S+) +(\\w+) +endpoint=(\\S+) status=(\\d+) latency_ms=(\\d+) zone=(\\d+)$\"\nlogs = brut.select(\n    ...\n)"),
    md("### 3. Watermark de 2 minutes, fenêtre de 1 minute par `endpoint`\n\nCalculer : `requetes` (count), `erreurs` (status >= 500), `taux_erreur`, `latence_p95` (`percentile_approx(latence_ms, 0.95)`)."),
    code("par_minute = (logs\n    .withWatermark(...)\n    .groupBy(...)\n    .agg(...))"),
    md("### 4. Écrire en mode **append** vers Parquet (`out/tp10/par_minute`), avec un checkpoint\n\nEn direct : `trigger(processingTime=\"5 seconds\")` et `awaitTermination(120)`. Avec `--all` : `trigger(availableNow=True)`."),
    code("q = ..."),
    md("### 5. Lire le Parquet (en batch) et afficher les alertes : taux d'erreur > 10 %\n\nQuel endpoint ? À quelles minutes ? Que devient la latence p95 ?"),
    code("..."),
    md("### 6. Combien de lignes ont été ignorées par le watermark ?\n\nIndice : `q.recentProgress`, champ `stateOperators[0][\"numRowsDroppedByWatermark\"]`.\n\nPourquoi les toutes dernières minutes n'apparaissent-elles pas dans le Parquet ?"),
    code("..."),
    md("### Bonus – Fenêtre glissante de 5 minutes, toutes les minutes : l'alerte est-elle plus stable ?"),
    code("..."),
]

tp11 = [
    md("# TP 11 – Le modèle de pourboire en temps réel, puis sous le capot (55 min)\n\n**Objectif** : appliquer sur le flux de courses le modèle entraîné au TP 7, puis lire et comparer des plans d'exécution.\n\n**Réussi si** : des prédictions sont produites en continu et vous savez expliquer le plan.\n" + LANCEMENT.format(flux="courses")),
    code(RACINE),
    code(SESSION_STREAM + '''
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
import time
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True).select(
    F.col("LocationID").alias("PULocationID"), "Borough")'''),
    md("### 1. La préparation des features, dans UNE fonction (la même qu'au TP 7)\n\nCarte uniquement, `heure`, `manhattan`, `nuit`, `label` (pourboire > 0)."),
    code("def features(df):\n    return (df.filter(\"trip_distance > 0 AND payment_type = 1\")\n              ...)"),
    md("### 2. Recharger le modèle du TP 7 : `PipelineModel.load(\"out/modele_pourboire\")`\n\nS'il n'existe pas, lancez d'abord `python jour2/corriges/tp7_classification.py`."),
    code("modele = ..."),
    md("### 3. Appliquer le modèle sur le flux\n\n`modele.transform(features(flux))`, puis `p_pourboire` = `vector_to_array(\"probability\")[1]`."),
    code("schema = spark.read.parquet(\"data/trips.parquet\").schema\nflux = ...\npredictions = ..."),
    md("### 4. Taux de pourboire **prédit** (moyenne de `p_pourboire`) vs **réel** (moyenne de `label`), par fenêtre de 30 min et par `Borough`\n\nWatermark de 10 minutes sur `ts` = `tpep_pickup_datetime` converti en timestamp. Pourquoi la moyenne des probabilités et pas des prédictions 0/1 ?"),
    code("suivi = ..."),
    md("### 5. `foreachBatch` : pour chaque micro-batch, écrire en Parquet (`out/tp11/suivi`) et afficher le nombre de fenêtres"),
    code("def traiter(batch_df, batch_id):\n    ...\n\nq = ..."),
    md("### 6. Sous le capot : `explain(\"formatted\")` sur la même requête, en CSV puis en Parquet\n\n```python\nrequete = lambda df: df.filter(\"trip_distance > 10\").groupBy(\"PULocationID\").agg(F.avg(\"total_amount\"))\n```\nComparez `PushedFilters`, `ReadSchema`, `Batched`. Mesurez le temps de chaque version (3 exécutions)."),
    code("..."),
    md("### 7. AQE : exécutez la requête Parquet, puis `explain()`. Trouvez `isFinalPlan=true` et `AQEShuffleRead`. Combien de blocs `*(n)` (whole-stage codegen) ?"),
    code("..."),
    md("### Bonus – Spark UI, onglet **Structured Streaming** : débit, durée des micro-batches, lignes d'état"),
]

write(J3 / "TP9_premier_stream.ipynb", tp9)
write(J3 / "TP10_logs_temps_reel.ipynb", tp10)
write(J3 / "TP11_ml_streaming_catalyst.ipynb", tp11)
write(J3 / "corriges" / "TP9_premier_stream.ipynb", corrige_from_py(J3 / "corriges" / "tp9_premier_stream.py", "TP 9 – Premier stream"))
write(J3 / "corriges" / "TP10_logs_temps_reel.ipynb", corrige_from_py(J3 / "corriges" / "tp10_logs_temps_reel.py", "TP 10 – Logs en temps réel"))
write(J3 / "corriges" / "TP11_ml_streaming_catalyst.ipynb", corrige_from_py(J3 / "corriges" / "tp11_ml_streaming_catalyst.py", "TP 11 – ML en temps réel et plans"))
