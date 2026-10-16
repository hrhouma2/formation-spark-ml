# TP 2 – Top 10 des zones avec les RDD (corrigé)
# Lancer : python jour1/corriges/tp2_rdd.py
import csv
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TP2-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext            # le SparkContext vit dans la SparkSession

# %% 1. Charger trips.csv en RDD de lignes et retirer l'en-tête
lignes = sc.textFile("data/trips.csv")
entete = lignes.first()
courses = lignes.filter(lambda l: l != entete)
print("Lignes :", courses.count())

# %% 2. PairRDD (PULocationID, total_amount) — colonnes 7 et 16 (index à partir de 0)
champs = courses.map(lambda l: l.split(","))
paires = champs.map(lambda c: (int(c[7]), float(c[16])))
print("Exemple de paires :", paires.take(3))

# %% 3. Chiffre d'affaires par zone : reduceByKey (pré-agrège sur chaque partition avant le shuffle)
ca_par_zone = paires.reduceByKey(lambda a, b: a + b)

# %% 4. Top 10 (takeOrdered est une action : elle déclenche le calcul)
top10 = ca_par_zone.takeOrdered(10, key=lambda kv: -kv[1])
print("Top 10 (id de zone, CA) :", [(z, round(v, 2)) for z, v in top10])

# %% 5. Nom des zones : join avec taxi_zone_lookup.csv
#    Le fichier met tous les champs entre guillemets → csv.reader plutôt qu'un split(",")
zl = sc.textFile("data/taxi_zone_lookup.csv")
zl_entete = zl.first()
zones = (zl.filter(lambda l: l != zl_entete)
           .map(lambda l: next(csv.reader([l])))
           .map(lambda c: (int(c[0]), f"{c[2]} ({c[1]})")))
top10_rdd = sc.parallelize(top10)
resultat = top10_rdd.join(zones).sortBy(lambda kv: -kv[1][0])      # (id, (ca, nom))
print("\nTop 10 des zones de départ par chiffre d'affaires :")
for rang, (zid, (ca, nom)) in enumerate(resultat.collect(), 1):
    print(f"{rang:2d}. {nom:45s} {ca:12,.2f} $")

# %% 6. Dans la Spark UI : combien de stages ? Où est le shuffle ?
#    reduceByKey = transformation wide → 1 shuffle → le job a 2 stages.
#    Le join et le sortBy ajoutent d'autres shuffles (donc d'autres stages).

# %% Bonus – groupByKey vs reduceByKey : comparer le volume du SHUFFLE (pas le temps, trop bruité sur un portable)
import json, urllib.request
sc.setJobGroup("reduce", "reduceByKey"); paires.reduceByKey(lambda a, b: a + b).count()
sc.setJobGroup("group", "groupByKey");   paires.groupByKey().mapValues(sum).count()
stages = json.load(urllib.request.urlopen(f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"))
for st in stages:                                  # ce que montre la colonne « Shuffle Write » de l'onglet Stages
    if st["shuffleWriteBytes"] and ("reduceByKey" in st["name"] or "groupByKey" in st["name"]):
        print(f"{st['name'].split(' at ')[0]:12s} shuffle write = {st['shuffleWriteBytes'] / 1024:8.1f} Ko")
# groupByKey envoie TOUTES les valeurs sur le réseau (~480 Ko) ; reduceByKey additionne d'abord sur chaque
# partition et n'envoie qu'un total par zone (~7 Ko) : environ 70 fois moins de données échangées.

spark.stop()
