# TP 1 – Premier contact avec Spark 4 (corrigé)
# Lancer : python jour1/corriges/tp1_premier_contact.py   (depuis la racine du dépôt)
import time
from pyspark.sql import SparkSession

# %% 1. Une SparkSession en local, tous les cœurs
spark = (SparkSession.builder
         .appName("TP1-premier-contact")
         .master("local[*]")
         .getOrCreate())
print("Spark", spark.version, "— Spark UI :", spark.sparkContext.uiWebUrl)

# %% 2. Charger le fil rouge et compter les lignes
df = spark.read.csv("data/trips.csv", header=True, inferSchema=True)
print("Nombre de courses :", df.count())

# %% 3. Schéma et 5 lignes
df.printSchema()
df.select("tpep_pickup_datetime", "PULocationID", "trip_distance", "total_amount").show(5)

# %% 4. Combien de partitions ? (regarder aussi l'onglet Jobs de la Spark UI)
print("Partitions :", df.rdd.getNumPartitions())

# %% Bonus – CSV ou Parquet : qui lit le plus vite ?
t = time.time(); spark.read.csv("data/trips.csv", header=True, inferSchema=True).count(); t_csv = time.time() - t
t = time.time(); spark.read.parquet("data/trips.parquet").count(); t_pq = time.time() - t
print(f"CSV (avec inférence du schéma) : {t_csv:.2f} s — Parquet : {t_pq:.2f} s")
# Parquet est plus rapide : format colonne, compressé, avec le schéma embarqué (pas d'inférence).

spark.stop()
