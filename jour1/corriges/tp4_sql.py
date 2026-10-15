# TP 4 – Le fil rouge en DataFrames et Spark SQL (corrigé)
# Lancer : python jour1/corriges/tp4_sql.py
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampNTZType

spark = SparkSession.builder.appName("TP4-sql").master("local[*]").getOrCreate()

# %% 1. Lire trips.parquet avec un schéma explicite (on ne garde que les colonnes utiles)
#    Attention : les types doivent correspondre au type physique du fichier (ici INT32 → IntegerType).
#    Un IntegerType sur une colonne INT64 lève PARQUET_COLUMN_DATA_TYPE_MISMATCH.
schema = StructType([
    StructField("tpep_pickup_datetime", TimestampNTZType()),   # heure locale NY, sans fuseau : NTZ évite tout décalage
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("total_amount", DoubleType()),
])
trips = spark.read.schema(schema).parquet("data/trips.parquet")
trips.printSchema()

zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)

# %% 2. Des vues pour écrire du SQL
trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

# %% 3a. Top 10 des zones de départ (le TP 2, en 6 lignes de SQL)
top10 = spark.sql("""
    SELECT z.Zone, z.Borough, ROUND(SUM(t.total_amount), 2) AS ca, COUNT(*) AS courses
    FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
    GROUP BY z.Zone, z.Borough
    ORDER BY ca DESC
    LIMIT 10
""")
top10.show(truncate=False)

# %% 3b. Montant moyen par heure de la journée
par_heure = spark.sql("""
    SELECT HOUR(tpep_pickup_datetime) AS heure,
           ROUND(AVG(total_amount), 2) AS montant_moyen,
           COUNT(*) AS courses
    FROM trips
    GROUP BY heure
    ORDER BY heure
""")
par_heure.show(24)

# %% 4. Classement des arrondissements — la même chose avec l'API DataFrame
par_borough = (trips.join(zones, trips.PULocationID == zones.LocationID)
               .groupBy("Borough")
               .agg(F.round(F.sum("total_amount"), 2).alias("ca"), F.count("*").alias("courses"))
               .orderBy(F.desc("ca")))
par_borough.show()

# %% 5. Écrire le résultat partitionné par jour
shutil.rmtree("out/trips_par_jour", ignore_errors=True)
(trips.withColumn("jour", F.to_date("tpep_pickup_datetime"))
      .write.mode("overwrite").partitionBy("jour").parquet("out/trips_par_jour"))
print("Partitions en mémoire :", trips.rdd.getNumPartitions(), "— dossiers out/trips_par_jour/jour=… : 31")

# %% Bonus – explain() : Catalyst a optimisé le plan (filtre et projection poussés vers la lecture Parquet)
top10.explain()

# %% Bonus – ANSI SQL par défaut dans Spark 4 : une division par zéro lève une erreur
try:
    spark.sql("SELECT total_amount / trip_distance AS prix_par_mile FROM trips WHERE trip_distance = 0").show(3)
except Exception as e:
    print("Erreur ANSI attendue :", type(e).__name__, "→ utiliser try_divide() ou filtrer trip_distance > 0")
spark.sql("SELECT ROUND(AVG(try_divide(total_amount, trip_distance)), 2) AS prix_moyen_par_mile FROM trips").show()

spark.stop()
