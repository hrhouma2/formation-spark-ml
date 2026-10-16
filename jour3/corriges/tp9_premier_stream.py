# TP 9 – Les courses arrivent en temps réel : premier stream (corrigé)
#
# En TP (en direct) :   terminal 1 → python scripts/stream_simulator.py courses --reset
#                       terminal 2 → ce script avec MODE = "live" (format console, s'arrête après 60 s)
# Corrigé reproductible : python scripts/stream_simulator.py courses --all --reset, puis ce script (MODE = "availableNow")
import os
import shutil
from pyspark.sql import SparkSession, functions as F

MODE = os.environ.get("MODE", "availableNow")       # "live" pendant le TP
spark = SparkSession.builder.appName("TP9-premier-stream").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")   # petit volume : 200 partitions par défaut, c'est trop
shutil.rmtree("out/checkpoints/tp9", ignore_errors=True)

# %% 1. Un stream a besoin d'un schéma EXPLICITE : on le prend sur les données statiques
schema = spark.read.parquet("data/trips.parquet").schema
courses = (spark.readStream
           .schema(schema)
           .option("maxFilesPerTrigger", 2)           # 2 fichiers par micro-batch : on voit le flux avancer
           .json("stream/courses"))
print("isStreaming :", courses.isStreaming)            # True : c'est un DataFrame « dynamique »

# %% 2. Jointure stream-statique : le référentiel des zones est un DataFrame statique ordinaire
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
enrichies = courses.join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID")

# %% 3. Une agrégation continue : chiffre d'affaires et nombre de courses par arrondissement, depuis le début
par_borough = (enrichies.groupBy("Borough")
               .agg(F.count("*").alias("courses"), F.round(F.sum("total_amount"), 2).alias("ca")))

# %% 4. Démarrer la requête : outputMode « complete » = toute la table résultat à chaque micro-batch
writer = (par_borough.writeStream
          .outputMode("complete")
          .option("checkpointLocation", "out/checkpoints/tp9"))
if MODE == "live":
    q = writer.format("console").option("truncate", False).trigger(processingTime="5 seconds").start()
    q.awaitTermination(60)          # 60 s de démonstration, puis on arrête
    q.stop()
else:
    q = writer.format("memory").queryName("ca_par_borough").trigger(availableNow=True).start()
    q.awaitTermination()
    spark.sql("SELECT * FROM ca_par_borough ORDER BY ca DESC").show()

# %% 5. Le suivi de la requête : combien de micro-batches, combien de lignes ?
batches = q.recentProgress
print(f"{len(batches)} micro-batches, {sum(p['numInputRows'] for p in batches)} courses traitées")
print("Dernier micro-batch :", {k: batches[-1][k] for k in ("batchId", "numInputRows")})

# %% Bonus – le même calcul en batch sur les mêmes fichiers : le résultat est identique
statique = (spark.read.schema(schema).json("stream/courses")
            .join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID")
            .groupBy("Borough").agg(F.count("*").alias("courses")).orderBy(F.desc("courses")))
statique.show()
spark.stop()
