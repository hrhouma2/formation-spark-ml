# TP 11 – Le modèle de pourboire en temps réel + regarder sous le capot (Catalyst, Tungsten, AQE) (corrigé)
#
# Prérequis : le modèle du TP 7 (out/modele_pourboire). S'il n'existe pas, ce script l'entraîne (1 minute).
# Flux : python scripts/stream_simulator.py courses --all --reset   (ou en direct, sans --all, avec MODE=live)
import os
import shutil
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.functions import vector_to_array
import time

MODE = os.environ.get("MODE", "availableNow")
spark = SparkSession.builder.appName("TP11-ml-streaming").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")
for d in ["out/checkpoints/tp11", "out/tp11"]:
    shutil.rmtree(d, ignore_errors=True)
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True).select(
    F.col("LocationID").alias("PULocationID"), "Borough")


# La MÊME préparation des features en batch et en streaming : une seule fonction, aucun décalage possible
def features(df):
    return (df.filter("trip_distance > 0 AND payment_type = 1")
              .withColumn("heure", F.hour("tpep_pickup_datetime"))
              .join(zones, "PULocationID")
              .withColumn("manhattan", (F.col("Borough") == "Manhattan").cast("double"))
              .withColumn("nuit", ((F.col("heure") >= 20) | (F.col("heure") < 5)).cast("double"))
              .withColumn("label", (F.col("tip_amount") > 0).cast("double")))


# %% 1. Recharger le modèle du TP 7 (ML persistence)… ou l'entraîner s'il manque
if os.path.isdir("out/modele_pourboire"):
    modele = PipelineModel.load("out/modele_pourboire")
    print("Modèle du TP 7 rechargé")
else:
    train = features(spark.read.parquet("data/trips.parquet"))
    modele = Pipeline(stages=[
        VectorAssembler(inputCols=["trip_distance", "heure", "passenger_count", "manhattan", "nuit"], outputCol="features_brutes"),
        StandardScaler(inputCol="features_brutes", outputCol="features"),
        RandomForestClassifier(numTrees=50, maxDepth=6, seed=1)]).fit(train)
    modele.write().overwrite().save("out/modele_pourboire")
    print("Modèle entraîné et sauvegardé dans out/modele_pourboire")

# %% 2. Appliquer le modèle sur le FLUX : transform() fonctionne sur un DataFrame en streaming
schema = spark.read.parquet("data/trips.parquet").schema
flux = spark.readStream.schema(schema).option("maxFilesPerTrigger", 3).json("stream/courses")
predictions = (modele.transform(features(flux))
               .withColumn("p_pourboire", vector_to_array("probability")[1])   # probabilité de la classe 1
               .select("tpep_pickup_datetime", "Borough", "label", "prediction", "p_pourboire"))

# %% 3. Taux de pourboire prédit vs réel, par fenêtre de 30 minutes et par arrondissement
suivi = (predictions
         .withColumn("ts", F.col("tpep_pickup_datetime").cast("timestamp"))
         .withWatermark("ts", "10 minutes")
         .groupBy(F.window("ts", "30 minutes"), "Borough")
         .agg(F.count("*").alias("courses"),
              F.round(F.avg("p_pourboire"), 3).alias("taux_predit"),   # moyenne des probabilités, pas des 0/1
              F.round(F.avg("label"), 3).alias("taux_reel")))


# %% 4. foreachBatch : pour chaque micro-batch, du code batch libre (ici : écrire en Parquet + afficher)
def traiter(batch_df, batch_id):
    batch_df.write.mode("append").parquet("out/tp11/suivi")
    n = batch_df.count()
    if n:
        print(f"micro-batch {batch_id} : {n} fenêtres closes écrites")


q = (suivi.writeStream.outputMode("append").foreachBatch(traiter)
     .option("checkpointLocation", "out/checkpoints/tp11"))
q = (q.trigger(processingTime="5 seconds") if MODE == "live" else q.trigger(availableNow=True)).start()
q.awaitTermination(120 if MODE == "live" else None)
q.stop()

res = spark.read.parquet("out/tp11/suivi")
(res.filter("Borough = 'Manhattan'")
    .select(F.date_format("window.start", "dd/MM HH:mm").alias("fenetre"), "courses", "taux_predit", "taux_reel")
    .orderBy("fenetre").show(12))
print("Écart moyen |prédit - réel| :", res.select(F.round(F.avg(F.abs(F.col("taux_predit") - F.col("taux_reel"))), 3)).first()[0])

# %% 5. Sous le capot : le plan de la requête de streaming
q.explain()

# %% 6. Catalyst : le même plan logique, quelle que soit la source. Regardez PushedFilters, ReadSchema et Batched
requete = lambda df: df.filter("trip_distance > 10").groupBy("PULocationID").agg(F.avg("total_amount"))
schema_csv = spark.read.parquet("data/trips.parquet").schema
csv = spark.read.csv("data/trips.csv", header=True, schema=schema_csv)
pq = spark.read.parquet("data/trips.parquet")
print("=== CSV ===");     requete(csv).explain("formatted")
print("=== Parquet ===");  requete(pq).explain("formatted")
# Dans les DEUX cas, Catalyst pousse le filtre (PushedFilters) et ne garde que 3 colonnes (ReadSchema).
# La différence est physique : le CSV doit lire et découper chaque ligne en entier ; Parquet saute les colonnes
# inutiles sur le disque, utilise les statistiques min/max pour sauter des blocs, et lit en vectorisé (Batched: true).
for nom, df in [("CSV", csv), ("Parquet", pq)]:
    t = time.time()
    for _ in range(3):
        requete(df).collect()
    print(f"{nom:8s} : {(time.time() - t) / 3:.2f} s par requête")

# %% 7. AQE (Adaptive Query Execution) : le plan est réoptimisé PENDANT l'exécution
print("AQE activé :", spark.conf.get("spark.sql.adaptive.enabled"))
r = requete(pq)
r.collect()
r.explain()     # « isFinalPlan=true » et « AQEShuffleRead coalesced » : AQE a fusionné les petites partitions

# %% 8. Tungsten : les opérateurs préfixés *(n) sont compilés ensemble en bytecode (whole-stage codegen)
plan_final = r._jdf.queryExecution().executedPlan().toString()
print("Blocs compilés par Tungsten (whole-stage codegen) :", len(set(__import__("re").findall(r"\*\((\d+)\)", plan_final))))
spark.stop()
