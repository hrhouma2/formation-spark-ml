# TP 8 – Les familles de courses : K-means (corrigé) + bonus GraphFrames
# Lancer : python jour2/corriges/tp8_clustering.py
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName("TP8-clustering").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# %% 1. Trois variables, normalisées : distance, montant, heure
df = (spark.read.parquet("data/trips.parquet").filter("trip_distance > 0")
      .withColumn("heure", F.hour("tpep_pickup_datetime"))
      .select("PULocationID", "DOLocationID", "trip_distance", "total_amount", "heure"))
assembleur = VectorAssembler(inputCols=["trip_distance", "total_amount", "heure"], outputCol="brut")
scaler = StandardScaler(inputCol="brut", outputCol="features", withMean=True, withStd=True)

# %% 2. k de 2 à 6 : la silhouette mesure à quel point chaque point est proche de son groupe plutôt que des autres
silhouette = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
resultats = {}
for k in range(2, 7):
    model = Pipeline(stages=[assembleur, scaler, KMeans(k=k, seed=1)]).fit(df)
    resultats[k] = silhouette.evaluate(model.transform(df))
    print(f"k = {k} : silhouette = {resultats[k]:.3f}")
k_choisi = max(resultats, key=resultats.get)
print("k retenu :", k_choisi, "(meilleure silhouette, à confronter au sens métier)")

# %% 3. Décrire chaque famille
model = Pipeline(stages=[assembleur, scaler, KMeans(k=k_choisi, seed=1)]).fit(df)
familles = model.transform(df)
(familles.groupBy("prediction")
         .agg(F.count("*").alias("courses"), F.round(F.avg("trip_distance"), 1).alias("distance"),
              F.round(F.avg("total_amount"), 1).alias("montant"), F.round(F.avg("heure"), 1).alias("heure"))
         .orderBy("prediction").show())

# %% 4. Nommer les familles avec des mots métier (à adapter selon les centres obtenus)
print("Exemple de lecture : « courtes urbaines de jour », « courtes de nuit », « longues / aéroport »…")

# %% 5. Sans StandardScaler : le montant (dizaines de dollars) écrase l'heure (0–23) et la distance (miles)
brut = Pipeline(stages=[assembleur, KMeans(k=3, seed=1, featuresCol="brut")]).fit(df)
print("Centres SANS normalisation (distance, montant, heure) :")
for c in brut.stages[-1].clusterCenters():
    print("  ", [round(v, 1) for v in c], "← l'heure est identique partout : elle n'a joué aucun rôle")

# %% Bonus – GraphFrames : les zones comme un graphe (sommets = zones, arêtes = courses)
#    pip install graphframes-py  +  spark.jars.packages=io.graphframes:graphframes-spark4_2.13:<version>
#    Si le package n'est pas disponible : mode dégradé avec un groupBy.
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
aretes = df.select(F.col("PULocationID").alias("src"), F.col("DOLocationID").alias("dst"))
try:
    from graphframes import GraphFrame
    g = GraphFrame(zones.withColumnRenamed("LocationID", "id"), aretes)
    print("Top 5 PageRank :")
    g.pageRank(resetProbability=0.15, maxIter=10).vertices.orderBy(F.desc("pagerank")).select("id", "Zone", "pagerank").show(5)
    print("Zones les plus desservies (inDegrees) :")
    g.inDegrees.join(zones, g.inDegrees.id == zones.LocationID).orderBy(F.desc("inDegree")).select("Zone", "inDegree").show(5)
except Exception as e:                                    # ImportError ou JAR absent
    print("GraphFrames indisponible (", type(e).__name__, ") → mode dégradé : degrés avec groupBy")
    (aretes.groupBy("dst").count().withColumnRenamed("count", "inDegree")
           .join(zones, F.col("dst") == zones.LocationID).orderBy(F.desc("inDegree")).select("Zone", "inDegree").show(5))

spark.stop()
