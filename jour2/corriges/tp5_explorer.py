# TP 5 – Explorer avant de prédire (corrigé)
# Lancer : python jour2/corriges/tp5_explorer.py   → graphiques dans out/tp5/
# Règle d'or : agréger (groupBy) ou échantillonner (sample) DANS Spark, puis toPandas() sur un petit résultat.
import os
import matplotlib
matplotlib.use("Agg")                      # pas d'écran : on écrit des PNG (dans un notebook, retirer cette ligne)
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("TP5-explorer").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
os.makedirs("out/tp5", exist_ok=True)
sns.set_theme(style="whitegrid")

# %% 0. Les données du fil rouge, enrichies de l'heure, du jour de semaine et de l'arrondissement
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
trips = (spark.read.parquet("data/trips.parquet")
         .withColumn("heure", F.hour("tpep_pickup_datetime"))
         .withColumn("jour_semaine", F.dayofweek("tpep_pickup_datetime"))     # 1 = dimanche
         .join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID"))
trips.cache()
print("Courses :", trips.count())

# %% 1. Distribution des montants : histogramme (Spark compte par tranche de 5 $, pandas dessine)
tranches = (trips.withColumn("tranche", (F.floor(F.col("total_amount") / 5) * 5).cast("int"))
                 .groupBy("tranche").count().orderBy("tranche")
                 .filter("tranche < 150").toPandas())
plt.figure(figsize=(7, 4))
sns.barplot(data=tranches, x="tranche", y="count", color="#3494BA")
plt.xticks(rotation=90); plt.xlabel("Montant total ($, par tranche de 5)"); plt.ylabel("Courses")
plt.title("Distribution des montants"); plt.tight_layout(); plt.savefig("out/tp5/1_histogramme.png"); plt.close()

# %% 2. Montant selon la distance : nuage de points sur un ÉCHANTILLON de 3 %
ech = trips.sample(fraction=0.03, seed=1).select("trip_distance", "fare_amount").toPandas()
plt.figure(figsize=(7, 4))
plt.scatter(ech.trip_distance, ech.fare_amount, s=5, alpha=0.3)
plt.xlim(0, 30); plt.ylim(0, 120); plt.xlabel("Distance (miles)"); plt.ylabel("Tarif ($)")
plt.title("Tarif selon la distance (échantillon 3 %)"); plt.tight_layout(); plt.savefig("out/tp5/2_nuage.png"); plt.close()

# %% 3. Courses par jour de semaine et par heure : heatmap (groupBy Spark → pivot pandas)
jh = (trips.groupBy("jour_semaine", "heure").count().toPandas()
           .pivot(index="jour_semaine", columns="heure", values="count").fillna(0))
jh.index = ["Dim", "Lun", "Mar", "Mer", "Jeu", "Ven", "Sam"]
plt.figure(figsize=(8, 3.5))
sns.heatmap(jh, cmap="crest", cbar_kws={"label": "courses"})
plt.xlabel("Heure de prise en charge"); plt.ylabel(""); plt.title("Courses par jour et par heure")
plt.tight_layout(); plt.savefig("out/tp5/3_heatmap.png"); plt.close()

# %% 4. Montant moyen par arrondissement : barres
par_borough = (trips.groupBy("Borough").agg(F.avg("total_amount").alias("montant_moyen"), F.count("*").alias("courses"))
                    .orderBy(F.desc("montant_moyen")).toPandas())
print(par_borough)
plt.figure(figsize=(7, 4))
sns.barplot(data=par_borough, x="Borough", y="montant_moyen", color="#58B6C0")
plt.ylabel("Montant moyen ($)"); plt.title("Montant moyen par arrondissement de départ")
plt.tight_layout(); plt.savefig("out/tp5/4_borough.png"); plt.close()

# %% 5. Trois observations utiles pour prédire le montant
print("""
Observations :
1. Le tarif est presque proportionnel à la distance … sauf un palier horizontal à 70 $ : le forfait JFK.
   → un modèle linéaire ratera ce palier ; un arbre le capturera.
2. Deux pics de trafic, 8h et 18h : l'heure est une feature utile (demande, embouteillages).
3. Les aéroports (Queens / EWR) ont les montants moyens les plus élevés, Manhattan le plus de courses :
   l'arrondissement de départ est une feature utile.
""")

# %% Bonus – le même nuage en Plotly (interactif) : pip install plotly
try:
    import plotly.express as px
    fig = px.scatter(ech, x="trip_distance", y="fare_amount", opacity=0.3, title="Tarif selon la distance (interactif)")
    fig.write_html("out/tp5/2_nuage_interactif.html")
    print("Plotly : out/tp5/2_nuage_interactif.html")
except ImportError:
    print("Plotly non installé (pip install plotly) : bonus ignoré")

spark.stop()
