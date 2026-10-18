# TP 3 – Le TP 2 sur un cluster Standalone, données lues depuis HDFS (ou en local)
#
#   spark-submit --master spark://localhost:7077 jour1/TP3_cluster/tp2_cluster.py hdfs://namenode:8020/data   # partie B (docker compose)
#   spark-submit --master spark://localhost:7077 jour1/TP3_cluster/tp2_cluster.py data        # sans HDFS (mode dégradé)
#
# Le code métier est IDENTIQUE au TP 2 : seuls --master et le chemin des données changent.
import csv
import sys
from pyspark.sql import SparkSession

base = sys.argv[1] if len(sys.argv) > 1 else "data"
spark = SparkSession.builder.appName("TP3-cluster").getOrCreate()    # pas de .master() : il vient de spark-submit
sc = spark.sparkContext
print("Master :", sc.master, "— données :", base)

lignes = sc.textFile(f"{base}/trips.csv")
entete = lignes.first()
paires = (lignes.filter(lambda l: l != entete)
                .map(lambda l: l.split(","))
                .map(lambda c: (int(c[7]), float(c[16]))))
ca_par_zone = paires.reduceByKey(lambda a, b: a + b)
top10 = ca_par_zone.takeOrdered(10, key=lambda kv: -kv[1])

zl = sc.textFile(f"{base}/taxi_zone_lookup.csv")
zl_entete = zl.first()
zones = (zl.filter(lambda l: l != zl_entete)
           .map(lambda l: next(csv.reader([l])))
           .map(lambda c: (int(c[0]), f"{c[2]} ({c[1]})")))
resultat = sc.parallelize(top10).join(zones).sortBy(lambda kv: -kv[1][0]).collect()

print("\nTop 10 des zones de départ — calculé sur", sc.defaultParallelism, "cœurs du cluster")
for rang, (zid, (ca, nom)) in enumerate(resultat, 1):
    print(f"{rang:2d}. {nom:45s} {ca:12,.2f} $")
spark.stop()
