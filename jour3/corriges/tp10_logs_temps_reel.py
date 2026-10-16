# TP 10 – Analyse temps réel d'un fichier de log (corrigé)
#
# L'application de réservation écrit ses logs dans stream/logs/. Une panne du service de paiement
# survient entre 8h22 et 8h26 : saurez-vous la détecter en temps réel ?
#
# En TP : terminal 1 → python scripts/stream_simulator.py logs --reset ; terminal 2 → ce script avec MODE=live
# Corrigé reproductible : python scripts/stream_simulator.py logs --all --reset, puis ce script
import os
import shutil
from pyspark.sql import SparkSession, functions as F

MODE = os.environ.get("MODE", "availableNow")
spark = SparkSession.builder.appName("TP10-logs").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", "4")
for d in ["out/checkpoints/tp10", "out/tp10"]:
    shutil.rmtree(d, ignore_errors=True)

# %% 1. Lire les logs en continu : une ligne de texte par événement
brut = spark.readStream.option("maxFilesPerTrigger", 3).text("stream/logs")
# 2024-01-15T08:22:03.120 ERROR endpoint=/payment status=503 latency_ms=812 zone=161

# %% 2. Parser chaque ligne avec des expressions régulières
motif = r"^(\S+) +(\w+) +endpoint=(\S+) status=(\d+) latency_ms=(\d+) zone=(\d+)$"
logs = brut.select(
    F.to_timestamp(F.regexp_extract("value", motif, 1), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("ts"),
    F.regexp_extract("value", motif, 2).alias("niveau"),
    F.regexp_extract("value", motif, 3).alias("endpoint"),
    F.regexp_extract("value", motif, 4).cast("int").alias("status"),
    F.regexp_extract("value", motif, 5).cast("int").alias("latence_ms"),
)

# %% 3. Agrégation par fenêtre de 1 minute (temps de l'ÉVÉNEMENT), avec un watermark de 2 minutes
#    Le watermark dit à Spark : « une ligne qui arrive avec plus de 2 min de retard est ignorée »,
#    ce qui lui permet de clore les fenêtres et de libérer la mémoire (l'état).
par_minute = (logs
              .withWatermark("ts", "2 minutes")
              .groupBy(F.window("ts", "1 minute"), "endpoint")
              .agg(F.count("*").alias("requetes"),
                   F.sum(F.when(F.col("status") >= 500, 1).otherwise(0)).alias("erreurs"),
                   F.round(F.expr("percentile_approx(latence_ms, 0.95)"), 0).alias("latence_p95"))
              .withColumn("taux_erreur", F.round(F.col("erreurs") / F.col("requetes"), 3)))

# %% 4. Sortie en mode « append » vers Parquet : une fenêtre n'est écrite qu'UNE fois, quand le watermark l'a close
q = (par_minute.writeStream
     .outputMode("append")
     .format("parquet")
     .option("path", "out/tp10/par_minute")
     .option("checkpointLocation", "out/checkpoints/tp10"))
q = (q.trigger(processingTime="5 seconds") if MODE == "live" else q.trigger(availableNow=True)).start()
q.awaitTermination(120 if MODE == "live" else None)
q.stop()

# %% 5. Lire le résultat (en batch) et détecter l'incident : taux d'erreur > 10 % sur /payment
res = spark.read.parquet("out/tp10/par_minute")
print("Fenêtres écrites :", res.count())
(res.filter("endpoint = '/payment'")
    .select(F.date_format("window.start", "HH:mm").alias("minute"), "requetes", "erreurs", "taux_erreur", "latence_p95")
    .orderBy("minute").show(40))
alertes = res.filter("taux_erreur > 0.10").select(F.date_format("window.start", "HH:mm").alias("minute"), "endpoint", "taux_erreur")
print("ALERTES (taux d'erreur > 10 %) :")
alertes.orderBy("minute").show()

# %% 6. Les lignes en retard : combien ont été ignorées par le watermark ?
p = q.recentProgress
ignorees = sum(int(x["stateOperators"][0].get("numRowsDroppedByWatermark", 0)) for x in p if x["stateOperators"])
print(f"Lignes ignorées car trop en retard (> 2 min) : {ignorees}")

# %% Bonus – fenêtres glissantes : 5 minutes, toutes les minutes (chaque événement compte dans 5 fenêtres)
glissant = (spark.read.text("stream/logs")
            .select(F.to_timestamp(F.regexp_extract("value", motif, 1), "yyyy-MM-dd'T'HH:mm:ss.SSS").alias("ts"),
                    F.regexp_extract("value", motif, 4).cast("int").alias("status"))
            .groupBy(F.window("ts", "5 minutes", "1 minute"))
            .agg(F.round(F.avg((F.col("status") >= 500).cast("int")), 3).alias("taux_erreur_5min"))
            .orderBy("window"))
(glissant.filter("window.start BETWEEN '2024-01-15 08:15:00' AND '2024-01-15 08:27:00'")
         .select(F.date_format("window.start", "HH:mm").alias("debut"), "taux_erreur_5min").show(13))
# La fenêtre glissante lisse le signal : l'alerte monte plus progressivement, mais résiste mieux aux pics isolés.
spark.stop()
