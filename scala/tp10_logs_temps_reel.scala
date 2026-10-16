// TP 10 – Logs en temps réel (corrigé Scala)
// python scripts/stream_simulator.py logs --all --reset   puis   spark-shell -i scala/tp10_logs_temps_reel.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
spark.conf.set("spark.sql.shuffle.partitions", "4")
for (d <- Seq("out/scala/chk_tp10", "out/scala/tp10")) new scala.reflect.io.Directory(new java.io.File(d)).deleteRecursively()

val motif = """^(\S+) +(\w+) +endpoint=(\S+) status=(\d+) latency_ms=(\d+) zone=(\d+)$"""
val logs = spark.readStream.option("maxFilesPerTrigger", 3).text("stream/logs").select(
  to_timestamp(regexp_extract(col("value"), motif, 1), "yyyy-MM-dd'T'HH:mm:ss.SSS").as("ts"),
  regexp_extract(col("value"), motif, 3).as("endpoint"),
  regexp_extract(col("value"), motif, 4).cast("int").as("status"),
  regexp_extract(col("value"), motif, 5).cast("int").as("latence_ms"))

val parMinute = logs.withWatermark("ts", "2 minutes")
  .groupBy(window(col("ts"), "1 minute"), col("endpoint"))
  .agg(count("*").as("requetes"), sum(when(col("status") >= 500, 1).otherwise(0)).as("erreurs"))
  .withColumn("taux_erreur", round(col("erreurs") / col("requetes"), 3))

val q = parMinute.writeStream.outputMode("append").format("parquet")
  .option("path", "out/scala/tp10").option("checkpointLocation", "out/scala/chk_tp10")
  .trigger(Trigger.AvailableNow()).start()
q.awaitTermination()

println("ALERTES (taux d'erreur > 10 %) :")
spark.read.parquet("out/scala/tp10").filter("taux_erreur > 0.10")
  .select(date_format(col("window.start"), "HH:mm").as("minute"), col("endpoint"), col("taux_erreur"))
  .orderBy("minute").show()
System.exit(0)
