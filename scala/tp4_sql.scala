// TP 4 – DataFrames et Spark SQL (corrigé Scala)      spark-shell -i scala/tp4_sql.scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("tpep_pickup_datetime", TimestampNTZType),
  StructField("PULocationID", IntegerType),
  StructField("DOLocationID", IntegerType),
  StructField("trip_distance", DoubleType),
  StructField("total_amount", DoubleType)))
val trips = spark.read.schema(schema).parquet("data/trips.parquet")
val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
trips.createOrReplaceTempView("trips")
zones.createOrReplaceTempView("zones")

// Top 10 des zones de départ
spark.sql("""
  SELECT z.Zone, z.Borough, ROUND(SUM(t.total_amount), 2) AS ca, COUNT(*) AS courses
  FROM trips t JOIN zones z ON t.PULocationID = z.LocationID
  GROUP BY z.Zone, z.Borough ORDER BY ca DESC LIMIT 10""").show(false)

// Courses et CA par heure
spark.sql("""SELECT HOUR(tpep_pickup_datetime) AS heure, COUNT(*) AS courses, ROUND(SUM(total_amount), 0) AS ca
             FROM trips GROUP BY heure ORDER BY ca DESC""").show(3)

// API DataFrame : arrondissements
trips.join(zones, trips("PULocationID") === zones("LocationID"))
  .groupBy("Borough").agg(round(sum("total_amount"), 2).as("ca"), count("*").as("courses"))
  .orderBy(desc("ca")).show()

// Écriture partitionnée par jour
trips.withColumn("jour", to_date(col("tpep_pickup_datetime")))
  .write.mode("overwrite").partitionBy("jour").parquet("out/scala/trips_par_jour")
println("Dossiers jour=… : " + new java.io.File("out/scala/trips_par_jour").list().count(_.startsWith("jour=")))

// Dataset typé : le compilateur connaît le type de chaque colonne (.as[...] + lambdas Scala)
// Dans une application compilée (sbt), on écrirait .as[Course] avec une case class ; dans spark-shell -i,
// une case class déclarée dans le script peut bloquer l'exécution : on utilise ici un tuple typé.
import spark.implicits._
val ds = trips.select("PULocationID", "trip_distance", "total_amount").as[(Int, Double, Double)]
println("Courses > 10 miles (Dataset typé) : " + ds.filter { case (_, distance, _) => distance > 10 }.count())
System.exit(0)
