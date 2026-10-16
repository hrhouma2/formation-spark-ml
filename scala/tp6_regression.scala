// TP 6 – Prédire le montant d'une course (corrigé Scala)      spark-shell -i scala/tp6_regression.scala
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, DecisionTreeRegressor}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val df = spark.read.parquet("data/trips.parquet").filter("trip_distance > 0")
  .withColumn("heure", hour(col("tpep_pickup_datetime")))
  .join(zones.select(col("LocationID").as("PULocationID"), col("Borough")), "PULocationID")
  .select("trip_distance", "heure", "passenger_count", "Borough", "fare_amount")
val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 42)

val preparation: Array[PipelineStage] = Array(
  new StringIndexer().setInputCol("Borough").setOutputCol("borough_idx").setHandleInvalid("keep"),
  new OneHotEncoder().setInputCols(Array("borough_idx")).setOutputCols(Array("borough_vec")),
  new VectorAssembler().setInputCols(Array("trip_distance", "heure", "passenger_count", "borough_vec")).setOutputCol("features"))

val rmse = new RegressionEvaluator().setLabelCol("fare_amount").setMetricName("rmse")
for ((nom, modele) <- Seq(
       "Régression linéaire" -> new LinearRegression().setLabelCol("fare_amount"),
       "Arbre (maxDepth=6)"  -> new DecisionTreeRegressor().setLabelCol("fare_amount").setMaxDepth(6))) {
  val m = new Pipeline().setStages(preparation :+ modele).fit(train)
  println(f"$nom%-22s RMSE = ${rmse.evaluate(m.transform(test))}%5.2f $$")
  if (nom.startsWith("Arbre")) {
    m.write.overwrite().save("out/scala/modele_montant")
    println("Rechargé : " + PipelineModel.load("out/scala/modele_montant").stages.last.getClass.getSimpleName)
  }
}
System.exit(0)
