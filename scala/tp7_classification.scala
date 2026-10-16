// TP 7 – Ce client laissera-t-il un pourboire ? (corrigé Scala)      spark-shell -i scala/tp7_classification.scala
import org.apache.spark.sql.functions._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.{RandomForestClassifier, LinearSVC}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

val zones = spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zone_lookup.csv")
val df = spark.read.parquet("data/trips.parquet").filter("trip_distance > 0 AND payment_type = 1")
  .withColumn("heure", hour(col("tpep_pickup_datetime")))
  .join(zones.select(col("LocationID").as("PULocationID"), col("Borough")), "PULocationID")
  .withColumn("label", (col("tip_amount") > 0).cast("double"))
  .withColumn("manhattan", (col("Borough") === "Manhattan").cast("double"))
  .withColumn("nuit", (col("heure") >= 20 || col("heure") < 5).cast("double"))
val Array(train, test) = df.randomSplit(Array(0.8, 0.2), seed = 7)

val assembleur = new VectorAssembler()
  .setInputCols(Array("trip_distance", "heure", "passenger_count", "manhattan", "nuit")).setOutputCol("features_brutes")
val scaler = new StandardScaler().setInputCol("features_brutes").setOutputCol("features")
val auc = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")

val rf = new Pipeline().setStages(Array(assembleur, scaler, new RandomForestClassifier().setNumTrees(50).setMaxDepth(6).setSeed(1))).fit(train)
val svm = new Pipeline().setStages(Array(assembleur, scaler, new LinearSVC().setMaxIter(50))).fit(train)
val pred = rf.transform(test)
println(f"AUC Random Forest : ${auc.evaluate(pred)}%.3f   AUC LinearSVC : ${auc.evaluate(svm.transform(test))}%.3f")
pred.groupBy("label").pivot("prediction").count().orderBy("label").show()
rf.write.overwrite().save("out/scala/modele_pourboire")   // le Random Forest : il fournit une probabilité
System.exit(0)
