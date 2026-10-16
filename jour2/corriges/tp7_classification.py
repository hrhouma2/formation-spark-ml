# TP 7 – Ce client laissera-t-il un pourboire ? Classification avec Spark ML (corrigé)
# Lancer : python jour2/corriges/tp7_classification.py   → courbe ROC dans out/tp7/
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

spark = SparkSession.builder.appName("TP7-classification").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
os.makedirs("out/tp7", exist_ok=True)

# %% 1. Les données : paiements par CARTE uniquement (les pourboires en espèces ne sont pas enregistrés)
#    label = 1 si pourboire, 0 sinon
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
df = (spark.read.parquet("data/trips.parquet")
      .filter("trip_distance > 0 AND payment_type = 1")
      .withColumn("heure", F.hour("tpep_pickup_datetime"))
      .join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID")
      .withColumn("label", (F.col("tip_amount") > 0).cast("double"))
      .withColumn("manhattan", (F.col("Borough") == "Manhattan").cast("double"))
      .withColumn("nuit", ((F.col("heure") >= 20) | (F.col("heure") < 5)).cast("double"))
      .select("trip_distance", "heure", "passenger_count", "manhattan", "nuit", "label"))
print("Courses par carte :", df.count(), "— taux de pourboire :", round(df.agg(F.avg("label")).first()[0], 3))

train, test = df.randomSplit([0.8, 0.2], seed=7)

# %% 2. Deux classifieurs sur les mêmes features
assembleur = VectorAssembler(inputCols=["trip_distance", "heure", "passenger_count", "manhattan", "nuit"], outputCol="features_brutes")
normalisation = StandardScaler(inputCol="features_brutes", outputCol="features")   # indispensable pour la SVM
auc = BinaryClassificationEvaluator(metricName="areaUnderROC")

rf_model = Pipeline(stages=[assembleur, normalisation, RandomForestClassifier(numTrees=50, maxDepth=6, seed=1)]).fit(train)
svm_model = Pipeline(stages=[assembleur, normalisation, LinearSVC(maxIter=50)]).fit(train)
rf_pred, svm_pred = rf_model.transform(test), svm_model.transform(test)
print(f"AUC Random Forest : {auc.evaluate(rf_pred):.3f}   AUC LinearSVC : {auc.evaluate(svm_pred):.3f}")

# %% 3. Matrice de confusion, précision, rappel (Random Forest, seuil 0,5)
rf_pred.groupBy("label").pivot("prediction").count().orderBy("label").show()
for metrique in ["accuracy", "weightedPrecision", "weightedRecall"]:
    print(f"{metrique:18s} {MulticlassClassificationEvaluator(metricName=metrique).evaluate(rf_pred):.3f}")

# %% 4. Importance des features (Random Forest) : qu'est-ce qui fait pencher la balance ?
rf = rf_model.stages[-1]
for nom, imp in sorted(zip(assembleur.getInputCols(), rf.featureImportances.toArray()), key=lambda t: -t[1]):
    print(f"  {nom:16s} {imp:.3f}")

# %% 5. Courbe ROC en Matplotlib : le test est petit → toPandas() est raisonnable
scores = rf_pred.select("label", vector_to_array("probability")[1].alias("p")).toPandas()
y, p = scores.label.values, scores.p.values
import numpy as np
seuils = list(np.quantile(p, np.linspace(0, 1, 50))) + [1.01]   # seuils = quantiles : points bien répartis sur la courbe
tpr = [((p >= t) & (y == 1)).sum() / (y == 1).sum() for t in seuils]
fpr = [((p >= t) & (y == 0)).sum() / (y == 0).sum() for t in seuils]
plt.figure(figsize=(5, 5))
plt.plot(fpr, tpr, label=f"Random Forest (AUC = {auc.evaluate(rf_pred):.2f})")
plt.plot([0, 1], [0, 1], "--", color="gray", label="Hasard (AUC = 0,5)")
plt.xlabel("Taux de faux positifs"); plt.ylabel("Taux de vrais positifs (rappel)"); plt.legend(); plt.title("Courbe ROC – pourboire")
plt.tight_layout(); plt.savefig("out/tp7/roc.png"); plt.close()
print("Courbe ROC : out/tp7/roc.png")

# %% 6. Sauvegarder le meilleur pipeline
rf_model.write().overwrite().save("out/modele_pourboire")

# %% Bonus – CrossValidator : chercher les meilleurs hyperparamètres (3 plis × 4 combinaisons = 12 entraînements)
grille = (ParamGridBuilder()
          .addGrid(rf.numTrees, [20, 50])
          .addGrid(rf.maxDepth, [4, 8])
          .build())
cv = CrossValidator(estimator=Pipeline(stages=[assembleur, normalisation, RandomForestClassifier(seed=1)]),
                    estimatorParamMaps=grille, evaluator=auc, numFolds=3, seed=1)
cv_model = cv.fit(train)
best = cv_model.bestModel.stages[-1]
print(f"Meilleur : numTrees={best.getNumTrees}, maxDepth={best.getMaxDepth()} — AUC test = {auc.evaluate(cv_model.transform(test)):.3f}")

spark.stop()
