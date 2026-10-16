# TP 6 – Combien va coûter cette course ? Régression avec Spark ML (corrigé)
# Lancer : python jour2/corriges/tp6_regression.py
from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

spark = SparkSession.builder.appName("TP6-regression").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# %% 1. Les features : distance, heure, passagers, arrondissement de départ. Le label : fare_amount
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)
df = (spark.read.parquet("data/trips.parquet")
      .filter("trip_distance > 0")                                   # courses annulées / erreurs GPS
      .withColumn("heure", F.hour("tpep_pickup_datetime"))
      .join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID")
      .select("trip_distance", "heure", "passenger_count", "Borough", "fare_amount"))

# %% 2. Train / test : 80 / 20, graine fixée pour que tout le monde ait le même découpage
train, test = df.randomSplit([0.8, 0.2], seed=42)
print(f"train : {train.count():,} courses — test : {test.count():,} courses")

# %% 3. La préparation, commune aux deux modèles
preparation = [
    StringIndexer(inputCol="Borough", outputCol="borough_idx", handleInvalid="keep"),   # texte → index
    OneHotEncoder(inputCols=["borough_idx"], outputCols=["borough_vec"]),               # index → vecteur 0/1
    VectorAssembler(inputCols=["trip_distance", "heure", "passenger_count", "borough_vec"],
                    outputCol="features"),                                              # tout dans une colonne
]

# %% 4. Deux modèles, le même pipeline : seule la dernière étape change
def evaluer(nom, estimateur):
    model = Pipeline(stages=preparation + [estimateur]).fit(train)   # apprend sur le train
    pred = model.transform(test)                                      # prédit sur le test
    rmse = RegressionEvaluator(labelCol="fare_amount", metricName="rmse").evaluate(pred)
    r2 = RegressionEvaluator(labelCol="fare_amount", metricName="r2").evaluate(pred)
    print(f"{nom:22s} RMSE = {rmse:5.2f} $   R² = {r2:.3f}")
    return model, pred

lr_model, lr_pred = evaluer("Régression linéaire", LinearRegression(labelCol="fare_amount"))
dt_model, dt_pred = evaluer("Arbre (maxDepth=6)", DecisionTreeRegressor(labelCol="fare_amount", maxDepth=6))

# %% 5. Regarder quelques prédictions, et le premier découpage de l'arbre
dt_pred.select("trip_distance", "heure", "Borough", "fare_amount", F.round("prediction", 2).alias("prediction")).show(5)
print("\n".join(dt_model.stages[-1].toDebugString.splitlines()[:4]))   # « If (feature 0 <= 5.96) » : la distance d'abord

# %% 6. Sauvegarder le meilleur pipeline, puis le recharger (ML persistence)
dt_model.write().overwrite().save("out/modele_montant")
recharge = PipelineModel.load("out/modele_montant")
print("Modèle rechargé :", type(recharge.stages[-1]).__name__)

# %% Bonus 1 – GBTRegressor : des arbres en série, chacun corrige les erreurs du précédent (plus long)
evaluer("GBT (20 itérations)", GBTRegressor(labelCol="fare_amount", maxIter=20, maxDepth=5))
# … et un arbre avec la distance SEULE : heure et passagers apportent très peu (≈ 4,8 $ contre ≈ 4,7 $)
seule = Pipeline(stages=[VectorAssembler(inputCols=["trip_distance"], outputCol="features"),
                         DecisionTreeRegressor(labelCol="fare_amount", maxDepth=6)]).fit(train).transform(test)
print(f"Arbre, distance seule  RMSE = {RegressionEvaluator(labelCol='fare_amount', metricName='rmse').evaluate(seule):5.2f} $")

# %% Bonus 2 – Connaître le métier : RatecodeID (2 = forfait JFK) divise l'erreur par deux
df_rc = (spark.read.parquet("data/trips.parquet").filter("trip_distance > 0")
         .withColumn("heure", F.hour("tpep_pickup_datetime"))
         .join(zones.select(F.col("LocationID").alias("PULocationID"), "Borough"), "PULocationID"))
tr_rc, te_rc = df_rc.randomSplit([0.8, 0.2], seed=42)
prep_rc = preparation[:2] + [VectorAssembler(inputCols=["trip_distance", "heure", "passenger_count", "borough_vec", "RatecodeID"],
                                             outputCol="features")]
p = Pipeline(stages=prep_rc + [DecisionTreeRegressor(labelCol="fare_amount", maxDepth=6)]).fit(tr_rc).transform(te_rc)
print(f"Arbre + RatecodeID     RMSE = {RegressionEvaluator(labelCol='fare_amount', metricName='rmse').evaluate(p):5.2f} $"
      "  ← une seule feature métier vaut mieux que tous les réglages")

# %% Bonus 3 – « Tricher » avec total_amount : excellent score, modèle inutilisable
#    (R² < 1 car total_amount contient aussi le pourboire, les taxes et les péages, qui varient)
df_triche = (spark.read.parquet("data/trips.parquet").filter("trip_distance > 0")
             .select("trip_distance", "total_amount", "fare_amount"))
tr2, te2 = df_triche.randomSplit([0.8, 0.2], seed=42)
p = Pipeline(stages=[VectorAssembler(inputCols=["trip_distance", "total_amount"], outputCol="features"),
                     LinearRegression(labelCol="fare_amount")]).fit(tr2).transform(te2)
print(f"Avec total_amount : R² = {RegressionEvaluator(labelCol='fare_amount', metricName='r2').evaluate(p):.3f}"
      "  ← fuite de données : total_amount contient déjà le tarif et n'est connu qu'après la course")

spark.stop()
