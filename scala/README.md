# Corrigés en Scala

La fiche programme annonce les TP « en Scala par défaut, Python en option ». Les notebooks sont en Python ;
voici les corrigés Scala des TP centraux, pour les groupes qui travaillent en Scala.

```bash
spark-shell -i scala/tp2_rdd.scala               # RDD et PairRDD
spark-shell -i scala/tp4_sql.scala               # DataFrames, Spark SQL, Dataset typé (Encoders)
spark-shell -i scala/tp6_regression.scala        # Pipeline Spark ML, régression
spark-shell -i scala/tp7_classification.scala    # Random Forest, LinearSVC, AUC
python scripts/stream_simulator.py logs --all --reset
spark-shell -i scala/tp10_logs_temps_reel.scala  # Structured Streaming, fenêtres, watermark
```

`spark-shell` est fourni par `pip install pyspark` (dans le dossier `bin` du paquet) ou par une distribution Spark.
Testés avec Spark 4.2.0 (Scala 2.13). Résultats obtenus : top 10 et CA par heure identiques au Python,
RMSE 5,34 $ (linéaire) et 4,67 $ (arbre), AUC 0,696 (Random Forest) et 0,699 (LinearSVC), alertes /payment de 8h22 à 8h25.
Les AUC diffèrent de ±0,01 du Python : après la jointure, l'ordre des lignes change, donc le tirage de randomSplit aussi.

Deux pièges propres à `spark-shell -i` :
- une `case class` déclarée dans le script peut bloquer l'exécution (TP 4 utilise donc un tuple typé) ;
  dans un projet sbt compilé, `.as[Course]` avec une case class est la bonne pratique ;
- les accents s'affichent « ? » si le terminal n'est pas en UTF-8 (Windows : `chcp 65001`).
