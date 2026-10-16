// TP 2 – Top 10 des zones avec les RDD (corrigé Scala)
// Lancer depuis la racine du dépôt :  spark-shell -i scala/tp2_rdd.scala
val sc = spark.sparkContext

// 1. Charger trips.csv et retirer l'en-tête
val lignes = sc.textFile("data/trips.csv")
val entete = lignes.first()
val courses = lignes.filter(_ != entete)

// 2. PairRDD (PULocationID, total_amount) – colonnes 7 et 16
val paires = courses.map(_.split(",")).map(c => (c(7).toInt, c(16).toDouble))

// 3. Chiffre d'affaires par zone
val caParZone = paires.reduceByKey(_ + _)

// 4. Top 10
val top10 = caParZone.takeOrdered(10)(Ordering.by[(Int, Double), Double](-_._2))

// 5. Noms des zones : le CSV met tous les champs entre guillemets
val zl = sc.textFile("data/taxi_zone_lookup.csv")
val zlEntete = zl.first()
val zones = zl.filter(_ != zlEntete)
  .map(_.split(",").map(_.replace("\"", "")))
  .map(c => (c(0).toInt, s"${c(2)} (${c(1)})"))
val resultat = sc.parallelize(top10.toSeq).join(zones).sortBy(-_._2._1).collect()

println("\nTop 10 des zones de départ par chiffre d'affaires :")
resultat.zipWithIndex.foreach { case ((_, (ca, nom)), i) => println(f"${i + 1}%2d. $nom%-45s $ca%,12.2f $$") }
System.exit(0)
