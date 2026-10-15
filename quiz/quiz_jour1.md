# Quiz express – Jour 1 (11h50, 10 min)

Format : Slido / Kahoot, ou mains levées. Chaque mauvaise réponse fréquente = 1 minute de rappel.
Ces questions préparent le QCM de validation des acquis du dernier jour.

## 1. Transformation (paresseuse) ou action (déclenche le calcul) ?

| Opération | Réponse |
|---|---|
| `map` | transformation |
| `collect` | **action** |
| `filter` | transformation |
| `count` | **action** |
| `reduceByKey` | transformation (wide) |
| `take(5)` | **action** |
| `join` | transformation (wide) |
| `saveAsTextFile` | **action** |

## 2. Combien de stages pour ce job ?

```python
rdd.map(f).filter(g).reduceByKey(h).map(k).count()
```

**2 stages.** Un seul shuffle (`reduceByKey`) ; `map` et `filter` sont narrow et restent dans le même stage.

## 3. Vrai ou faux ?

| Affirmation | Réponse |
|---|---|
| Un RDD peut être modifié après sa création | **Faux** : immuable, une transformation crée un nouveau RDD |
| Si un Executor tombe, Spark recalcule ses partitions grâce à la lignée | **Vrai** |
| `groupByKey` envoie moins de données sur le réseau que `reduceByKey` | **Faux** : `reduceByKey` pré-agrège sur chaque partition avant le shuffle |

## Questions de réserve (si le groupe est rapide)

- Que contient `spark.sparkContext` ? → le SparkContext de la session ; sert à créer des RDD.
- `local[*]` : que signifie l'étoile ? → autant de threads que de cœurs sur la machine.
- 1 action = combien de jobs ? → 1 job. 1 job = N stages (1 par shuffle + 1). 1 stage = 1 tâche par partition.
- Pourquoi Parquet est-il plus rapide que CSV à lire ? → format colonne, compressé, schéma embarqué (pas d'inférence).

## Questions pour la fin de journée (tour de table, 16h30)

- RDD ou DataFrame : lequel est le plus rapide sur le top 10 ? Pourquoi ? → DataFrame : Catalyst optimise (broadcast join, projection poussée vers Parquet).
- Que change `--master` dans `spark-submit` ? → où s'exécutent les Executors ; le code métier ne change pas.
- Que se passe-t-il si un DataNode tombe ? → les copies des blocs sur les autres DataNodes prennent le relais ; HDFS re-réplique.
