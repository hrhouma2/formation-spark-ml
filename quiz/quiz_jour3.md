# Quiz du Jour 3 – avec les réponses

## Hier en 10 minutes (9h05)

| Question | Réponse |
|---|---|
| StringIndexer, VectorAssembler : Estimator ou Transformer ? | StringIndexer = Estimator (apprend les catégories) ; VectorAssembler = Transformer |
| Une RMSE de 4,66 $ ? | Erreur typique de 4,66 $ sur des courses jamais vues, en pénalisant les grosses erreurs |
| Une AUC de 0,5 ? | Le modèle répond au hasard |
| Une fuite de données vue hier ? | total_amount pour prédire le tarif ; payment_type pour le pourboire |
| Pourquoi normaliser avant K-means ? | Sinon la variable à la plus grande échelle (montant en $) écrase les autres |

## Quiz flash (Partie 2, 11h52)

| Situation | Réponse |
|---|---|
| Agrégation en append : rien n'est écrit | Fenêtres pas encore closes (watermark pas dépassé), ou pas de watermark (Spark refuse alors l'append) |
| Redémarrage après panne : depuis le début ? | Non avec un checkpoint : reprise aux derniers offsets, avec l'état |
| Ligne en retard de 10 min, watermark 2 min | Ignorée (numRowsDroppedByWatermark) |
| count(distinct user) sur un flux | Non supporté (distinct exact) ; approx_count_distinct l'est |

## Partages (questions et réponses)

- TP 9 – Le batch donne-t-il le même résultat ? → oui : le streaming est un batch incrémental.
- TP 10 – Pourquoi 8h27–8h29 n'apparaissent pas ? → fenêtres pas encore closes en mode append.
- TP 10 – Watermark de 2 ou 10 min pour une alerte ? → court pour la réactivité ; long ou batch de rattrapage pour l'exhaustivité.
- TP 11 – Parquet 3× plus rapide alors que Catalyst pousse le filtre en CSV aussi ? → format colonne, statistiques min/max, lecture vectorisée.
- TP 11 – Réduire le bruit du taux prédit ? → fenêtres plus longues, agréger plusieurs quartiers, intervalle de confiance.

## Atelier Lambda / Kappa – une correction possible

Tableau de bord à 5 s et alertes : Kappa (Kafka → Structured Streaming). Facturation mensuelle exacte : job batch sur les
données archivées (Parquet / Delta). Architecture hybride, très fréquente. Valoriser la justification plutôt qu'une « bonne réponse ».
