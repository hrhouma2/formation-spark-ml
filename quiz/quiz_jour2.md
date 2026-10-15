# Quiz du Jour 2 – avec les réponses

## Hier en 10 minutes (9h05)

| Question | Réponse |
|---|---|
| `reduceByKey` : transformation ou action ? `count` ? | Transformation (wide) / action |
| Qu'est-ce qui crée un nouveau stage ? | Un shuffle (transformation wide) |
| Driver, Executor, Cluster Manager : qui fait quoi ? | Le Driver planifie, le Cluster Manager alloue, les Executors exécutent |
| Pourquoi Parquet se lit-il plus vite que CSV ? | Format colonne, compressé, schéma embarqué |
| RDD ou DataFrame pour le top 10 des zones ? | DataFrame : Catalyst optimise (broadcast join, projection poussée) |

## Activité « quel type de problème ? » (Partie 1)

| Situation | Réponse |
|---|---|
| Estimer la durée d'une course | Régression |
| Repérer les courses au montant anormal | Classification si on a des exemples étiquetés, sinon non supervisé (anomalies) : débat utile |
| Prédire si un client paiera par carte | Classification |
| Regrouper les zones selon leur activité horaire | Clustering |
| Prévoir le nombre de courses demain à 18h | Régression (série temporelle, J3) |
| Dire à un chauffeur où attendre ses clients | Plusieurs briques : régression de la demande + règle métier |

## Quiz flash (Partie 3, avant le TP 7)

| Situation | Diagnostic |
|---|---|
| Détecteur de fraude qui répond toujours « pas de fraude », exactitude 99 % | Mauvais modèle : classes déséquilibrées, rappel de la classe fraude = 0. Regarder rappel, AUC, précision-rappel |
| 99 % en train, 60 % en test | Sur-apprentissage : réduire maxDepth, plus d'arbres, plus de données, validation croisée |
| Utiliser l'heure d'arrivée pour prédire la durée | Non : inconnue au moment de prédire = fuite de données |
| AUC = 0,5 | Le modèle répond au hasard : vérifier label, features, jointures |

## Tour de table de fin de journée (16h30)

- Pourquoi l'arbre bat-il la régression linéaire sur le montant ? → il capte le palier du forfait JFK (70 $).
- R² = 0,98 avec `total_amount` : bonne nouvelle ? → non, fuite : `total_amount` contient le tarif.
- AUC 0,70 pour le pourboire : bon modèle ? → honnête pour un comportement humain avec 5 features ; on gagnerait avec la zone précise, le jour, la météo.
- Sans StandardScaler, l'heure n'a joué aucun rôle dans K-means : pourquoi ? → le montant (dizaines de dollars) domine la distance euclidienne.
