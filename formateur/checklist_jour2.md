# Checklist formateur – Jour 2 (Machine Learning)

## J-1
- [ ] `pip install matplotlib seaborn` vérifié dans l'environnement des participants (dans requirements.txt)
- [ ] `python jour2/corriges/tp6_regression.py` lancé sur votre poste : RMSE arbre ≈ 4,66 $ (les chiffres des slides)
- [ ] GraphFrames testé : `pip install graphframes-py` + JAR `io.graphframes:graphframes-spark4_2.13` ; sinon prévoir la démo en mode dégradé (`groupBy`)
- [ ] Slido / Kahoot : quiz flash (4 questions) et activité « quel type de problème ? » préparés (`quiz/quiz_jour2.md`)
- [ ] Corrigés du jour 1 poussés sur le dépôt ; notions « floues » d'hier relues

## Jour J
| Heure | Quoi | Point de vigilance |
|---|---|---|
| 9h00 | Météo du groupe à l'oral, « Hier en 10 minutes » | Qui n'a pas fini le TP 4 ? Aider avant d'avancer |
| 9h20 | Partie 1 : programmer ou apprendre, supervisé / non supervisé, cycle, activité, visualisation | Activité sans ordinateur en binômes avant le premier TP |
| 9h55 | **TP 5** (35 min) | Règle d'or : agréger dans Spark avant toPandas(). Réussi si 4 graphiques + 3 observations |
| 10h30 | Pause | |
| 10h45 | Partie 2 : briques, pipeline, arbre, code en direct | Exécuter le code cellule par cellule |
| 11h10 | **TP 6** (30 min) + partage | Réussi si RMSE arbre < 5 $. Bonus « tricher » = introduction à la fuite |
| 11h50 | Persistance, PySpark et R | SparkR déprécié : orienter vers sparklyr |
| 12h00 | **Émargement**, déjeuner | |
| 13h00 | Partie 3 : RF vs SVM (schémas), matrice, ROC, quiz flash, fuite | Faire calculer la précision à la main |
| 13h45 | **TP 7** (50 min) + partage | Réussi si AUC > 0,65 + ROC. Classes équilibrées (52 %) |
| 15h00 | Pause | |
| 15h15 | Partie 4 : K-means, démonstration GraphFrames | Animer K-means au tableau avec 10 points |
| 15h35 | **TP 8** (45 min) + partage | Réussi si k justifié + 3 familles nommées. GraphFrames en bonus |
| 16h30 | Ce qu'il faut retenir, attentes | Faire reformuler par les participants |
| 16h45 | Fin, **émargement** | Pousser les corrigés TP 5–8 |

## Erreurs fréquentes des participants
- `toPandas()` sur tout le DataFrame → mémoire du Driver saturée : rappeler sample / groupBy.
- Oubli de `handleInvalid="keep"` sur StringIndexer → erreur sur une catégorie absente du train.
- Features de la SVM non normalisées → convergence lente, AUC basse.
- `vector_to_array` oublié pour lire la probabilité (colonne `probability` est un vecteur).
- Fuite de données : `total_amount`, `tip_amount`, `payment_type` dans les features.
