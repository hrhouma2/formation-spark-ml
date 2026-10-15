# Checklist formateur – Jour 1

## J-7
- [ ] Mail de prérequis envoyé sur le fil « Organisation & Prérequis d'installation » (`formateur/mail_prerequis.md`)
- [ ] Questionnaire de préformation lu sur Digiforma : niveaux, attentes, contraintes (proxy, sécurité, handicap)
- [ ] Dernière version stable de Spark vérifiée sur spark.apache.org ; slide « Spark 4 en bref » à jour
- [ ] `docker compose up` du TP 3 testé sur votre poste ; `SPARK_VERSION` aligné sur `pip show pyspark`
- [ ] Environnement de secours réservé auprès d'Ambient IT si des participants sont bloqués (VM / Desktop-as-a-Service)

## J-1 – le mode dégradé (clé USB)
- [ ] Wheel `pyspark-4.x.tar.gz` + `py4j` (`pip download pyspark -d usb/`)
- [ ] Installateur JDK 17 Temurin (Windows .msi, macOS .pkg, Linux .tar.gz)
- [ ] Installateurs VS Code + extensions Python / Jupyter (`.vsix`)
- [ ] Le dépôt complet (zip), données incluses
- [ ] Images Docker sauvegardées : `docker save apache/spark:<v>-python3 apache/hadoop:3.4.1 | gzip > usb/images.tar.gz`
- [ ] Slides en PDF, corrigés en PDF
- [ ] Adaptateur HDMI / USB-C, casque micro (classe virtuelle)

## Jour J – 8h45
- [ ] Salle, projecteur, Yeti X (classe mixte), 2e écran
- [ ] Terminal et VS Code en **fond blanc**, police 16+
- [ ] Dépôt cloné et `check_env.py` ✔ sur le poste du formateur ; master Standalone testé (`start_cluster_local.sh`)
- [ ] Slido / Kahoot prêt pour le quiz de 11h50 (questions dans `quiz/quiz_jour1.md`)
- [ ] Tableau : une colonne « Attentes » (tour de table) à cocher en fin de journée

## Pendant la journée
| Heure | Quoi | Point de vigilance |
|---|---|---|
| 9h00 | Accueil, présentations, objectifs, règles | Noter les attentes au tableau. Prérequis manquants → prévenir Ambient IT |
| 9h20 | Partie 1 : contexte, pourquoi Spark, installation | Démo en direct, pas à pas |
| 10h00 | **TP 1** (30 min) | Baromètre du groupe : repérer les binômes avancé / débutant |
| 10h30 | Pause | Annoncer l'heure de reprise |
| 10h45 | Partie 2 : RDD, transformations, DAG | Montrer la Spark UI à chaque notion |
| 11h10 | **TP 2** (30 min) + partage | « Qui a réussi ? » à mi-parcours ; correction en direct |
| 11h50 | Quiz express | Noter les notions floues pour demain matin |
| 12h00 | **Émargement** puis déjeuner | Vérifier que tout le monde a signé |
| 13h00 | Partie 3 : HDFS, Standalone, gestionnaires de cluster | « Comment ça va ? » au retour |
| 13h50 | **TP 3** (50 min) + partage | Partie A pour tous ; partie B si Docker fonctionne ; mode dégradé sinon |
| 15h00 | Pause | |
| 15h15 | Partie 4 : Spark SQL, DataFrames | Démo explain() en direct |
| 15h30 | **TP 4** (45 min) + partage | Bonus explain() et ANSI pour les plus rapides |
| 16h30 | Ce qu'il faut retenir + attentes cochées | Faire reformuler par les participants |
| 16h45 | Fin de journée, **émargement** | Pousser les corrigés sur le dépôt |

## Après la journée
- [ ] Corrigés poussés (`jour1/corriges/`), message aux participants
- [ ] Notions « floues » du quiz et du tour de table notées → question / réponse de 10 min demain à 9h00
- [ ] Questionnaire intervenant Digiforma : observations sur le groupe
