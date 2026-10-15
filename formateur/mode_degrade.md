# Mode dégradé – que faire quand ça casse

Règle Ambient IT : tout problème qui dure plus de 5 minutes → appeler le 01 48 10 09 46.

| Situation | Plan B |
|---|---|
| Pas d'internet, pip bloqué | Clé USB : `pip install usb/pyspark-4.x.tar.gz usb/py4j-*.whl` ; JDK depuis l'installateur USB |
| GitHub inaccessible | Zip du dépôt sur la clé USB ou le NAS de la salle ; `git init` local pour les livrables |
| Java introuvable malgré l'installation | `JAVA_HOME` non défini ; sous Windows, redémarrer le terminal après l'installation |
| Un poste impossible à configurer (sécurité) | Environnement de secours Ambient IT : Desktop-as-a-Service (Linux dans le navigateur) ou VM VirtualBox |
| Docker absent ou interdit (TP 3) | Partie A seulement : `start_cluster_local.sh` ; données lues en `file://` |
| HDFS ne démarre pas (TP 3 partie B) | `docker compose down -v` puis `up -d` ; sinon partie A + explication de HDFS sur l'UI du formateur |
| Version pyspark ≠ image Docker | `pip install pyspark==<version de l'image>` dans le venv, ou changer `SPARK_VERSION` dans `.env` |
| Spark UI (4040) inaccessible | Port déjà pris : Spark bascule sur 4041, 4042… ; lire `spark.sparkContext.uiWebUrl` |
| Le groupe est très hétérogène | Binômes avancé / débutant ; bonus des TP pour les rapides ; `nano` plutôt que `vim` en terminal |
| Le formateur est en retard sur le programme | Réduire le TP 3 à la partie A (20 min) ; le TP 4 est prioritaire (Spark SQL est un objectif de la fiche) |
