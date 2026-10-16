# TP 3 – Le fil rouge sur un cluster Standalone + HDFS (50 min)

**Objectif** : exécuter le TP 2 **sans changer le code métier** sur un vrai cluster, avec les données lues depuis HDFS.
Seuls changent `--master` et le chemin des données.

Le script `tp2_cluster.py` est le TP 2 corrigé, sans `.master()` dans le code : le master vient de `spark-submit`.

## Partie A – Cluster Standalone sur votre poste (sans Docker, 20 min)

Linux, macOS ou WSL2, depuis la racine du dépôt :

```bash
bash jour1/TP3_cluster/start_cluster_local.sh        # venv activé
# → Master : spark://127.0.0.1:7077, UI http://localhost:8080 — 1 worker, 2 cœurs
spark-submit --master spark://127.0.0.1:7077 jour1/TP3_cluster/tp2_cluster.py data
```

1. Ouvrez http://localhost:8080 : combien de workers ? de cœurs ? Repérez votre application « TP3-cluster ».
2. Dans l'UI de l'application (port 4040 pendant l'exécution), retrouvez les stages du TP 2.
3. **Bonus** : lancez un second worker (commande affichée par le script) et relancez le `spark-submit`.
   Dans l'UI du master, comment les tâches se répartissent-elles ? Le job est-il plus rapide ?
   (Sur un portable de 2 à 4 cœurs avec 120 000 lignes, souvent non : les deux workers se partagent les mêmes
   cœurs physiques, et le coût de coordination dépasse le gain. Un cluster ne paie qu'avec de vraies machines et de vrais volumes.)

> Pourquoi le chemin `data` fonctionne-t-il ? Ici, driver et workers partagent le même disque. Sur un vrai cluster,
> chaque worker doit voir les données : d'où HDFS (partie B) ou un stockage objet (S3, ADLS).
> L'UI de l'application est sur le port 4040 (ou 4041 si un notebook est encore ouvert).

Arrêt : `jour1/TP3_cluster/stop_cluster_local.sh`

> Pourquoi `spark-class` et pas `start-master.sh` ? Les scripts `sbin/` ne sont plus livrés avec `pip install pyspark`.
> Avec une distribution Spark complète (tar.gz), `sbin/start-master.sh` puis `sbin/start-worker.sh spark://<hôte>:7077` font la même chose.

## Partie B – Cluster + HDFS en conteneurs (30 min)

Prérequis : Docker Desktop (ou Docker Engine) avec 4 Go de RAM alloués.

```bash
cd jour1/TP3_cluster
cp .env.example .env            # puis alignez SPARK_VERSION sur votre `pip show pyspark`
docker compose up -d
docker compose ps               # namenode, datanode, spark-master, spark-worker-1 : tous « running »
```

1. **HDFS** – déposer les données (le dépôt est monté dans `/workspace`) :
   ```bash
   docker compose exec namenode hdfs dfs -mkdir -p /data
   docker compose exec namenode hdfs dfs -put /workspace/data/trips.csv /workspace/data/taxi_zone_lookup.csv /data
   docker compose exec namenode hdfs dfs -ls /data
   ```
   Ouvrez http://localhost:9870 → *Utilities › Browse the file system*. Combien de blocs pour `trips.csv` ? (12 Mo < 128 Mo → 1 bloc)
2. **Spark** – lancer le job sur le cluster, données lues depuis HDFS :
   ```bash
   docker compose exec spark-master spark-submit --master spark://spark-master:7077 \
       /workspace/jour1/TP3_cluster/tp2_cluster.py hdfs://namenode:8020/data
   ```
3. http://localhost:8080 : l'application, le worker, les cœurs utilisés.
4. **Bonus** : décommentez `spark-worker-2` dans `docker-compose.yml`, `docker compose up -d`, relancez : combien de cœurs maintenant ?

Arrêt : `docker compose down -v`

### Lire la configuration

- `conf/core-site.xml` → **où** est le système de fichiers (`fs.defaultFS`)
- `conf/hdfs-site.xml` → **comment** stocker (`dfs.replication`, taille des blocs, répertoires)
- `conf/hadoop.env` → les mêmes propriétés, au format attendu par l'image `apache/hadoop`

## Mode dégradé (si Docker ou le réseau ne coopère pas)

- Faites uniquement la **Partie A** : tout tourne avec le PySpark déjà installé.
- Les données se lisent alors en local : `tp2_cluster.py data` (au lieu de `hdfs://…`).

## Blocages fréquents

| Symptôme | Cause probable | Solution |
|---|---|---|
| `Connection refused` sur 7077 | Master pas démarré ou IP différente | `SPARK_LOCAL_IP=127.0.0.1`, vérifier http://localhost:8080 |
| Executors qui meurent, `Python in worker has different version` | Python du worker ≠ Python du driver | `export PYSPARK_PYTHON=$(which python)` avant de lancer le worker |
| `Incompatible Spark version` | pyspark (pip) ≠ image Docker | Aligner `SPARK_VERSION` dans `.env` |
| `Permission denied` sur HDFS | droits HDFS | `dfs.permissions.enabled=false` est déjà dans `hadoop.env` ; sinon `hdfs dfs -chmod -R 777 /data` |
| Port 8080 déjà pris | autre service | `--webui-port 8081` dans le script / `ports` du compose |
