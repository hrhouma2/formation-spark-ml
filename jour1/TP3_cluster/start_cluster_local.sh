#!/usr/bin/env bash
# TP 3 – Partie A : un cluster Spark Standalone SANS Docker, à partir du PySpark installé par pip.
# (Les scripts sbin/start-master.sh ne sont plus livrés avec pip install pyspark : on passe par spark-class.)
#
#   source jour1/TP3_cluster/start_cluster_local.sh        # démarre master + 1 worker
#   spark-submit --master spark://127.0.0.1:7077 jour1/TP3_cluster/tp2_cluster.py data
#   jour1/TP3_cluster/stop_cluster_local.sh
#
# Linux, macOS, WSL2. Interface du master : http://localhost:8080
set -e
export SPARK_HOME="${SPARK_HOME:-$(python -c 'import pyspark, os; print(os.path.dirname(pyspark.__file__))')}"
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$(command -v python)}"
mkdir -p /tmp/spark-tp3
echo "SPARK_HOME = $SPARK_HOME"

nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
      --host 127.0.0.1 --port 7077 --webui-port 8080 > /tmp/spark-tp3/master.log 2>&1 &
sleep 5
WORKER_CORES="${WORKER_CORES:-2}"
nohup "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker spark://127.0.0.1:7077 \
      --cores "$WORKER_CORES" --memory 1g > /tmp/spark-tp3/worker-1.log 2>&1 &
sleep 5
echo "Master  : spark://127.0.0.1:7077  (UI http://localhost:8080)"
echo "Worker  : $WORKER_CORES cœurs, 1 Go — logs dans /tmp/spark-tp3/"
echo "Bonus   : un 2e worker → relancez la commande Worker ci-dessus avec --webui-port 8082"
