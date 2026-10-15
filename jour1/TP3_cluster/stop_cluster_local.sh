#!/usr/bin/env bash
# Arrête le master et les workers lancés par start_cluster_local.sh
pkill -f org.apache.spark.deploy.worker.Worker || true
pkill -f org.apache.spark.deploy.master.Master || true
echo "Cluster Standalone arrêté."
