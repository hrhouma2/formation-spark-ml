#!/usr/bin/env python3
"""
Vérification du poste AVANT la formation (à envoyer aux participants avec le mail de prérequis) :

    python scripts/check_env.py

Contrôle Java 17+, Python 3.10+, PySpark, Jupyter, Git, puis lance une vraie SparkSession.
"""
import re
import shutil
import subprocess
import sys

ok = True


def check(label, cond, hint=""):
    global ok
    print(("  ✔ " if cond else "  ✘ ") + label + ("" if cond else f"   → {hint}"))
    ok = ok and bool(cond)


def run(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except Exception:
        return None


print("Vérification de l'environnement – Formation Spark 4 & Machine Learning\n")

# Python
v = sys.version_info
check(f"Python {v.major}.{v.minor}", (v.major, v.minor) >= (3, 10), "installer Python 3.10 ou plus récent")

# Java
r = run(["java", "-version"])
java_line = ""
if r:
    for ln in (r.stderr + r.stdout).splitlines():
        if "version" in ln:          # ex. : openjdk version "21.0.2" 2024-01-16
            java_line = ln.strip()
            break
major = 0
m = re.search(r'version "(\d+)(?:\.(\d+))?', java_line)
if m:
    major = int(m.group(2)) if m.group(1) == "1" and m.group(2) else int(m.group(1))
check(f"Java : {java_line or 'introuvable'}", major >= 17, "installer un JDK 17 ou 21 (Temurin : adoptium.net) et vérifier JAVA_HOME")

# PySpark
try:
    import pyspark
    check(f"PySpark {pyspark.__version__}", pyspark.__version__.startswith("4."), "pip install pyspark (version 4.x)")
except ImportError:
    check("PySpark", False, "pip install pyspark")

# Jupyter / nbformat
try:
    import jupyter_core  # noqa
    check("Jupyter", True)
except ImportError:
    check("Jupyter", False, "pip install jupyter (ou l'extension Jupyter de VS Code)")

# Git
check("Git : " + (shutil.which("git") or "introuvable"), shutil.which("git") is not None, "installer Git (git-scm.com)")

# Une vraie SparkSession
if ok:
    print("\nDémarrage d'une SparkSession locale…")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("check_env").master("local[*]").getOrCreate()
        n = spark.range(1_000_000).count()
        check(f"SparkSession OK — Spark {spark.version}, count = {n:,}", n == 1_000_000)
        # Écrire et relire un fichier : c'est là que les postes Windows échouent souvent (winutils / HADOOP_HOME)
        import tempfile, os
        tmp = tempfile.mkdtemp(prefix="check_spark_")
        try:
            spark.range(1000).write.mode("overwrite").parquet(os.path.join(tmp, "test.parquet"))
            ok_ecrit = spark.read.parquet(os.path.join(tmp, "test.parquet")).count() == 1000
            check("Écriture / lecture Parquet", ok_ecrit)
        except Exception as e:
            check("Écriture / lecture Parquet", False,
                  f"{type(e).__name__} – sous Windows : utiliser WSL2 (recommandé) ou installer winutils + HADOOP_HOME, voir docs/prerequis.md")
        # Un mini stream avec checkpoint (jour 3)
        try:
            q = (spark.readStream.format("rate").option("rowsPerSecond", 10).load()
                 .writeStream.format("memory").queryName("check_stream")
                 .option("checkpointLocation", os.path.join(tmp, "chk")).trigger(availableNow=True).start())
            q.awaitTermination(60)
            check("Streaming + checkpoint", q.exception() is None)
        except Exception as e:
            check("Streaming + checkpoint", False, f"{type(e).__name__} : {str(e)[:150]}")
        spark.stop()
    except Exception as e:
        check("SparkSession", False, f"{type(e).__name__} : {str(e)[:200]}")

print("\n" + ("Tout est prêt pour la formation !" if ok else "Il reste des points à corriger (voir les lignes ✘)."))
sys.exit(0 if ok else 1)
