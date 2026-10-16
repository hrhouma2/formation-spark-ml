# Prérequis techniques – à installer avant la formation

À faire **au moins 7 jours avant** la session. Comptez 20 minutes. En cas de blocage (droits administrateur,
proxy, antivirus), signalez-le tout de suite sur le fil de mail « Organisation & Prérequis d'installation » :
nous préparerons un environnement de secours.

## 1. Logiciels

| Outil | Version | Vérifier | Installer |
|---|---|---|---|
| **Java (JDK)** | 17 ou 21 | `java -version` | [adoptium.net](https://adoptium.net) (Temurin). Définir `JAVA_HOME`. |
| **Python** | 3.10 ou plus | `python --version` | [python.org](https://www.python.org/downloads/) – cocher « Add to PATH » sous Windows |
| **Git** | récente | `git --version` | [git-scm.com](https://git-scm.com) |
| **VS Code** | récente | – | [code.visualstudio.com](https://code.visualstudio.com) + extensions **Python** et **Jupyter** |
| **Docker Desktop** (TP 3 partie B, optionnel) | récente | `docker compose version` | [docker.com](https://www.docker.com/products/docker-desktop/) – 4 Go de RAM alloués |

### Vous êtes sous Windows ? Utilisez WSL2 (fortement recommandé)

Spark est conçu pour Linux. Sous Windows natif, l'**écriture de fichiers** (Parquet, checkpoints de streaming)
échoue souvent sans les binaires Hadoop `winutils.exe` / `hadoop.dll` et la variable `HADOOP_HOME`, et plusieurs
TP (3, 4, 6, 7, 10, 11) écrivent des fichiers. La solution la plus fiable :

1. Installer **WSL2** avec Ubuntu : `wsl --install` dans PowerShell administrateur, puis redémarrer.
2. Dans Ubuntu : `sudo apt install openjdk-17-jdk python3-venv git`, puis suivre l'étape 2 ci-dessous.
3. Dans VS Code : extension **WSL**, puis « Connect to WSL » et ouvrir le dossier du dépôt.

Si vous restez en Windows natif : PowerShell peut refuser d'activer le venv →
`Set-ExecutionPolicy RemoteSigned -Scope CurrentUser`. Si `java` n'est pas trouvé, définissez `JAVA_HOME`.
`check_env.py` teste une vraie écriture Parquet : s'il échoue sur ce point, passez à WSL2 ou signalez-le-nous.

## 2. Le dépôt et PySpark

```bash
git clone <URL_DU_DEPOT> formation-spark-ml
cd formation-spark-ml
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell : .venv\Scripts\Activate.ps1
pip install -r requirements.txt    # pyspark 4.x, jupyter, pandas, pyarrow
```

## 3. Vérification (1 minute)

```bash
python scripts/check_env.py
```

Toutes les lignes doivent être ✔ et se terminer par « Tout est prêt pour la formation ! ».
Envoyez-nous la sortie de cette commande si une ligne est ✘.

## 4. Configuration matérielle

- 8 Go de RAM minimum (16 Go recommandés pour le TP 3 avec Docker)
- 5 Go d'espace disque libre
- Droits d'installation sur le poste, ou environnement déjà préparé par votre service informatique
- Accès réseau à pypi.org, github.com et hub.docker.com (ou leurs miroirs internes)

## Solution de secours

Si votre poste ne peut pas être configuré (politique de sécurité, proxy), signalez-le : un environnement
Linux prêt à l'emploi dans le navigateur (Desktop-as-a-Service) ou une machine virtuelle vous sera fournie.
