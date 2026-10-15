# Mail de prérequis – à envoyer 7 jours avant la session

Sur le fil « Organisation & Prérequis d'installation » créé par Ambient IT, en copie à l'équipe.

---

**Objet :** Formation Spark 4 & Machine Learning du [DATES] – prérequis d'installation (20 min)

Bonjour à toutes et à tous,

Au plaisir de vous retrouver le [DATE] pour la formation **Spark 4 & Machine Learning**.

Pour démarrer les travaux pratiques dès la première matinée, merci de préparer votre poste **avant le [DATE – 2 jours]**.
Comptez une vingtaine de minutes :

1. Installer **Java 17 ou 21** (JDK Temurin : https://adoptium.net), **Python 3.10+**, **Git** et **VS Code** avec les extensions Python et Jupyter.
2. Récupérer le dépôt de la formation et installer PySpark :
   ```
   git clone [URL_DU_DEPOT] formation-spark-ml
   cd formation-spark-ml
   python -m venv .venv && source .venv/bin/activate     (Windows : .venv\Scripts\activate)
   pip install -r requirements.txt
   ```
3. Vérifier que tout fonctionne : `python scripts/check_env.py` – toutes les lignes doivent être ✔.
4. Optionnel, pour le TP « cluster » de l'après-midi : **Docker Desktop** avec 4 Go de RAM.

Le détail est dans le fichier `docs/prerequis.md` du dépôt.

**Si une étape bloque** (droits administrateur, proxy d'entreprise, antivirus), répondez à ce mail avec la sortie
de `check_env.py` : nous vous fournirons un environnement de secours prêt à l'emploi.

Matériel : 8 Go de RAM minimum, 5 Go de disque libre, accès à pypi.org et github.com.

Les TP sont en **Python (PySpark)**. Une connaissance de base de Python, d'un terminal Unix et de Git est attendue.

À très bientôt,

[Prénom Nom] – formateur
[téléphone] – Ambient IT : 01 48 10 09 46
