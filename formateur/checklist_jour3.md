# Checklist formateur – Jour 3 (Streaming, performance, clôture)

## J-1
- [ ] `python scripts/stream_simulator.py courses --reset` et `logs --reset` testés en direct sur votre poste
- [ ] Les 3 corrigés du J3 exécutés (avec `--all`) : alerte /payment 8h22–8h25 au TP 10, CSV ≈ 0,42 s / Parquet ≈ 0,14 s au TP 11
- [ ] `out/modele_pourboire` présent (TP 7) sur votre poste pour la démonstration du TP 11
- [ ] Quiz flash et atelier Lambda / Kappa prêts (`quiz/quiz_jour3.md`)
- [ ] Rappel Digiforma : QCM + satisfaction envoyés automatiquement à 16h

## Jour J
| Heure | Quoi | Point de vigilance |
|---|---|---|
| 9h00 | Météo, « Hier en 10 minutes » | Qui n'a pas fini le TP 8 ? |
| 9h20 | Partie 1 : table non bornée, code, sources/sinks, Flume, DStreams | Montrer le simulateur en direct |
| 9h55 | **TP 9** (35 min) | Schéma obligatoire ; shuffle.partitions = 4 ; un checkpoint par requête |
| 10h30 | Pause | Arrêter requêtes et simulateurs |
| 10h45 | Partie 2 : fenêtres, watermark, output modes, Encoders | Le watermark est LA notion du jour : prendre le temps |
| 11h10 | **TP 10** (35 min) + partage | Réussi si alerte /payment 8h22–8h25 |
| 11h52 | Quiz flash | |
| 12h00 | **Émargement**, déjeuner | |
| 13h00 | Partie 3 : Catalyst, Tungsten / AQE, ML sur flux | Faire lire PushedFilters / ReadSchema |
| 13h25 | **TP 11** (55 min) + partage | Les deux plans montrent PushedFilters : la différence vient du format |
| 15h00 | Pause | |
| 15h15 | Partie 4 : Deep Learning, SparkR, atelier Lambda / Kappa, récap | Tenir l'horaire : récap terminé à 16h00 |
| **16h00** | **QCM de validation + questionnaire de satisfaction** (20 min) | Remplir en parallèle le questionnaire intervenant |
| 16h25 | Conclusion sur la formation (tour de table) | Noter les retours pour l'équipe pédagogique |
| 16h50 | **Émargement**, merci, restons en contact | Feuille complète pour les 3 jours |

## Erreurs fréquentes des participants
- Relancer une requête modifiée avec l'ancien checkpoint → erreur ou résultats incohérents : supprimer le dossier de checkpoint.
- Oublier le watermark avec une agrégation en append → « Append output mode not supported… without watermark ».
- Format de date du log : `yyyy-MM-dd'T'HH:mm:ss.SSS` (le T entre apostrophes).
- Lire un fichier pendant qu'il est écrit : le simulateur écrit puis déplace ; reproduire ce principe en production.
- Plusieurs requêtes actives oubliées dans le notebook : `[q.stop() for q in spark.streams.active]`.
