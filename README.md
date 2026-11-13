# Detection de fraudes financieres et de cycles de blanchiment

Mini-projet Big Data n°18, Groupe 4 : **Hamdouch Mohammed** et **Marouan Ezbakhe**

Pipeline Big Data complet qui combine le **streaming temps reel** (Kafka + Spark Structured Streaming) et l'**analyse de graphes** (GraphFrames) pour detecter des transactions frauduleuses et des cycles de blanchiment d'argent. Les resultats sont stockes dans **Delta Lake** et supervises en direct avec **Prometheus + Grafana**.

## Architecture

```
                 ┌──────────────┐     ┌───────────┐     ┌──────────────────────────┐
 PaySim CSV ───▶ │ Kafka        │ ──▶ │  Kafka    │ ──▶ │ Spark Structured         │ ──▶ Delta Lake
 (6,3 M tx)      │ Producer     │     │  (Docker) │     │ Streaming (micro-batch)  │     transactions/
                 └──────┬───────┘     └───────────┘     └────────────┬─────────────┘     fraud_alerts/
                        │ :8000                                      │ :8001
                        ▼                                            ▼
                 ┌─────────────────────── Prometheus ──────────────────────┐ ──▶ Grafana :3000
                 └─────────────────────────────────────────────────────────┘

 IBM AML CSV ──▶ Spark + GraphFrames ──▶ PageRank / Label Propagation / SCC ──▶ Delta Lake
 (5 M tx)                                                                       suspicious_nodes/
                                                                                cycle_nodes/
                                                                                laundering_cycles/
```

## Stack technique

| Composant | Version | Role |
|---|---|---|
| Apache Kafka + Zookeeper | Confluent 7.4.0 (Docker) | Bus de messages pour le flux de transactions |
| PySpark | 3.5.0 | Traitement distribue, batch et streaming |
| GraphFrames | 0.8.3 (spark3.5) | PageRank, Label Propagation, composantes fortement connexes |
| Delta Lake | 3.0.0 | Stockage ACID des resultats |
| Prometheus + Grafana | Docker | Metriques et tableau de bord temps reel |
| Jupyter | | Exploration et presentation des resultats |

## Jeux de donnees

| Dataset | Volume | Usage |
|---|---|---|
| [PaySim](https://www.kaggle.com/datasets/ealaxi/paysim1) | 6 362 620 transactions, 8 213 fraudes (0,13 %) | Simulation du flux temps reel via Kafka |
| [IBM AML HI-Small](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) | 5 078 345 transactions, 515 088 comptes, 5 177 blanchiments (0,10 %) | Graphe de transactions pour la detection de cycles |

Les fichiers ne sont pas inclus dans le depot (1,6 Go). Les placer ainsi :

```
data/paysim/PS_20174392719_1491204439457_log.csv
data/ibm_aml/HI-Small_Trans.csv
data/ibm_aml/HI-Small_accounts.csv
```

## Structure du projet

```
config/config.py              Chemins, Kafka, ports Prometheus
config/spark_session.py       Session Spark unique (memoire, AQE, Delta, JAR) partagee par tous les jobs
producer/kafka_producer.py    Envoie PaySim dans le topic Kafka "transactions"
streaming/spark_consumer.py   Consomme Kafka, ecrit dans Delta Lake (exactly-once), expose les metriques
graph/build_graph.py          Chargement IBM AML (schema explicite) et construction du graphe
graph/fraud_detection.py      PageRank (top 1 %, valide contre les comptes de blanchiment) + Label Propagation
graph/cycle_detection.py      SCC sur le sous-graphe de blanchiment : cycles
maintenance/optimize_delta.py Dedoublonnage et compaction des tables Delta
monitoring/                   Configuration Prometheus et tableau de bord Grafana provisionne
notebooks/01_exploration      Analyse exploratoire des deux datasets
notebooks/02_resultats        Lecture et visualisation des resultats Delta Lake
docker-compose.yml            Kafka, Zookeeper, Prometheus, Grafana
```

## Installation

Prerequis : Python 3.9, Java 11 ou 17, Docker Desktop.

```bash
pip install -r requirements.txt
docker compose up -d --wait
```

`--wait` rend la main une fois Kafka pret : Zookeeper et Kafka ont des health checks, et Kafka ne demarre qu'apres Zookeeper.

Les JAR GraphFrames, Delta et Kafka sont telecharges automatiquement par Spark au premier lancement. La memoire du driver vaut 6 Go par defaut ; la variable `SPARK_DRIVER_MEMORY` permet de l'ajuster.

## Execution

**1. Analyse de graphes (batch)**

```bash
python graph/fraud_detection.py     # PageRank + Label Propagation
python graph/cycle_detection.py     # Cycles de blanchiment (SCC)
```

**2. Streaming temps reel** (deux terminaux)

```bash
python streaming/spark_consumer.py
python producer/kafka_producer.py                 # dataset complet
python producer/kafka_producer.py --limit 50000   # demo courte
```

Le consommateur reprend la ou il s'etait arrete grace a son checkpoint (`delta_lake/_checkpoints`). Pour rejouer le flux depuis zero, supprimer ce dossier et les tables `transactions` et `fraud_alerts`.

**3. Supervision** : ouvrir http://localhost:3000 (admin / admin). Le tableau de bord "Fraud Detection" est charge automatiquement.

**4. Maintenance** (apres un streaming)

```bash
python maintenance/optimize_delta.py
```

**5. Resultats** : ouvrir `notebooks/02_resultats_executed.ipynb`.

## Resultats

**Streaming (PaySim via Kafka)**

| Indicateur | Valeur |
|---|---|
| Transactions uniques ingerees dans Delta Lake | 6 362 620 (100 % du dataset) |
| Fraudes isolees dans `fraud_alerts` | 8 213 (0,13 %) |

**Comptes suspects (PageRank, IBM AML)**

| Indicateur | Valeur |
|---|---|
| Seuil (percentile 99 du score) | 6,21 |
| Comptes suspects | 5 192 sur 515 088 (1 %) |
| Dont reellement impliques en blanchiment | 267, soit une precision de 5,14 % |
| Taux de base (tirage aleatoire) | 1,23 % |
| Lift | x4,2 |
| Rappel | 4,2 % des 6 357 comptes de blanchiment |

PageRank isole les comptes pivots qui concentrent les flux : un compte du top 1 % a 4,2 fois plus de chances d'etre implique dans du blanchiment qu'un compte pris au hasard. Le rappel reste faible car une grande partie du blanchiment passe par des comptes peu connectes ; c'est la detection de cycles (SCC) qui couvre ces schemas.

**Cycles de blanchiment (IBM AML)**

| Indicateur | Valeur |
|---|---|
| Comptes analyses | 515 088 |
| Transactions dans le graphe | 5 078 345 |
| Transactions de blanchiment connues | 5 177 |
| Cycles de blanchiment detectes (SCC, taille > 1) | 50 |
| Comptes impliques dans ces cycles | 289 |
| Transactions de blanchiment dans les cycles | 585 |
| Plus grand cycle | 25 comptes |

Le detail et les graphiques se trouvent dans `notebooks/02_resultats_executed.ipynb`.

## Choix techniques

**Analyse**

- **SCC sur le sous-graphe de blanchiment.** Lancer `stronglyConnectedComponents` sur les 5 M d'aretes saturait la memoire du driver (plus de 140 000 etapes Spark, `OutOfMemoryError`). En restreignant l'algorithme aux 5 177 transactions marquees comme blanchiment, le calcul se termine en quelques minutes et fait ressortir exactement les boucles d'argent recherchees.
- **Seuil PageRank au percentile 99.** Dans GraphFrames le score moyen vaut 1 ; un seuil a 1,0 retenait environ 40 % des comptes. Le seuil est maintenant calcule (`approxQuantile`) pour garder le top 1 %, et chaque compte suspect est confronte a la verite terrain : precision, rappel et lift par rapport a un tirage aleatoire.
- **Identifiant de compte = banque + numero.** Un meme numero de compte peut exister dans plusieurs banques ; la cle `BankID_Account` evite de fusionner des comptes differents.

**Ingenierie**

- **Exactly-once en streaming.** Le consommateur ecrit un checkpoint (offsets Kafka deja traites) et des ecritures Delta idempotentes (`txnAppId` / `txnVersion`) : un redemarrage ou un batch rejoue par Spark ne cree pas de doublons. Sans checkpoint, chaque redemarrage relisait le topic depuis le debut, ce qui avait produit 8,1 M de lignes pour 6,4 M de transactions lors des premiers tests.
- **Un seul calcul par micro-batch.** Le batch est mis en cache avant les deux comptages et les deux ecritures : Kafka est lu et le JSON parse une seule fois au lieu de quatre.
- **Verifie par un test.** 50 000 messages consommes, consommateur tue puis relance : 50 000 lignes, aucune en double. Puis 20 000 nouveaux messages : la table passe a 70 000 lignes, le consommateur reprend exactement a l'offset 50 000 (sans checkpoint il aurait relu le topic et ecrit 120 000 lignes).
- **Contre-pression.** `maxOffsetsPerTrigger` borne la taille d'un batch et `minPartitions` repartit la lecture du topic sur 8 taches.
- **Producteur.** Lecture par blocs de 100 000 lignes et des 6 colonnes utiles seulement (memoire bornee), envoi par lots compresses (`linger_ms`, `batch_size`, gzip), cle de partition = compte emetteur, `tx_id` unique par transaction, compteur d'erreurs d'envoi expose a Prometheus. Debit mesure : environ 5 100 transactions/s contre 100 a 170 avec `iterrows()`, soit le dataset complet en une vingtaine de minutes au lieu de plusieurs heures.
- **Schema explicite.** Le CSV IBM AML (5 M de lignes) est lu avec un schema declare au lieu de `inferSchema`, ce qui evite une passe complete sur le fichier et nomme proprement les deux colonnes `Account` dupliquees.
- **Session Spark centralisee.** Un seul module configure memoire, Adaptive Query Execution, Delta et les JAR pour les jobs batch, streaming et les notebooks.
- **Infrastructure fiable.** Health checks Docker et redemarrage automatique : sans eux, Kafka s'arretait au demarrage s'il n'atteignait pas Zookeeper a temps. Topic, broker et chemin Delta sont surchargeables par variables d'environnement (`KAFKA_TOPIC`, `KAFKA_BROKER`, `DELTA_PATH`) pour tester sans toucher aux donnees.
- **Maintenance Delta.** Le streaming produit un petit fichier toutes les 10 s (348 fichiers d'environ 600 Ko). `optimize_delta.py` dedoublonne et compacte ces tables sans VACUUM, pour garder l'historique (time travel). Resultat : `transactions` passe de 8 121 085 lignes (rejeux des premiers tests) a 6 362 620 lignes en 3 fichiers.

## Limites et pistes

- La detection en streaming s'appuie sur l'etiquette `isFraud` de PaySim ; l'etape suivante serait d'y brancher un modele (MLlib) entraine sur l'historique.
- Une couche bronze en Delta (CSV converti une fois en Parquet) accelererait chaque relance des jobs de graphe.
- Le deploiement est local (`local[*]`) ; le meme code tourne sur un cluster Spark en changeant `SPARK_MASTER`.
