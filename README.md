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
| [IBM AML HI-Small](https://www.kaggle.com/datasets/ealtman2019/ibm-transactions-for-anti-money-laundering-aml) | 5 078 345 transactions, 515 080 comptes, 5 177 blanchiments (0,10 %) | Graphe de transactions pour la detection de cycles |

Les fichiers ne sont pas inclus dans le depot (1,6 Go). Les placer ainsi :

```
data/paysim/PS_20174392719_1491204439457_log.csv
data/ibm_aml/HI-Small_Trans.csv
data/ibm_aml/HI-Small_accounts.csv
```

## Structure du projet

```
config/config.py              Chemins, Kafka, Spark, ports Prometheus
producer/kafka_producer.py    Envoie PaySim dans le topic Kafka "transactions"
streaming/spark_consumer.py   Consomme Kafka, ecrit dans Delta Lake, expose les metriques
graph/build_graph.py          Construit le graphe IBM AML (noeuds = comptes, aretes = transactions)
graph/fraud_detection.py      PageRank (comptes pivots) + Label Propagation (communautes)
graph/cycle_detection.py      SCC sur le sous-graphe de blanchiment : cycles
monitoring/                   Configuration Prometheus et tableau de bord Grafana provisionne
notebooks/01_exploration      Analyse exploratoire des deux datasets
notebooks/02_resultats        Lecture et visualisation des resultats Delta Lake
docker-compose.yml            Kafka, Zookeeper, Prometheus, Grafana
```

## Installation

Prerequis : Python 3.9, Java 11 ou 17, Docker Desktop.

```bash
pip install -r requirements.txt
pip install jupyter matplotlib numpy
docker compose up -d
```

Les JAR GraphFrames, Delta et Kafka sont telecharges automatiquement par Spark au premier lancement.

## Execution

**1. Analyse de graphes (batch)**

```bash
python graph/fraud_detection.py     # PageRank + Label Propagation
python graph/cycle_detection.py     # Cycles de blanchiment (SCC)
```

**2. Streaming temps reel** (deux terminaux)

```bash
python streaming/spark_consumer.py
python producer/kafka_producer.py
```

**3. Supervision** : ouvrir http://localhost:3000 (admin / admin). Le tableau de bord "Fraud Detection" est charge automatiquement.

**4. Resultats** : ouvrir `notebooks/02_resultats_executed.ipynb`.

## Resultats

**Streaming (PaySim via Kafka)**

| Indicateur | Valeur |
|---|---|
| Transactions uniques ingerees dans Delta Lake | 6 362 620 (100 % du dataset) |
| Fraudes isolees dans `fraud_alerts` | 8 213 (0,13 %) |
| Messages recus au total, rejeux compris | 8 121 085 |

Le producteur a ete relance pendant les tests ; le notebook deduplique sur la cle `(step, type, amount, nameOrig, nameDest)` pour obtenir les chiffres exacts.

**Analyse de graphes (IBM AML)**

| Indicateur | Valeur |
|---|---|
| Comptes analyses (IBM AML) | 515 080 |
| Transactions dans le graphe | 5 078 345 |
| Transactions de blanchiment connues | 5 177 |
| Cycles de blanchiment detectes (SCC, taille > 1) | 50 |
| Comptes impliques dans ces cycles | 289 |
| Transactions de blanchiment dans les cycles | 585 |
| Plus grand cycle | 25 comptes |

Le detail et les graphiques se trouvent dans `notebooks/02_resultats_executed.ipynb`.

## Choix techniques

- **SCC sur le sous-graphe de blanchiment.** Lancer `stronglyConnectedComponents` sur les 5 M d'aretes saturait la memoire du driver (plus de 140 000 etapes Spark, `OutOfMemoryError`). En restreignant l'algorithme aux 5 177 transactions marquees comme blanchiment, le calcul se termine en quelques minutes et fait ressortir exactement les boucles d'argent recherchees.
- **Identifiant de compte = banque + numero.** Un meme numero de compte peut exister dans plusieurs banques ; la cle `BankID_Account` evite de fusionner des comptes differents.
- **foreachBatch en streaming.** Chaque micro-batch de 10 s est ecrit dans Delta Lake, les fraudes sont isolees dans `fraud_alerts`, et les compteurs Prometheus sont mis a jour au meme moment.

## Limites et pistes

- Le seuil PageRank (> 1,0, soit la moyenne) est large : il retient environ 40 % des comptes. Un seuil sur le top 1 % ou une validation croisee avec les comptes de blanchiment connus rendrait ce signal plus discriminant.
- La detection en streaming s'appuie sur l'etiquette `isFraud` de PaySim ; l'etape suivante serait d'y brancher un modele (MLlib) entraine sur l'historique.
- Le deploiement est local (`local[*]`) ; le meme code tourne sur un cluster Spark en changeant `SPARK_MASTER`.
