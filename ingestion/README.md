


## Objectif

assurer l'ingestion de tickets de support client
dans Apache Kafka

## Technologies

- Python
- Apache Kafka 3.9.1
- Docker
- Hugging Face Datasets
- kafka-python

## Dataset

Source : Bitext Customer Support Dataset (Hugging Face)

Nombre d'exemples : 26 872

Colonnes originales :
- flags
- instruction
- category
- intent
- response

## Topics Kafka

- tickets : création des tickets de support
- mises_a_jour_tickets : changements de statut

Chaque topic possède 3 partitions.

## Structure du module

ingestion/
- download_dataset.py
- prepare_tickets.py
- producer_tickets.py
- producer_updates.py
- producer_dataset.py
- requirements.txt
- README.md

## Installation

Depuis la racine du projet :

```powershell
py -m pip install -r ingestion/requirements.txt
```

## Démarrer Kafka

```powershell
docker compose up -d kafka
```

## Préparer les données

```powershell
py ingestion/download_dataset.py
py ingestion/prepare_tickets.py
```

## Envoyer les tickets vers Kafka

```powershell
py ingestion/producer_dataset.py
```

## Envoyer les mises à jour

```powershell
py ingestion/producer_updates.py
```

## Vérifier les messages Kafka

```powershell
docker exec il07-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic tickets --time latest
```

## Résultats du test

- 26 872 tickets du dataset envoyés
- 13 messages supplémentaires envoyés lors des tests
- 26 885 messages au total
- 3 partitions Kafka
- Ingestion validée

## Intégration

Les messages JSON du topic tickets pourront être
consommés par Spark Structured Streaming.

Le topic mises_a_jour_tickets permettra de traiter
les changements de statut des tickets.
