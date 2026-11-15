# IL-07 — Classification des tickets de support

Projet Big Data réalisé dans le cadre de la filière Ingénierie Logicielle,
à la Faculté des Sciences d’Agadir — Université Ibn Zohr.

## Objectif

Construire une plateforme capable de :

- recevoir des tickets de support en flux ;
- prédire leur catégorie et leur priorité ;
- suivre les changements de statut des tickets ;
- calculer les délais de résolution et le respect des SLA ;
- afficher les indicateurs dans un tableau de bord.

## État du projet

**Phase actuelle : initialisation du dépôt.**

Les composants décrits ci-dessous sont prévus, mais ne sont pas encore
implémentés. Les instructions d’installation seront ajoutées progressivement.

## Architecture prévue

```text
Sources de données → Kafka → Spark Streaming et MLlib
                                ↓
                         MinIO + Apache Hudi
                    Bronze → Silver → Gold
                                ↓
                     Airflow + Dashboard BI
```

- **Bronze** : événements bruts.
- **Silver** : tickets nettoyés, classifiés et mis à jour.
- **Gold** : indicateurs agrégés pour l’analyse.

## Technologies prévues

- Python
- Apache Kafka
- Apache Spark : Structured Streaming, SQL et MLlib
- MinIO et Apache Hudi
- Apache Airflow
- Apache Superset ou Grafana
- Docker Compose et Git

## Structure du dépôt

```text
il07-support-tickets-lakehouse/
├── README.md
├── .gitignore
├── data/               # Préparation des données
├── ingestion/          # Producteurs et topics Kafka
├── lakehouse/          # MinIO et Apache Hudi
├── spark_jobs/         # Streaming, NLP et calculs Spark SQL
├── airflow/
│   └── dags/           # Workflows Airflow
└── dashboard/          # Configuration et exports du dashboard
```

Un fichier `docker-compose.yml` sera ajouté à la racine lors de la
configuration commune de la plateforme.

## Équipe et responsabilités

| Membre | Responsabilité principale |
| --- | --- |
| Imane ZORY | Données et ingestion Kafka |
| Mohamed BAILLA | MinIO, stockage Bronze et Lakehouse Hudi |
| Ayoub CHAMMAKH | Spark Streaming, Spark SQL et MLlib NLP |
| Zainab ERROSSAFI | Airflow et dashboard BI |

L’intégration, Docker Compose et les rapports sont réalisés collectivement.

## Sources de données prévues

- Bitext Customer Support Dataset : textes et catégories de demandes.
- Customer Support Ticket Dataset sur Kaggle : métadonnées de tickets.

L’équipe documentera les liens exacts, les conditions d’utilisation et les
étapes de préparation après vérification des jeux de données.

## Mise en route

La plateforme est en cours de construction. L’objectif final est de pouvoir
la démarrer avec Docker Compose. Les prérequis et commandes testées seront
documentés ici dès qu’ils seront disponibles.

## Collaboration

Chaque membre travaille sur une branche dédiée à sa tâche, puis propose
ses modifications à l’équipe avec une Pull Request vers `main`.
Les fichiers sensibles, les données volumineuses et les résultats générés
ne doivent pas être ajoutés au dépôt sans décision de l’équipe.
