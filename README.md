# 🛒 End-to-End E-Commerce Lakehouse & Decision Analytics Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-4.2-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-4.4-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io/)
[![dbt](https://img.shields.io/badge/dbt_Core-1.12-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)
[![Apache Airflow](https://img.shields.io/badge/Apache_Airflow-Orchestration-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Power BI](https://img.shields.io/badge/Power_BI-Desktop-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)

---

##  Présentation du Projet

Conception et déploiement d'une **plateforme de données décisionnelle et prédictive complète** basée sur l'**Architecture Médaillon (Bronze, Silver, Gold)**. 

Le système ingère près d'un demi-million d'enregistrements bruts (dataset e-commerce réel d'Olist), applique des traitements distribués avec **PySpark** et **Delta Lake** (garantie ACID), modélise un **schéma dimensionnel en étoile certifié par des tests dbt**, entraîne **deux modèles de Machine Learning** (Supply Chain & Marketing) et restitue les indicateurs à travers des **tableaux de bord interactifs Power BI**.

---

##  Architecture Globale de la Plateforme

```mermaid
graph TD
    A[📦 Sources Olist CSV <br> ~450k lignes brutes] -->|Ingestion PySpark| B

    subgraph Lakehouse ["🏢 DATA LAKEHOUSE (Architecture Médaillon)"]
        B[🥉 Couche Bronze <br> Format Snappy Parquet + Horodatage Audit] -->|Nettoyage & Typage Spark| C[🥈 Couche Silver <br> Tables Delta Lake ACID & Log Transactions]
    end

    C -->|DeltaTable Reader Snapshot| D

    subgraph Warehouse ["🗄️ DATA WAREHOUSE POSTGRESQL (Docker)"]
        D[Vues Staging dbt: stg_*] --> E[🥇 Couche Gold : Schéma en Étoile <br> dim_customers, dim_products, fct_orders]
        E --> F{🛡️ dbt test <br> 8/8 Tests de Qualité Validés}
    end

    F -->|Données Qualifiées| G[🤖 Random Forest Classifier <br> Prédiction Retards Logistiques]
    F -->|Données Qualifiées| H[🤖 K-Means Clustering & Scoring <br> Segmentation RFM & Risque de Churn]

    G --> I[Table: gold.ml_delivery_risk]
    H --> J[Table: gold.ml_customer_insights]

    E --> K
    I --> K
    J --> K

    subgraph BI ["📊 RESTITUTION : POWER BI DESKTOP"]
        K[Dashboards Décisionnels <br> • Page 1: Supply Chain & IA Retards <br> • Page 2: Customer Intelligence & IA Churn]
    end

    style Lakehouse fill:#f0f9ff,stroke:#0284c7,stroke-width:2px
    style Warehouse fill:#fef3c7,stroke:#d97706,stroke-width:2px
    style BI fill:#fdf4ff,stroke:#c026d3,stroke-width:2px

⚡ Métriques Clés & Performance Technique

| Étape du Pipeline       | Technologie         | Volume / Métrique Clé          | Résultat Métier                                                         |
| :---------------------- | :------------------ | :----------------------------- | :---------------------------------------------------------------------- |
| **Ingestion Bronze**    | PySpark 4.2         | 450 000 lignes brutes          | Conversion Parquet & traçabilité (`ingestion_timestamp`)                |
| **Nettoyage Silver**    | Delta Lake 4.4      | 4 tables transactionnelles     | Support ACID, Time Travel & Feature Engineering                         |
| **Data Warehouse**      | PostgreSQL 15       | Conteneur Docker persistant    | Landing zone analytique isolée sur port 5432                            |
| **Transformation Gold** | dbt-Core 1.12       | 7 modèles (4 vues, 3 tables)   | Schéma en étoile dimensionnel matérialisé en \~3s                       |
| **Data Quality**        | dbt test            | **8/8 tests passés (100 %)**   | Unicité des PK, complétude et intégrité référentielle FK                |
| **ML Supply Chain**     | Scikit-Learn        | Random Forest Classifier       | **61,3 % de recall** sur les retards grâce au `class_weight='balanced'` |
| **ML Marketing**        | Scikit-Learn        | K-Means + Scoring Churn        | 96 000 clients segmentés (4,9 % VIPs générant 1,6 M$)                   |
| **Pipeline complet**    | Python / Subprocess | **Exécution totale en \~215s** | Orchestration automatisée de bout en bout                               |

📊 Tableaux de Bord Power BI (Aperçu)

Le modèle de données alimente deux pages décisionnelles hautement interactives
avec filtres croisés en temps réel.

Page 1 : Supply Chain & Delivery Delay Risk

  - KPIs Exécutifs : Chiffre d'Affaires total ($15,84M), Volume de commandes
    (99K), Délai moyen de livraison (12,5 jours), Taux de retard réel (7,87 %).
  - Visualisation IA : Répartition des commandes par niveau de risque prédictif
    (Haut Risque, Modéré, Faible).
  - Analyse Temporelle & Géographique : Saisonnalité des ventes et classement
    des États les plus touchés par les retards (São Paulo, Rio de Janeiro, Minas
    Gerais).
  - Pilotage Opérationnel : Tableau de surveillance des commandes critiques pour
    action proactive.

(Placez votre capture dans screenshots/page1_supply_chain.png)
Supply Chain Dashboard

Page 2 : Customer Intelligence & Churn Retention

  - KPIs Valeur Client : 96K clients uniques, Panier moyen ($159,33), Risque
    moyen d'attrition (71,41 %), 4,87K clients VIP.
  - Clustering K-Means : 4 segments comportementaux identifiés (Clients
    Endormis 41,4 %, Clients Perdus 30,3 %, Clients Actifs 23,5 %, VIP /
    Champions 4,9 %).
  - Analyse Financière : Répartition du chiffre d'affaires par cluster
    (identification du réservoir de réactivation de 6,4 M$).
  - Plan d'Action Marketing : Liste de ciblage dynamique avec score de Churn par
    client pour campagnes promotionnelles.

(Placez votre capture dans screenshots/page2_customer_churn.png)
Customer Intelligence Dashboard

📂 Structure du Répertoire

modern-lakehouse-ecommerce/
├── airflow/
│   └── dags/
│       └── lakehouse_pipeline_dag.py    # Définition officielle du DAG d'orchestration
├── data/
│   └── raw/                             # Fichiers sources CSV d'Olist
├── dbt_project/
│   ├── models/
│   │   ├── staging/                     # Vues SQL de nettoyage (stg_*)
│   │   │   ├── sources.yml
│   │   │   ├── stg_orders.sql
│   │   │   ├── stg_order_items.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_products.sql
│   │   └── marts/                       # Schéma en étoile Gold (dim_*, fct_*)
│   │       ├── dim_customers.sql
│   │       ├── dim_products.sql
│   │       ├── fct_orders.sql
│   │       └── schema.yml               # Tests automatisés (unique, not_null, relationships)
│   ├── dbt_project.yml                  # Configuration du projet dbt
│   └── profiles.yml                     # Profil de connexion PostgreSQL
├── lakehouse/
│   ├── bronze/                          # Couche Bronze brute au format Parquet
│   └── silver/                          # Couche Silver avec journaux Delta Lake (_delta_log)
├── ml_jobs/
│   ├── predict_delays.py                # Modèle Random Forest pour la Supply Chain
│   └── predict_churn.py                 # Clustering K-Means & Scoring Churn Marketing
├── screenshots/                         # Captures d'écran des tableaux de bord Power BI
├── spark_jobs/
│   ├── ingest_bronze.py                 # Ingestion PySpark CSV -> Parquet
│   ├── process_silver.py                # Traitement PySpark -> Delta Lake
│   └── load_to_postgres.py              # Chargement optimisé DeltaTable -> PostgreSQL
├── docker-compose.yml                   # Déploiement du conteneur PostgreSQL persistant
├── run_pipeline.py                      # Orchestrateur global exécutable en 1 commande
├── ecommerce_lakehouse_analytics.pbix   # Fichier de rapport Power BI Desktop
└── README.md

🚀 Guide de Reproduction Locale

1. Prérequis

  - Système : Windows 10/11 ou Linux / macOS
  - Docker Desktop opérationnel
  - Python : Version 3.11+
  - Java : JDK 17 (pour PySpark)

2. Démarrer l'infrastructure

docker compose up -d

3. Configurer l'environnement Python

# Création et activation de l'environnement virtuel
python -m venv venv
venv\Scripts\activate   # Sur Windows (ou source venv/bin/activate sur Linux)

# Installation des dépendances
pip install pyspark delta-spark deltalake dbt-postgres scikit-learn pandas pyarrow sqlalchemy psycopg2-binary

4. Lancer le pipeline complet

Une seule commande exécute l'ensemble du cycle (Ingestion, Delta Lake,
Chargement, dbt, Tests et Modèles ML) :

python run_pipeline.py

5. Visualiser dans Power BI

Ouvrez le fichier ecommerce_lakehouse_analytics.pbix dans Power BI Desktop.
La connexion pointe automatiquement vers votre base locale PostgreSQL
localhost:5432 (ecommerce_dw).

👨‍💻 Auteur

Omar Ben Amara
Ingénieur BI & Big Data 

  - LinkedIn
  - Email : omarbenamara1919@gmail.com


---


