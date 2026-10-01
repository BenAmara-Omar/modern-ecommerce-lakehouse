from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Définition des paramètres par défaut du DAG
default_args = {
    'owner': 'omar_ben_amara',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Instanciation du DAG
with DAG(
    dag_id='ecommerce_lakehouse_end_to_end_pipeline',
    default_args=default_args,
    description='Pipeline complet E-Commerce Lakehouse : PySpark, Delta Lake, dbt, PostgreSQL et Machine Learning',
    schedule_interval='@daily',
    catchup=False,
    tags=['lakehouse', 'pyspark', 'delta', 'dbt', 'ml'],
) as dag:

    # 1. Ingestion de la Couche Bronze (PySpark)
    task_bronze_ingestion = BashOperator(
        task_id='ingest_raw_to_bronze_parquet',
        bash_command='python spark_jobs/ingest_bronze.py',
    )

    # 2. Nettoyage et Couche Silver (Delta Lake)
    task_silver_processing = BashOperator(
        task_id='process_bronze_to_silver_delta',
        bash_command='python spark_jobs/process_silver.py',
    )

    # 3. Chargement de la Couche Silver dans PostgreSQL
    task_load_to_postgres = BashOperator(
        task_id='load_silver_delta_to_postgres',
        bash_command='python spark_jobs/load_to_postgres.py',
    )

    # 4. Modélisation de la Couche Gold (dbt run)
    task_dbt_run_gold = BashOperator(
        task_id='dbt_run_star_schema_marts',
        bash_command='cd dbt_project && dbt run --profiles-dir .',
    )

    # 5. Validation de la Qualité des Données (dbt test)
    task_dbt_test_quality = BashOperator(
        task_id='dbt_test_data_integrity',
        bash_command='cd dbt_project && dbt test --profiles-dir .',
    )

    # 6. Machine Learning : Prédiction des retards logistiques
    task_ml_delivery_delays = BashOperator(
        task_id='ml_predict_delivery_delays',
        bash_command='python ml_jobs/predict_delays.py',
    )

    # 7. Machine Learning : Segmentation RFM et Churn client
    task_ml_customer_churn = BashOperator(
        task_id='ml_predict_customer_churn_rfm',
        bash_command='python ml_jobs/predict_churn.py',
    )

    # Définition des dépendances (Le Lineage du pipeline)
    task_bronze_ingestion >> task_silver_processing >> task_load_to_postgres >> task_dbt_run_gold >> task_dbt_test_quality >> [task_ml_delivery_delays, task_ml_customer_churn]