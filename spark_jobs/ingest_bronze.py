from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
import os

def create_spark_session():
    """Initialise le moteur Spark en local."""
    return SparkSession.builder \
        .appName("ECommerce_Bronze_Ingestion") \
        .master("local[*]") \
        .getOrCreate()

def ingest_csv_to_bronze(spark, table_name, input_path, output_path):
    print(f"--> Ingestion de la table : {table_name}...")
    
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    df_bronze = df_raw.withColumn("ingestion_timestamp", current_timestamp())
    
    destination = os.path.join(output_path, table_name)
    df_bronze.write \
        .mode("overwrite") \
        .parquet(destination)
    
    print(f"OK : {table_name} enregistrée dans {destination} (Lignes : {df_bronze.count()})")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_dir = "data/raw"
    bronze_dir = "lakehouse/bronze"

    tables = {
        "orders": f"{raw_dir}/olist_orders_dataset.csv",
        "order_items": f"{raw_dir}/olist_order_items_dataset.csv",
        "customers": f"{raw_dir}/olist_customers_dataset.csv",
        "products": f"{raw_dir}/olist_products_dataset.csv",
        "payments": f"{raw_dir}/olist_order_payments_dataset.csv"
    }

    print("================ DÉMARRAGE COUCHE BRONZE ================")
    for table_name, file_path in tables.items():
        if os.path.exists(file_path):
            ingest_csv_to_bronze(spark, table_name, file_path, bronze_dir)
        else:
            print(f"ATTENTION : Le fichier {file_path} est introuvable !")
            
    print("================ COUCHE BRONZE TERMINÉE ================")
    spark.stop()

if __name__ == "__main__":
    main()