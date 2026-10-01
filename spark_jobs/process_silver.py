from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, round as spark_round, 
    upper, trim, datediff, current_timestamp
)
from delta import configure_spark_with_delta_pip
import os

def create_delta_spark_session():
    """
    Initialise Spark avec le moteur Delta Lake.
    Forcé sur l'adresse IP locale 127.0.0.1 pour éviter les conflits réseau Windows / Docker.
    """
    builder = SparkSession.builder \
        .appName("ECommerce_Silver_Processing") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def process_orders(spark, bronze_path, silver_path):
    print("--> Traitement Silver : Orders...")
    df = spark.read.parquet(os.path.join(bronze_path, "orders"))
    
    df_clean = df \
        .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
        .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
        .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
        .withColumn("order_estimated_delivery_date", to_timestamp(col("order_estimated_delivery_date"))) \
        .withColumn("actual_delivery_days", datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))) \
        .withColumn("is_delayed", col("order_delivered_customer_date") > col("order_estimated_delivery_date")) \
        .dropDuplicates(["order_id"]) \
        .withColumn("silver_processed_at", current_timestamp())

    dest = os.path.join(silver_path, "orders")
    df_clean.write.format("delta").mode("overwrite").save(dest)
    print(f"OK : Orders nettoyées en Delta Lake ({df_clean.count()} lignes)")

def process_order_items(spark, bronze_path, silver_path):
    print("--> Traitement Silver : Order Items...")
    df = spark.read.parquet(os.path.join(bronze_path, "order_items"))

    df_clean = df \
        .withColumn("price", spark_round(col("price").cast("double"), 2)) \
        .withColumn("freight_value", spark_round(col("freight_value").cast("double"), 2)) \
        .withColumn("total_item_value", spark_round(col("price") + col("freight_value"), 2)) \
        .withColumn("silver_processed_at", current_timestamp())

    dest = os.path.join(silver_path, "order_items")
    df_clean.write.format("delta").mode("overwrite").save(dest)
    print(f"OK : Order Items nettoyées en Delta Lake ({df_clean.count()} lignes)")

def process_customers(spark, bronze_path, silver_path):
    print("--> Traitement Silver : Customers...")
    df = spark.read.parquet(os.path.join(bronze_path, "customers"))

    df_clean = df \
        .withColumn("customer_city", upper(trim(col("customer_city")))) \
        .withColumn("customer_state", upper(trim(col("customer_state")))) \
        .dropDuplicates(["customer_id"]) \
        .withColumn("silver_processed_at", current_timestamp())

    dest = os.path.join(silver_path, "customers")
    df_clean.write.format("delta").mode("overwrite").save(dest)
    print(f"OK : Customers nettoyées en Delta Lake ({df_clean.count()} lignes)")

def process_products(spark, bronze_path, silver_path):
    print("--> Traitement Silver : Products...")
    df = spark.read.parquet(os.path.join(bronze_path, "products"))

    df_clean = df \
        .na.fill({"product_category_name": "UNKNOWN"}) \
        .dropDuplicates(["product_id"]) \
        .withColumn("silver_processed_at", current_timestamp())

    dest = os.path.join(silver_path, "products")
    df_clean.write.format("delta").mode("overwrite").save(dest)
    print(f"OK : Products nettoyées en Delta Lake ({df_clean.count()} lignes)")

def main():
    spark = create_delta_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_dir = "lakehouse/bronze"
    silver_dir = "lakehouse/silver"

    print("================ DÉMARRAGE COUCHE SILVER (DELTA LAKE) ================")
    process_orders(spark, bronze_dir, silver_dir)
    process_order_items(spark, bronze_dir, silver_dir)
    process_customers(spark, bronze_dir, silver_dir)
    process_products(spark, bronze_dir, silver_dir)
    print("================ COUCHE SILVER TERMINÉE ================")
    
    spark.stop()

if __name__ == "__main__":
    main()