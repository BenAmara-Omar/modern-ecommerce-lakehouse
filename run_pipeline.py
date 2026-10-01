import os
import sys
import subprocess
import time

# VERROUILLAGE PERMANENT DES VARIABLES RÉSEAU ET HADOOP
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = f"C:\\hadoop\\bin;{os.environ.get('PATH', '')}"

def run_step(step_name, command):
    print(f"\n========================================================")
    print(f"🚀 ÉTAPE EN COURS : {step_name}")
    print(f"========================================================")
    start_time = time.time()
    
    # Transmission automatique de l'environnement verrouillé aux sous-processus
    result = subprocess.run(command, shell=True, env=os.environ)
    
    elapsed = round(time.time() - start_time, 2)
    if result.returncode != 0:
        print(f"❌ ERREUR sur l'étape '{step_name}'. Arrêt du pipeline.")
        sys.exit(1)
    else:
        print(f"✅ SUCCÈS : {step_name} terminé en {elapsed}s.")

def main():
    print("🌟 DÉMARRAGE DU PIPELINE COMPLET LAKEHOUSE E-COMMERCE 🌟")
    total_start = time.time()

    # 1. Bronze
    run_step("1. Ingestion Bronze (PySpark)", "python spark_jobs/ingest_bronze.py")
    
    # 2. Silver
    run_step("2. Nettoyage Silver (Delta Lake)", "python spark_jobs/process_silver.py")
    
    # 3. Chargement PostgreSQL (avec le lecteur DeltaTable)
    run_step("3. Chargement vers PostgreSQL", "python spark_jobs/load_to_postgres.py")
    
    # 4. dbt run
    run_step("4. Modélisation Schéma en Étoile (dbt run)", "cd dbt_project && dbt run --profiles-dir .")
    
    # 5. dbt test
    run_step("5. Tests Qualité Données (dbt test)", "cd dbt_project && dbt test --profiles-dir .")
    
    # 6. ML Retards
    run_step("6. Machine Learning Retards Livraison", "python ml_jobs/predict_delays.py")
    
    # 7. ML Churn
    run_step("7. Machine Learning Churn Client & RFM", "python ml_jobs/predict_churn.py")

    total_time = round(time.time() - total_start, 2)
    print(f"\n🎉 TOUT LE PIPELINE S'EST EXÉCUTÉ DE BOUT EN BOUT AVEC SUCCÈS EN {total_time}s ! 🎉")

if __name__ == "__main__":
    main()