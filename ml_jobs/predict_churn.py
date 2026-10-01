import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier

def get_db_connection():
    """Connexion au Data Warehouse PostgreSQL."""
    return create_engine("postgresql+psycopg2://lakehouse_user:lakehouse_password@localhost:5432/ecommerce_dw")

def extract_customer_rfm(engine):
    """Calcule les métriques RFM pour chaque client depuis la couche Gold."""
    query = """
        SELECT 
            c.customer_id,
            c.customer_city,
            c.customer_state,
            COALESCE(c.total_orders, 0) AS frequency,
            COALESCE(SUM(f.total_order_value), 0) AS monetary,
            MAX(f.order_purchase_timestamp) AS last_order_date
        FROM gold.dim_customers c
        LEFT JOIN gold.fct_orders f ON c.customer_id = f.customer_id
        GROUP BY c.customer_id, c.customer_city, c.customer_state, c.total_orders
    """
    df = pd.read_sql(query, engine)
    
    max_date = df["last_order_date"].max()
    df["recency"] = (max_date - df["last_order_date"]).dt.days.fillna(999)
    return df

def run_customer_intelligence():
    print("================ DÉMARRAGE ML : CUSTOMER INTELLIGENCE (RFM & CHURN) ================")
    engine = get_db_connection()

    print("--> Extraction des indicateurs RFM depuis PostgreSQL...")
    df = extract_customer_rfm(engine)
    print(f"OK : {len(df)} profils clients extraits.")

    print("--> Clustering K-Means pour la segmentation comportementale...")
    rfm_features = ["recency", "frequency", "monetary"]
    
    scaler = StandardScaler()
    rfm_scaled = scaler.fit_transform(df[rfm_features])

    kmeans = KMeans(n_clusters=4, random_state=42, n_init=10)
    df["cluster"] = kmeans.fit_predict(rfm_scaled)

    def label_cluster(row):
        if row["recency"] < 120 and row["monetary"] > 150:
            return "VIP / Champions"
        elif row["recency"] < 180:
            return "Clients Actifs"
        elif row["recency"] < 365:
            return "Clients Endormis"
        else:
            return "Clients Perdus (Churned)"

    df["customer_segment"] = df.apply(label_cluster, axis=1)

    df["is_churn"] = (df["recency"] > 180).astype(int)

    X = df[["recency", "frequency", "monetary"]]
    y = df["is_churn"]

    print("--> Entraînement du modèle de scoring du risque d'attrition (Churn)...")
    clf = RandomForestClassifier(n_estimators=50, max_depth=6, random_state=42)
    clf.fit(X, y)

    df["churn_risk_score"] = np.round(clf.predict_proba(X)[:, 1] * 100, 2)

    output_cols = [
        "customer_id", "customer_city", "customer_state",
        "recency", "frequency", "monetary", 
        "customer_segment", "churn_risk_score"
    ]
    output_df = df[output_cols]

    print("--> Sauvegarde des profils enrichis dans PostgreSQL (gold.ml_customer_insights)...")
    output_df.to_sql("ml_customer_insights", engine, schema="gold", if_exists="replace", index=False, chunksize=10000)
    print(f"OK : {len(output_df)} clients enrichis et enregistrés avec succès !")
    print("================ FIN ML : CUSTOMER INTELLIGENCE ================\n")

if __name__ == "__main__":
    run_customer_intelligence()