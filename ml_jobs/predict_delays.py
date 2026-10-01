import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, roc_auc_score

def get_db_connection():
    """Connexion au Data Warehouse PostgreSQL."""
    return create_engine("postgresql+psycopg2://lakehouse_user:lakehouse_password@localhost:5432/ecommerce_dw")

def load_data(engine):
    """Extraction des faits de commandes depuis la couche Gold."""
    query = """
        SELECT 
            order_id,
            total_items_count,
            total_order_amount,
            total_freight_amount,
            is_delayed
        FROM gold.fct_orders
        WHERE is_delayed IS NOT NULL
          AND total_order_amount > 0
    """
    return pd.read_sql(query, engine)

def train_and_predict():
    print("================ DÉMARRAGE ML : PRÉDICTION DES RETARDS ================")
    engine = get_db_connection()
    
    print("--> Chargement des données depuis PostgreSQL (gold.fct_orders)...")
    df = load_data(engine)
    print(f"OK : {len(df)} commandes chargées pour l'entraînement.")

    df["freight_ratio"] = df["total_freight_amount"] / df["total_order_amount"]
    df["is_delayed"] = df["is_delayed"].astype(int)

    features = ["total_items_count", "total_order_amount", "total_freight_amount", "freight_ratio"]
    X = df[features]
    y = df["is_delayed"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

    print("--> Entraînement du Random Forest Classifier...")
    model = RandomForestClassifier(
        n_estimators=100, 
        max_depth=10, 
        class_weight='balanced',  
        random_state=42, 
        n_jobs=-1
    )
    model.fit(X_train, y_train)

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    y_pred = model.predict(X_test)
    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"--> Performance du Modèle - ROC-AUC : {auc:.3f}")
    print("\nRapport de classification sur le jeu de test :")
    print(classification_report(y_test, y_pred, digits=3))

    print("--> Calcul du score de risque sur toutes les commandes...")
    df["delay_risk_score"] = np.round(model.predict_proba(X)[:, 1] * 100, 2)
    df["predicted_delay"] = (df["delay_risk_score"] >= 50).astype(int)
    
    df["risk_category"] = pd.cut(
        df["delay_risk_score"], 
        bins=[-1, 25, 60, 100], 
        labels=["Faible Risque", "Risque Modéré", "Haut Risque"]
    )

    output_df = df[["order_id", "delay_risk_score", "predicted_delay", "risk_category"]]
    print("--> Sauvegarde des prédictions dans PostgreSQL (gold.ml_delivery_risk)...")
    output_df.to_sql("ml_delivery_risk", engine, schema="gold", if_exists="replace", index=False, chunksize=10000)
    print(f"OK : {len(output_df)} prédictions enregistrées avec succès dans 'gold.ml_delivery_risk' !")
    print("================ FIN ML : PRÉDICTION DES RETARDS ================\n")

if __name__ == "__main__":
    train_and_predict()