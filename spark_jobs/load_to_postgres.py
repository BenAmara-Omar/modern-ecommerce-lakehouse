import os
from deltalake import DeltaTable
from sqlalchemy import create_engine, inspect, text

def get_postgres_engine():
    """Crée la connexion au conteneur Docker PostgreSQL avec le pilote psycopg2."""
    user = "lakehouse_user"
    password = "lakehouse_password"
    host = "localhost"
    port = "5432"
    db = "ecommerce_dw"
    
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

def load_silver_table(engine, table_name, silver_dir):
    table_path = os.path.join(silver_dir, table_name)
    print(f"--> Chargement de la table Silver '{table_name}' vers PostgreSQL...")

    dt = DeltaTable(table_path)
    df = dt.to_pandas()
    
    target_table = f"silver_{table_name}"
    inspector = inspect(engine)
    
    if inspector.has_table(target_table):
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {target_table};"))
        df.to_sql(target_table, engine, if_exists="append", index=False, chunksize=10000)
    else:
        df.to_sql(target_table, engine, if_exists="replace", index=False, chunksize=10000)

    print(f"OK : Table '{target_table}' insérée avec succès ({len(df)} lignes) !")

def main():
    engine = get_postgres_engine()
    silver_dir = "lakehouse/silver"

    tables = ["orders", "order_items", "customers", "products"]

    print("================ DÉBUT DU CHARGEMENT VERS POSTGRESQL ================")
    for table in tables:
        load_silver_table(engine, table, silver_dir)
    print("================ CHARGEMENT VERS POSTGRESQL TERMINÉ ================")

if __name__ == "__main__":
    main()