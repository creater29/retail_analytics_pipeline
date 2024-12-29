from sqlalchemy import create_engine

def load_data(dataframe, db_uri, table_name):
    if dataframe.empty:
        raise Exception("Dataframe is empty. No data to load.")
    try:
        engine = create_engine(db_uri)
        with engine.connect() as connection:
            dataframe.to_sql(table_name, connection, if_exists='append', index=False)
    except Exception as e:
        raise Exception(f"Error loading data: {e}")
