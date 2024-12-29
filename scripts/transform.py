import pandas as pd

def transform_data(data):
    try:
        data['transaction_date'] = pd.to_datetime(data['transaction_date'])
        data['quantity'] = data['quantity'].astype(int)
        return data
    except Exception as e:
        raise Exception(f"Error transforming data: {e}")
