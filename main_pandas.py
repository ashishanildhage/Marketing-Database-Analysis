import sqlite3
import pandas as pd

try:
    # Connect to SQLite3 db
    conn = sqlite3.connect('Data Engineer_ETL Assignment.db')

    # Results with Pandas
    customers, sales, orders, items = [pd.read_sql_query(f"SELECT * FROM {table}", conn) for table in ["Customers", "Sales", "Orders", "Items"]]
    merged_data = pd.merge(pd.merge(pd.merge(customers, sales, on='customer_id'), orders, on='sales_id'), items, on='item_id')
    merged_data.dropna(inplace=True)
    pandas_results = merged_data[(merged_data['age'] >= 18) & (merged_data['age'] <= 35) & (merged_data['quantity'].notnull())]
    pandas_results = pandas_results.groupby(['customer_id', 'age', 'item_name']).agg({'quantity': 'sum'}).reset_index()
    pandas_results['quantity'] = pandas_results['quantity'].astype(int) 
    pandas_filtered_results = pandas_results[pandas_results['quantity'] > 0]
    pandas_filtered_results.to_csv('marketing_pandas.csv', sep=';', index=False, header=["Customer", "Age", "Item", "Quantity"])

except Exception as e:
    print("An error occurred:", e)

finally:
    # Close db connection
    if 'conn' in locals():
        conn.close()
