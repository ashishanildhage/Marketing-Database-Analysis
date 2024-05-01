import sqlite3
import pandas as pd

try:
    # Connect to SQLite3 db
    conn = sqlite3.connect('Data Engineer_ETL Assignment.db')

    # Results with SQL
    sql_query = """
    SELECT c.customer_id AS Customer, CAST(c.age AS INTEGER) AS Age, i.item_name AS Item, SUM(o.quantity) as Quantity
    FROM Customers c
    JOIN Sales s ON c.customer_id = s.customer_id
    JOIN Orders o ON s.sales_id = o.sales_id
    JOIN Items i ON o.item_id = i.item_id
    WHERE CAST(c.age AS INTEGER) BETWEEN 18 AND 35 AND o.quantity IS NOT NULL
    GROUP BY c.customer_id, c.age, i.item_name
    """
    sql_results = pd.read_sql_query(sql_query, conn)
    sql_filtered_results = sql_results[sql_results['Quantity'] > 0]
    sql_filtered_results.to_csv('marketing_sql.csv', sep=';', index=False, header=["Customer", "Age", "Item", "Quantity"])

except Exception as e:
    print("An error occurred:", e)

finally:
    # Close db connection
    if 'conn' in locals():
        conn.close()
