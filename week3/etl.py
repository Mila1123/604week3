import pandas as pd
import sqlite3


customers_df = pd.read_csv('customer.csv')
orders_df = pd.read_csv('orders.csv')


merged_df = pd.merge(orders_df, customers_df, on='CustomerID', how='inner')

#Merge order data with customer data.
merged_df['TotalAmount'] = merged_df['Quantity'] * merged_df['Price']

#Calculate the total amount for each order.
merged_df['Status'] = merged_df['OrderDate'].apply(lambda d: 'New' if d.startswith('2025-03') else 'Old')

#Label the status of each order based on the order date.
high_value_orders = merged_df[merged_df['TotalAmount'] > 5000]

#Filter out high-value orders with a total amount greater than 5000.
conn = sqlite3.connect('ecommerce.db')


create_table_query = '''
CREATE TABLE IF NOT EXISTS HighValueOrders (
    OrderID INTEGER,
    CustomerID INTEGER,
    Name TEXT,
    Email TEXT,
    Product TEXT,
    Quantity INTEGER,
    Price REAL,
    OrderDate TEXT,
    TotalAmount REAL,
    Status TEXT
)
'''
conn.execute(create_table_query)


high_value_orders.to_sql('HighValueOrders', conn, if_exists='replace', index=False)

#Retrieve all data from the database table HighValueOrders using an SQL query statement.Extract the query results row by row and print them to the console.
result = conn.execute('SELECT * FROM HighValueOrders')
for row in result.fetchall():
    print(row)


conn.close()

print("ETL process completed successfully!")
