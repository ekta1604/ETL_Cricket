import mysql.connector

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    port=3307,
    password='root',
    database='IPL_data'
)
cursor = conn.cursor()

table_name = '335985'  # Replace with the actual table name

select_query = f"SELECT * FROM `{table_name}`"
cursor.execute(select_query)
rows = cursor.fetchall()

for row in rows:
    print(row)

cursor.close()
conn.close()
