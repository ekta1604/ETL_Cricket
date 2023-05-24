import os
import glob
import csv
import json
import mysql.connector

# Define the connection details
connection_config = {
    'host': 'localhost',
    'user': 'root',
    'port': 3307,
    'password': 'root',
    'database': 'IPL_data'
}

def create_database(database_name):
    """Create a MySQL database if it does not exist."""
    conn = mysql.connector.connect(**connection_config)
    cursor = conn.cursor()

    try:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
        print(f"Database '{database_name}' created successfully")
    except mysql.connector.errors.DatabaseError as e:
        print(f"Error creating database: {e}")

def create_table(table_name, column_names):
    """Create a table in the MySQL database."""
    conn = mysql.connector.connect(**connection_config)
    cursor = conn.cursor()

    # Generate a valid table name based on the CSV file name
    table_name = table_name.replace(".", "_").replace("-", "_").replace(" ", "_")

    # Drop the table if it already exists
    drop_table_query = f"DROP TABLE IF EXISTS `{table_name}`"
    cursor.execute(drop_table_query)

    # Format the column names and types for the SQL query
    formatted_columns = []
    for name in column_names:
        if name == 'season':
            formatted_columns.append('`season` VARCHAR(10)')
        else:
            formatted_columns.append(f'`{name}` VARCHAR(255)')

    create_table_query = f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
    {", ".join(formatted_columns)}
)"""

    cursor.execute(create_table_query)
    conn.commit()
    print(f"Table '{table_name}' created successfully")

def insert_data(table_name, csv_file):
    
    conn = mysql.connector.connect(**connection_config)
    cursor = conn.cursor()
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            try:
                values = tuple(row)
                placeholders = ",".join(["%s"] * len(row))
                insert_query = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                cursor.execute(insert_query, values)
            except Exception as e:
                print(f"Error inserting row: {values} - {e}")

    conn.commit()
    print(f"Data from '{csv_file}' inserted into '{table_name}' successfully")

# Read the JSON config file
with open('config.json') as f:
    config_data = json.load(f)

# Get the paths from the 'paths' section of the config file
paths = config_data['paths']
raw_files_path = paths['raw_files_path']
processed_files_path = paths['processed_files_path']

# Get a list of CSV files in the processed_files folder
csv_files = glob.glob(os.path.join(processed_files_path, "*.csv"))

# Create the IPL_data database
create_database("IPL_data")

# Process all CSV files and create a single table
table_name = "combined_data"
column_names = None

for csv_file in csv_files:
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        if not column_names:
            # Read the column names from the first CSV file
            column_names = next(csv_reader)
            create_table(table_name, column_names)

        insert_data(table_name, csv_file)

