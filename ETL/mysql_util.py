import mysql.connector
import os
import glob
import csv
import json

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
        cursor.execute(f"CREATE DATABASE {database_name}")
        print(f"Database '{database_name}' created successfully")
    except mysql.connector.errors.DatabaseError as e:
        if "database exists" in str(e):
            print(f"Database '{database_name}' already exists")
        else:
            raise e

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
)
"""

    cursor.execute(create_table_query)
    conn.commit()
    print(f"Table '{table_name}' created successfully")

def insert_data(table_name, csv_file):
    # Code to insert data into the table...
    conn = mysql.connector.connect(**connection_config)
    cursor = conn.cursor()
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        next(csv_reader)  # Skip the header row
        for row in csv_reader:
            try:
                # Convert string values to appropriate types if needed
                match_number = int(row[0])
                season = row[1]
                city = row[2]
                venue = row[3]
                gender = row[4]
                match_type = row[5]
                toss_winner = row[6]
                toss_decision = row[7]
                teams = row[8]
                team = row[9]
                over = int(float(row[10]))
                ballno = int(float(row[11]))
                batter = row[12]
                bowler = row[13]
                non_striker = row[14]
                runs_batter = int(float(row[15]))
                runs_extras = int(float(row[16]))
                runs_total = int(float(row[17]))
                legbyes = int(float(row[18]))
                wides = int(float(row[19]))
                noballs = int(float(row[20]))
                byes = int(float(row[21]))

                insert_query = f"INSERT INTO `{table_name}` VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
              
                values = (match_number, season, city, venue, gender, match_type, toss_winner, toss_decision, teams,
                          team, over, ballno, batter, bowler, non_striker, runs_batter, runs_extras, runs_total,
                          legbyes, wides, noballs, byes)
                cursor.execute(insert_query, values)
                #print(f"Inserted row: {values}")
            except Exception as e:
                #print(f"Error inserting row: {values}")
                print(f"Error: {e}")

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

# Process each CSV file and create tables
for csv_file in csv_files:
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    
    with open(csv_file, 'r') as f:
        csv_reader = csv.reader(f)
        column_names = next(csv_reader)
        create_table(table_name, column_names)
        insert_data(table_name, csv_file)
