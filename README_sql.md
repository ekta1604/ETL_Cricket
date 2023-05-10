### Documentation for mysql_util.py file 

#### The script imports the necessary libraries:

1. mysql.connector for connecting to the MySQL database.
2. os for interacting with the operating system.
3. glob for file matching and pattern matching.
4. csv for reading CSV files.

#### The script defines three functions:

1. create_database(database_name): Creates a MySQL database if it does not already exist. It connects to the MySQL server using the provided host, user, port, and password. It executes a SQL query to create the database.

2. create_table(table_name, column_names): Creates a table in the MySQL database. It connects to the MySQL server using the provided host, user, port, password, and database. It generates a valid table name based on the CSV file name. It drops the table if it already exists. It formats the column names and types for the SQL query. It executes a SQL query to create the table.

3. insert_data(table_name, csv_file): Inserts data into the table from a CSV file. It connects to the MySQL server using the provided host, user, port, password, and database. It opens the CSV file and reads its contents using the csv.reader. It skips the header row. It iterates over each row in the CSV file, converting the values to appropriate types if needed. It executes a SQL query to insert the row into the table.

4. The script defines the paths for the raw and processed CSV files.

5. It gets a list of CSV files in the processed_files folder using the glob.glob function.

6. It calls the create_database function to create the IPL_data database.

7. It iterates over each CSV file and performs the following actions:

    1. Extracts the table name from the CSV file name using os.path.basename and os.path.splitext.
    2. Opens the CSV file and reads the column names using csv.reader and next.
    3. Calls the create_table function to create the table in the database.
    4. Calls the insert_data function to insert the data from the CSV file into the table.
    
After processing all CSV files, the script completes.

Note: The script assumes a specific file structure where the raw CSV files are located in the raw_files folder and the processed CSV files are located in the processed_files folder. The script also assumes a specific MySQL server configuration with the host, user, port, and password. You may need to modify these values in the script according to your setup.






