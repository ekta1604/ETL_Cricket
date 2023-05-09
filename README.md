### ETL Pipeline for IPL Cricket Data
This Python script extracts data from JSON files containing Indian Premier League (IPL) cricket match data, flattens the data, and saves it to CSV files. It also updates a status.csv file to keep track of the files that have been processed and when they were processed.

### Libraries Used
1. pandas: data manipulation and analysis library
2. os: operating system library
3. glob: pathname pattern matching library
4. datetime: library to work with dates and times
5. json: library to work with JSON data
6. csv: library to work with CSV files
7. math: library to work with mathematical functions
8. shutil: library for file operations

### Variables
1. raw_files_path: path to the raw JSON files directory
2. processed_files_path: path to the processed CSV files directory
3. status_file_path: path to the status.csv file
4. processed_files: a set to store the names of the processed files

### Workflow
1. Initialize the processed_files set by populating it with the names of the existing CSV files in the processed_files_path directory.
2. Load the existing status.csv file if it exists, or create a new dataframe with columns "file name" and "process time" if it doesn't.
3. Get a list of all the unprocessed JSON files in the raw_files_path directory.
For each unprocessed file:
4. Extract the relevant information from the JSON file.
5. Flatten the JSON file and write the flattened data to a CSV file.
6. Move the CSV file to the processed_files_path directory.
7. Add the file name and process time to the status_df dataframe.
8. Save the updated status_df dataframe to the status.csv file.

Overall, this code automates the process of converting JSON files to CSV files and maintains a record of the processed files.