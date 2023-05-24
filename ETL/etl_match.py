import mysql.connector

# Connect to the MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    port=3307,
    database="IPL_data"
)

# Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Set the session mode to allow non-aggregated columns in SELECT clause
cursor.execute("SET SESSION sql_mode = ''")

# Create the match-level summary table
create_table_query = """
CREATE TABLE IF NOT EXISTS match_summary (
  match_id INT AUTO_INCREMENT PRIMARY KEY,
  match_number INT,
  total_runs_scored INT,
  total_wickets_taken INT,
  total_6 INT,
  total_4 INT,
  runs_in_each_over VARCHAR(255)
);
"""

cursor.execute(create_table_query)

# Write the SQL query for calculating match-level statistics
summary_query = """
INSERT INTO match_summary (match_number, total_runs_scored, total_wickets_taken, total_6, total_4, runs_in_each_over)
SELECT
  c.match_number,
  SUM(runs_total) AS total_runs_scored,
  COUNT(player_out) AS total_wickets_taken,
  COUNT(*) AS total_6,
  (SELECT COUNT(*) FROM combined_data WHERE runs_batter = 4 AND match_number = c.match_number) AS total_4,
  GROUP_CONCAT(CONCAT(`over`, '.', ballno, ':', runs_total) ORDER BY `over`, ballno SEPARATOR ' ') AS runs_in_each_over
FROM combined_data c
INNER JOIN (
  SELECT DISTINCT match_number
  FROM combined_data
) m ON c.match_number = m.match_number
GROUP BY c.match_number;
"""


# Execute the SQL query to calculate and store match-level statistics
cursor.execute(summary_query)

# Commit the changes to the database
conn.commit()

# Close the cursor and database connection
cursor.close()
conn.close()
