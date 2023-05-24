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

create_table_query = """
CREATE TABLE IF NOT EXISTS player_stats (
  player_id INT AUTO_INCREMENT PRIMARY KEY,
  player_name VARCHAR(255),
  total_matches_played INT,
  total_runs_scored INT,
  total_wickets_taken INT,
  batting_average FLOAT,
  bowling_average FLOAT,
  best_batsman VARCHAR(255),
  best_bowler VARCHAR(255)
);
"""

cursor.execute(create_table_query)

# Write the SQL query for calculating player statistics
summary_query = """
INSERT INTO player_stats (player_name, total_matches_played, total_runs_scored, total_wickets_taken, batting_average, bowling_average, best_batsman, best_bowler)
SELECT
  CONCAT(batter, ' (Batter)') AS player_name,
  COUNT(DISTINCT match_number),
  SUM(runs_batter),
  NULL AS total_wickets_taken,
  SUM(runs_batter) / COUNT(DISTINCT match_number) AS batting_average,
  NULL AS bowling_average,
  (SELECT CONCAT(batter, ' (Batter)') FROM combined_data GROUP BY batter ORDER BY SUM(runs_batter) DESC LIMIT 1),
  NULL AS best_bowler
FROM combined_data
GROUP BY batter

UNION ALL

SELECT
  CONCAT(bowler, ' (Bowler)') AS player_name,
  (SELECT COUNT(DISTINCT match_number) FROM combined_data WHERE bowler = combined_data.bowler) AS total_matches_played,
  NULL AS total_runs_scored,
  COUNT(player_out) AS total_wickets_taken,
  NULL AS batting_average,
  COUNT(player_out) / (SELECT COUNT(DISTINCT match_number) FROM combined_data) AS bowling_average,
  NULL AS best_batsman,
  (SELECT CONCAT(bowler, ' (Bowler)') FROM combined_data GROUP BY bowler ORDER BY COUNT(player_out) DESC LIMIT 1)
FROM combined_data
WHERE player_out IS NOT NULL
GROUP BY bowler;
"""

# Execute the SQL query to calculate and store player statistics
cursor.execute(summary_query)

# Commit the changes to the database
conn.commit()

# Close the cursor and database connection
cursor.close()
conn.close()
