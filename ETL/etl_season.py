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

create_table_query = """
CREATE TABLE IF NOT EXISTS summary_table (
  season_id INT AUTO_INCREMENT PRIMARY KEY,
  season_year INT,
  total_matches_played INT,
  total_runs_scored FLOAT,
  total_wickets_taken INT,
  average_runs_per_match FLOAT,
  average_wickets_per_match FLOAT,
  most_popular_venue VARCHAR(255),
  team_performance_across_season INT,
  total_6 INT,
  total_4 INT,
  max_run_scorer VARCHAR(255),
  max_wicket_taker VARCHAR(10)
);
"""

cursor.execute(create_table_query)

# Write the SQL query for calculating summary statistics
summary_query = """
INSERT INTO summary_table (season_year, total_matches_played, total_runs_scored, total_wickets_taken, average_runs_per_match, average_wickets_per_match, most_popular_venue, team_performance_across_season, total_6, total_4, max_run_scorer, max_wicket_taker)
SELECT
  CAST(SUBSTRING_INDEX(season, '/', 1) AS UNSIGNED),
  COUNT(DISTINCT match_number),
  SUM(runs_total),
  COUNT(player_out),
  SUM(runs_total) / COUNT(DISTINCT match_number),
  COUNT(player_out) / COUNT(DISTINCT match_number),
  (SELECT venue FROM combined_data c GROUP BY venue ORDER BY COUNT(DISTINCT match_number) DESC LIMIT 1),
  COUNT(DISTINCT winner),
  SUM(CASE WHEN runs_batter = 6 THEN 1 ELSE 0 END),
  SUM(CASE WHEN runs_batter = 4 THEN 1 ELSE 0 END),
  (SELECT batter FROM combined_data c GROUP BY batter ORDER BY SUM(runs_total) DESC LIMIT 1),
  (SELECT bowler FROM combined_data c GROUP BY bowler ORDER BY COUNT(player_out) DESC LIMIT 1)
FROM combined_data
GROUP BY season
ON DUPLICATE KEY UPDATE
  season_id = VALUES(season_id),
  season_year = VALUES(season_year),
  total_matches_played = VALUES(total_matches_played),
  total_runs_scored = VALUES(total_runs_scored),
  total_wickets_taken = VALUES(total_wickets_taken),
  average_runs_per_match = VALUES(average_runs_per_match),
  average_wickets_per_match = VALUES(average_wickets_per_match),
  most_popular_venue = VALUES(most_popular_venue),
  team_performance_across_season = VALUES(team_performance_across_season),
  total_6 = VALUES(total_6),
  total_4 = VALUES(total_4),
  max_run_scorer = VALUES(max_run_scorer),
  max_wicket_taker = VALUES(max_wicket_taker);
"""

# Execute the SQL query to calculate and store the summary statistics
cursor.execute(summary_query)

# Commit the changes to the database
conn.commit()

# Close the cursor and database connection
cursor.close()
conn.close()
