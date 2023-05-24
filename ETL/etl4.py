import json
import csv
import os
import shutil
import math
import pandas as pd
from datetime import datetime

# Read the JSON config file
with open('config.json') as f:
    config_data = json.load(f)

# Get the paths from the 'paths' section of the config file and normalize them
raw_files_path = os.path.normpath(config_data['paths']['raw_files_path'])
processed_files_path = os.path.normpath(config_data['paths']['processed_files_path'])
status_file_path = os.path.normpath(config_data['paths']['status_file_path'])

# Set of processed files
processed_files = set()

# Load the status dataframe from status.csv if it exists
if os.path.exists(status_file_path):
    status_df = pd.read_csv(status_file_path)
    processed_files = set(status_df['file name'].str[:-5])
else:
    status_df = pd.DataFrame(columns=['file name', 'process time'])

# Get the list of JSON files in the raw_files folder
json_files = [f for f in os.listdir(raw_files_path) if f.endswith('.json')]

# Sort the JSON files based on their names
json_files.sort()

# Variables to store previous match number and city
previous_match_number = ''
previous_city = ''

for filename in json_files:
    # Skip files that have already been processed
    if filename[:-5] in processed_files:
        continue

    print('Processing file:', filename)

    # Read the JSON file
    with open(os.path.join(raw_files_path, filename), 'r') as f:
        data = json.load(f)

    # Extract match information
    match_number = data['info']['event']['match_number']
    season = data['info']['season']
    city = data['info']['city']
    venue = data['info']['venue']
    gender = data['info']['gender']
    umpires = ','.join(data['info']['officials']['umpires'])
    result = data['info']['outcome'].get('result', '')
    player_of_match = ','.join(data['info']['player_of_match'])
    match_type = data['info']['match_type']
    toss_winner = data['info']['toss']['winner']
    toss_decision = data['info']['toss']['decision']
    teams = ','.join(data['info']['teams'])

    # Initialize rows list to store flattened data
    rows = []

    ball_no = 0  # initialize ball number
    over_no = 0  # initialize over number

    for inning in data['innings']:
        for over in inning['overs']:
            for delivery in over['deliveries']:
                extras = delivery.get('extras', {})

                ball_no += 1  # increment ball number by 1 for each delivery

                if ball_no % 6 == 1:
                    over_no += 1
                    ball_no = 1  # reset ball number to 1 after every 6 balls

                if over_no <= 20:
                    over_no_str = str(over_no)
                else:
                    over_no_str = str(int(over_no) - 20) + '.' + '0' * (1 - math.floor(math.log10(int(over_no) - 19)))

                row = {
                    'match_number': match_number,
                    'season': season,
                    'city': city,
                    'venue': venue,
                    'gender': gender,
                    'umpires': umpires,
                    'winner': data['info']['outcome'].get('winner', result),
                    'player_of_match': player_of_match,
                    'match_type': match_type,
                    'toss_winner': toss_winner,
                    'toss_decision': toss_decision,
                    'teams': teams,
                    'team': inning['team'],
                    'over': over_no_str,
                    'ballno': ball_no,
                    'batter': delivery['batter'],
                    'bowler': delivery['bowler'],
                    'non_striker': delivery['non_striker'],
                    'runs_batter': delivery['runs']['batter'],
                    'runs_extras': delivery['runs']['extras'],
                    'legbyes': extras.get('legbyes', 0),
                    'wides': extras.get('wides', 0),
                    'noballs': extras.get('noballs', 0),
                    'byes': extras.get('byes', 0),
                    'kind': delivery.get('kind', ''),
                    'player_out': delivery.get('player_out', ''),
                    'runs_total': delivery['runs']['total']
                }
                rows.append(row)

    # Define the CSV file path
    csv_file_path = os.path.join(processed_files_path, filename[:-5] + '.csv')

    # Write the rows to a CSV file
    with open(csv_file_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    # Add the processed file to the set
    processed_files.add(filename[:-5])

    # Get the current date and time
    process_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Add the file name and process time to the status dataframe
    status_df = status_df.append({'file name': filename[:-5], 'process time': process_time}, ignore_index=True)

    # Save the status dataframe to status.csv
    status_df.to_csv(status_file_path, index=False)

    print('Processed file:', filename)
    print('----------------------------------------')

# Move the processed CSV files to a separate folder
if not os.path.exists(processed_files_path):
    os.makedirs(processed_files_path)

for filename in processed_files:
    src_file_path = os.path.join(raw_files_path, filename + '.json')
    dst_file_path = os.path.join(processed_files_path, filename + '.json')
    shutil.move(src_file_path, dst_file_path)

print('All files processed successfully!')
