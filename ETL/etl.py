import os
import glob
from datetime import datetime
import json
import csv
import math
import shutil
import pandas as pd

# Read the JSON config file
with open('config.json') as f:
    config_data = json.load(f)

# Get the paths from the 'paths' section of the config file and normalize them
raw_files_path = os.path.normpath(config_data['paths']['raw_files_path'])
processed_files_path = os.path.normpath(config_data['paths']['processed_files_path'])
status_file_path = os.path.normpath(config_data['paths']['status_file_path'])
processed_files = set()

# populate the processed_files set with existing CSV files
for filename in os.listdir(processed_files_path):
    if filename.endswith('.csv'):
        processed_files.add(filename[:-4])

# load the existing status.csv file if it exists, or create a new dataframe if it doesn't
if os.path.exists(status_file_path):
    # check if the file is empty
    if os.path.getsize(status_file_path) > 0:
        status_df = pd.read_csv(status_file_path)
    else:
        status_df = pd.DataFrame(columns=["file name", "process time"])
else:
    status_df = pd.DataFrame(columns=["file name", "process time"])

# get a list of all the unprocessed JSON files in the raw_files directory
unprocessed_files = glob.glob(os.path.join(raw_files_path, "*.json"))
unprocessed_files = [f for f in unprocessed_files if os.path.basename(f) not in status_df["file name"].tolist()]

# Sort the unprocessed files based on their numerical order
unprocessed_files.sort(key=lambda x: int(os.path.basename(x).split('.')[0]))

# process each unprocessed file and update the status dataframe
previous_match_number = None
previous_city = None
previous_match = None
for file_path in unprocessed_files:
    filename = os.path.basename(file_path)

    with open(file_path) as f:
        data = json.load(f)

    # Extract relevant information
    if 'match_number' in data['info']['event']:
        match_number = data['info']['event']['match_number']
    else:
        # Assign the next sequential match number based on the previous match number
        if previous_match_number is not None:
            match_number = previous_match_number + 1
        else:
            match_number = None

    # Extract relevant information
    if 'city' in data['info']:
        city = data['info']['city']
    else:
        # Assign the next sequential city based on the previous city
        if previous_city is not None:
            city = previous_city + 1
        else:
            city = None

    season = data['info']['season']
    toss_winner = data['info']['toss']['winner']
    toss_decision = data['info']['toss']['decision']
    teams = ','.join(data['info']['teams'])
    venue = data['info']['venue']
    gender = data['info']['gender']
    umpires = ','.join(data['info']['officials']['umpires'])
    result = data['info']['outcome'].get('result')
    player_of_match = ','.join(data['info']['player_of_match'])
    match_type = data['info']['match_type']

    rows = []
    ball_no = 0  # initialize ball number
    over_no = 0  # initialize over number
    for inning in data['innings']:
        for over in inning['overs']:
            for delivery in over['deliveries']:
                extras = delivery.get('extras', {})
                wickets = delivery.get('wickets', [])

                ball_no += 1  # increment ball number by 1 for each delivery
                if ball_no % 6 == 1:
                    over_no += 1
                    ball_no = 1  # reset ball number to 1 after every 6 balls
                if over_no <= 20:
                    over_no_str = str(over_no)
                else:
                    over_no_str = str(int(over_no) - 20) + '.' + '0' * (1 - math.floor(math.log10(int(over_no) - 19)))

                if wickets:
                    wicket = wickets[0]
                    kind = wicket.get('kind', '-')
                    player_out = wicket.get('player_out', '-')
                    fielders = [fielder['name'] for fielder in wicket.get('fielders', [])]
                else:
                    kind = '-'
                    player_out = '-'
                    fielders = []
                fielder_list = '-'.join(fielders)

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
                    'runs_extras': sum(extras.values()),
                    'legbyes': extras.get('legbyes', 0),
                    'wides': extras.get('wides', 0),
                    'noballs': extras.get('noballs', 0),
                    'byes': extras.get('byes', 0),
                    'kind': kind,
                    'player_out': player_out,
                    'fielders': fielder_list,
                    'runs_total': delivery['runs']['total'],
                }
                rows.append(row)

    # Write the flattened data to a CSV file
    csv_filepath = os.path.join(raw_files_path, filename[:-5] + '.csv')
    print('Creating CSV file:', csv_filepath)
    with open(csv_filepath, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    # Move the CSV file to the processed_files folder
    shutil.move(csv_filepath, os.path.join(processed_files_path, filename[:-5] + '.csv'))

    # Update the set of processed files
    processed_files.add(filename[:-5])

    # Get the current date and time
    processtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Add the file and process time to the status dataframe
    status_df = status_df.append({"file name": filename, "process time": processtime}, ignore_index=True)

    # Update the previous match number and city for the next iteration
    previous_match_number = match_number
    previous_city = city

# Save the updated status dataframe to the status.csv file
status_df.to_csv(status_file_path, index=False)
