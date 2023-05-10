import pandas as pd
import os
import glob
from datetime import datetime
import json
import csv
import math
import shutil

# Read the JSON config file
with open('config.json') as f:
    config_data = json.load(f)

# Get the paths from the 'paths' section of the config file
paths = config_data['paths']
raw_files_path = paths['raw_files_path']
processed_files_path = paths['processed_files_path']
status_file_path = paths['status_file_path']

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

# get a list of all the unprocessed json files in the raw_files directory
unprocessed_files = glob.glob(os.path.join(raw_files_path, "*.json"))
unprocessed_files = [f for f in unprocessed_files if os.path.basename(f) not in status_df["file name"].tolist()]

# process each unprocessed file and update the status dataframe
for file_path in unprocessed_files:
        filename = os.path.basename(file_path)
    
        with open(os.path.join(raw_files_path, filename)) as f:
            data = json.load(f)

        # Extract relevant information
        match_number = data['info']['event']['match_number']
        season = data['info']['season']
        toss_winner = data['info']['toss']['winner']
        toss_decision = data['info']['toss']['decision']
        teams = ','.join(data['info']['teams'])
        city = data['info']['city']
        venue = data['info']['venue']
        gender = data['info']['gender']
        match_type = data['info']['match_type']

        # Flatten the JSON file
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
                    if over_no <= 20:
                        over_no_str = str(over_no)
                    else:
                        over_no_str = str(int(over_no) - 20) + '.' + '0'*(1-math.floor(math.log10(int(over_no) - 19)))
                    row = {
                        'match_number': match_number,
                        'season': season,
                        'city': city,
                        'venue': venue,
                        'gender': gender,
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
                        'runs_total': delivery['runs']['total'],
                        'legbyes': extras.get('legbyes', 0),
                        'wides': extras.get('wides', 0),
                        'noballs': extras.get('noballs', 0),
                        'byes': extras.get('byes', 0)
                    }
                    rows.append(row)

        # Write the flattened data to a CSV file
        csv_filepath = os.path.join(raw_files_path, filename[:-5] + '.csv')

        print('Creating CSV file:', csv_filepath)
        with open(csv_filepath, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
            
        # move the CSV file to the processed_files folder
        shutil.move(csv_filepath, os.path.join(processed_files_path, filename[:-5] + '.csv'))

        # update the set of processed files
        processed_files.add(filename[:-5])
         
        # get the current date and time
        processtime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # add the file and process time to the status dataframe
        status_df = status_df.append({"file name": filename, "process time": processtime}, ignore_index=True)

#save the updated status dataframe to the status.csv file
status_df.to_csv(status_file_path, index=False)
            
