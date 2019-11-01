#!/usr/bin/env python3

# Adapted from https://github.com/amanthedorkknight/fifa18-all-player-statistics/blob/master/2019/crawler.py

import sys
from time import sleep
from random import randint
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup

import logging,sys
logging.basicConfig(stream=sys.stderr, level=logging.INFO)

# Build a full list of players. Set this to False for a rerun where we've already gathered this information.
get_basic_player_info = True
saved_player_list = 'basic_player_info.json.zip'

# If you want to write blocks of player records to the CSV more or less often,
# change player_block_size. Don't mess with processed_count.
player_block_size = 500
processed_count = 1

output_file = 'data.csv'

# Overwrite an existing output CSV (this switches to 'a' in the loop, to append)
to_csv_mode = 'w'

# Only write a header the first time we write to the CSV.
write_header = True

base_url = "https://sofifa.com/players?offset="
columns = ['ID', 'Name', 'Age', 'Photo', 'Nationality', 'Flag', 'Overall', 'Potential', 'Club', 'Club Logo', 'Value', 'Wage', 'Special']
data = pd.DataFrame(columns = columns)

# Get basic players information for all players
if (get_basic_player_info):
    for offset in range(0, 300):
        url = base_url + str(offset * 61)
        source_code = requests.get(url)
        plain_text = source_code.text
        soup = BeautifulSoup(plain_text, 'html.parser')
        table_body = soup.find('tbody')
        for row in table_body.findAll('tr'):
            td = row.findAll('td')
            picture = td[0].find('img').get('data-src')
            pid = td[0].find('img').get('id')
            nationality = td[1].find('a').get('title')
            flag_img = td[1].find('img').get('data-src')
            name = td[1].findAll('a')[1].text
            logging.info("Retrieving player info for {}, ID {}".format(name,pid))
            age = td[2].text.strip()
            overall = td[3].text.strip()
            potential = td[4].text.strip()
            club = td[5].find('a').text
            club_logo = td[5].find('img').get('data-src')
            value = td[6].text.strip()
            wage = td[7].text.strip()
            special = td[8].text.strip()
            player_data = pd.DataFrame([[pid, name, age, picture, nationality, flag_img, overall, potential, club, club_logo, value, wage, special]])
            player_data.columns = columns
            data = data.append(player_data, ignore_index=True)
    data = data.drop_duplicates()

# Write basic player info, so we don't have to retrieve it again in a rerun.
    data.to_json(saved_player_list,orient='split',index=False)
else:
# Try reading the players list from a saved file
    data = pd.read_json(saved_player_list,orient='split')
    logging.info("Successfully read {}".format(saved_player_list))

logging.info("Player info dataframe shape: {}".format(data.shape))

# 2020 updates: 'Release Clause' and 'DefensiveAwareness' added.
detailed_columns = ['ID', 'Preferred Foot', 'International Reputation', 'Weak Foot', 'Skill Moves', 'Work Rate', 'Body Type', 'Real Face', 'Player Positions', 'Position', 'Jersey Number', 'Joined', 'Loaned From', 'Contract Valid Until', 'Release Clause', 'Height', 'Weight', 'LS', 'ST', 'RS', 'LW', 'LF', 'CF', 'RF', 'RW', 'LAM', 'CAM', 'RAM', 'LM', 'LCM', 'CM', 'RCM', 'RM', 'LWB', 'LDM', 'CDM', 'RDM', 'RWB', 'LB', 'LCB', 'CB', 'RCB', 'RB', 'Crossing', 'Finishing', 'HeadingAccuracy', 'ShortPassing', 'Volleys', 'Dribbling', 'Curve', 'FKAccuracy', 'LongPassing', 'BallControl', 'Acceleration', 'SprintSpeed', 'Agility', 'Reactions', 'Balance', 'ShotPower', 'Jumping', 'Stamina', 'Strength', 'LongShots', 'Aggression', 'Interceptions', 'Positioning', 'Vision', 'Penalties', 'Composure', 'Marking', 'DefensiveAwareness', 'StandingTackle', 'SlidingTackle', 'GKDiving', 'GKHandling', 'GKKicking', 'GKPositioning', 'GKReflexes']
detailed_data = pd.DataFrame(index = range(0, data.count()[0]), columns = detailed_columns)
player_data_url = 'https://sofifa.com/player/'

for id in data.ID:
# It's rude to hammer a website with requests. Let's sleep for some random amount of time to slow things down.
    sleep(randint(1,5))

    skill_map = {}
    logging.info("Retrieving individual info for player ID {}. Processed {}/{}".format(id,processed_count,player_block_size))
    url = player_data_url + str(id)
    try:
        source_code = requests.get(url)
    except:
        logging.info("Unable to retrieve info for player ID {}! Skipping.".format(id))
        continue
    plain_text = source_code.text
    soup = BeautifulSoup(plain_text, 'html.parser')

# Metadata format is: first_name, last_name, flag (returns an empty string), position(s), 'Age', age_number, '('+birth_month, birth_day+',', birth_year+')'
    meta_data = soup.find('div', {'class': 'meta'}).text.split(' ')
    length = len(meta_data)
    flag_space_ind = meta_data.index('')
    age_string_ind = meta_data.index('Age')
# We'll capture all of the positions listed here now. We can decide what to do with them in analysis.
    skill_map['Player Positions'] = str.join(' ',meta_data[flag_space_ind+1:age_string_ind])
    weight = meta_data[length - 1]
    height = meta_data[length - 2].split('\'')[0] + '\'' + meta_data[length - 2].split('\'')[1].split('\"')[0]
    skill_map["Height"] = height
    skill_map['Weight'] = weight

# Players' attributes have been split across a few different classes, so we'll extract values from all of them.
    for col_idx in [5,6]:
        columns = soup.find('div', {'class': 'teams'}).find('div', {'class': 'columns'}).find_all('div', {'class': 'column col-{}'.format(col_idx)})
        for column in columns:
            skills = column.find_all('li')
            for skill in skills:
                if(skill.find('label') != None):
                    label = skill.find('label').text
                    value = skill.text.replace(label, '').strip()
                    skill_map[label] = value

# col-5 information (the team to which a player belongs, and the position they play) may not be populated.
# If it isn't, use the first position listed next to their name as Position.
    if('Position' not in skill_map.keys()):
        skill_map['Position'] = meta_data[3]


# Position scores have been moved outside of the 'article' tag to an 'aside' tag.
# Goalkeepers don't have these stats.
    if (skill_map['Position'] != 'GK'):
        positions = soup.find('aside').find_all('div', class_='columns')
        for position in positions:
            for pos_div in position.find_all('div', class_=re.compile('column col-sm-2 text-center p')):
                my_output = re.split(r'(\d+)',pos_div.text,maxsplit=1)
                my_output[0] = my_output[0][1:]

# There is a bonus awarded to some players. Capture it--we can decide what to do with it in analysis.
                skill_map[my_output[0]] = str.join('',my_output[1:3])

# The rest of the players' attributes have been split into a bunch of classes as well.
# Bonus work, tbd: Capture the class information regarding the players' skill levels in each area--
#  tags look like "bp3-intent-(success|warning|danger)". Could be useful summary info?
    sections = soup.find('article').find_all('div', class_=re.compile('bp3-callout '))[2:]
    for section in sections:
        items = section.find('ul').find_all('li')
        for item in items:
            value = int(re.findall(r'\d+', item.text)[0])
            name = ''.join(re.findall('[a-zA-Z]*', item.text))
            skill_map[str(name)] = value

    detailed_data = detailed_data.append({'ID': id},ignore_index=True)
    for key, value in skill_map.items():
        detailed_data.loc[detailed_data.ID == id, key] = value
    processed_count += 1

# Have we processed a full block of records? If so, merge and write them to the CSV.
    if (processed_count > player_block_size):
        logging.info("Merging player data...")
        full_data = pd.merge(data, detailed_data, how = 'inner', on = 'ID')
        logging.info("Writing player data to CSV...")
        full_data.to_csv(output_file, encoding='utf-8-sig',mode=to_csv_mode,header=write_header)
        processed_count = 1
        to_csv_mode = 'a'
        write_header = False
        detailed_data = pd.DataFrame(index = range(0, data.count()[0]), columns = detailed_columns)

# One final merge and write to cover whatever we haven't yet processed
logging.info("Merging player data...")
full_data = pd.merge(data, detailed_data, how = 'inner', on = 'ID')
logging.info("Writing player data to CSV...")
full_data.to_csv(output_file, encoding='utf-8-sig',mode=to_csv_mode,header=write_header)
