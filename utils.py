
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import json
from pandas import json_normalize
import os


def CreatePlayersDF(match_data):
    # Extracting player-related information
    players_data = match_data.get("players", [])

    # List of keys to exclude
    include_keys = ["id","number", "gender","birthday","short_name","last_name","first_name"]

    # Remove unwanted keys from each player's data
    filtered_players_data = [{k: v for k, v in player.items() if k in include_keys} for player in players_data]

    # Creating a DataFrame for players
    players_df = pd.DataFrame(filtered_players_data)

    return players_df


def CreatePlayersMatchDF(match_data):
    # Extracting player-related information
    players_data = match_data.get("players", [])

    # List of keys to exclude
    exclude_keys = ["number", "gender","birthday","short_name","last_name","first_name"]

    # Remove unwanted keys from each player's data
    filtered_players_data = [{k: v for k, v in player.items() if k not in exclude_keys} for player in players_data]

    # Creating a DataFrame for players
    players_match_df = pd.DataFrame(filtered_players_data)

    # Extracting 'id' from 'player_role' and renaming the column
    players_match_df['player_role_id'] = players_match_df['player_role'].apply(lambda x: x.get('id') if pd.notna(x) else None)
    players_match_df.drop(columns=['player_role'], inplace=True)

    # Adding 'match_id' column
    players_match_df['match_id'] = match_data.get("id")

    return players_match_df


def CreatePlayerRolesDF(match_data):
    players_data = match_data.get("players", [])
    unique_roles = set()
    # Extract "player_role" keys and flatten the list of dictionaries
    player_role_data = []
    for player in players_data:
        role_key = tuple(player["player_role"].values())
        if role_key not in unique_roles:
            player_role_data.append({f"player_role_{key}": value for key, value in player["player_role"].items()})
            unique_roles.add(role_key)

    # Create a DataFrame from the flattened data
    player_role_df = pd.DataFrame(player_role_data)

    return player_role_df


def RemovePlayersData(match_data):
    # Remove the "players" key
    match_data.pop("players", None)
    return match_data


def CreateFlatMatchData(match_data):

    match_data= RemovePlayersData(match_data)
    # Define a function to flatten nested structures and lists
    def flatten_json(data, parent_key='', sep='_'):
        flat_data = {}
        if isinstance(data, dict):
            for k, v in data.items():
                new_key = f"{parent_key}{sep}{k}" if parent_key else k
                flat_data.update(flatten_json(v, new_key, sep=sep))
        elif isinstance(data, list):
            for i, item in enumerate(data, start=1):
                new_key = f"{parent_key}{sep}{i}" if parent_key else f"{i}"
                flat_data.update(flatten_json(item, new_key, sep=sep))
        else:
            flat_data[parent_key] = data
        return flat_data

    # Flatten the nested JSON structure
    flat_match_data = flatten_json(match_data)

    return flat_match_data


def CreateTeamsDF(flat_match_data, columns_to_execlude):   
    # Create team_df (home_team and away_team)
    teams_df = pd.DataFrame()
    for team_type in ['home_team', 'away_team']:
        team_data = {
            'team_id': flat_match_data[f'{team_type}_id'],
            'name': flat_match_data[f'{team_type}_name'],
            'short_name': flat_match_data[f'{team_type}_short_name'],
            'acronym': flat_match_data[f'{team_type}_acronym']
        }
        columns_to_execlude.append([
            f'{team_type}_name',
            f'{team_type}_short_name', f'{team_type}_acronym'
        ])
        teams_df = pd.concat([teams_df, pd.DataFrame([team_data])])

    return teams_df, columns_to_execlude

def CreateCoachesDF(flat_match_data, columns_to_execlude): 
    
    # Create coaches_df (home_team_coach and away_team_coach)
    coaches_df = pd.DataFrame()
    for team_type in ['home_team', 'away_team']:
        coach_data = {
            'coach_id': flat_match_data[f'{team_type}_coach_id'],
            'first_name': flat_match_data[f'{team_type}_coach_first_name'],
            'last_name': flat_match_data[f'{team_type}_coach_last_name']
        }
        columns_to_execlude.append([
            f'{team_type}_coach_first_name',
            f'{team_type}_coach_last_name'
        ])
        coaches_df = pd.concat([coaches_df, pd.DataFrame([coach_data])])

    return coaches_df, columns_to_execlude 


def CreateKitsDF(flat_match_data, columns_to_execlude): 
     
    # Create kits_df (home_team_kit and away_team_kit)
    kits_df = pd.DataFrame()
    for team_type in ['home_team', 'away_team']:
        kit_data = {
            'kit_id': flat_match_data[f'{team_type}_kit_id'],
            'team_id': flat_match_data[f'{team_type}_id'],
            'season_id': flat_match_data[f'{team_type}_kit_season_id'],
            'kit_name': flat_match_data[f'{team_type}_kit_name'],
            'jersey_color': flat_match_data[f'{team_type}_kit_jersey_color'],
            'number_color': flat_match_data[f'{team_type}_kit_number_color']
        }
        columns_to_execlude.append([
            f'{team_type}_kit_season_id', f'{team_type}_kit_name',
            f'{team_type}_kit_jersey_color', f'{team_type}_kit_number_color'
        ])
        kits_df = pd.concat([kits_df, pd.DataFrame([kit_data])])

    return kits_df, columns_to_execlude

def CreateCompetitionEditionDF(flat_match_data, columns_to_execlude): 
    
    # Create competition_edition_df
    competition_edition_data = {
        'competition_edition_id': flat_match_data['competition_edition_id'],
        'competition_edition_name': flat_match_data['competition_edition_name'],
        'competition_id': flat_match_data['competition_edition_competition_id']
    }
    columns_to_execlude.append([
        'competition_edition_name',
        'competition_edition_season_start_year',
        'competition_edition_season_end_year',
        'competition_edition_season_name'
    ])
    competition_edition_df = pd.DataFrame([competition_edition_data])

    return competition_edition_df, columns_to_execlude 

def CreateSeasonDF(flat_match_data, columns_to_execlude): 
        
    # Create season_df
    season_data = {
        'season_id': flat_match_data['home_team_kit_season_id'],
        'start_year': flat_match_data['home_team_kit_season_start_year'],
        'end_year': flat_match_data['home_team_kit_season_end_year'],
        'name': flat_match_data['home_team_kit_season_name'],
        'season_start_year': flat_match_data['competition_edition_season_start_year'],
        'season_end_year': flat_match_data['competition_edition_season_end_year']
    }
    columns_to_execlude.append([
        'home_team_kit_season_start_year',
        'home_team_kit_season_end_year',
        'home_team_kit_season_name',
        'away_team_kit_season_start_year',
        'away_team_kit_season_end_year',
        'away_team_kit_season_name'
    ])
    season_df = pd.DataFrame([season_data])

    return season_df, columns_to_execlude 

def CreateStadiumDF(flat_match_data, columns_to_execlude): 
    
    # Create stadium_df
    stadium_data = {
        'stadium_id': flat_match_data['stadium_id'],
        'name': flat_match_data['stadium_name'],
        'city': flat_match_data['stadium_city'],
        'capacity': flat_match_data['stadium_capacity']
    }
    columns_to_execlude.append([
        'stadium_name',
        'stadium_city',
        'stadium_capacity'
    ])
    stadium_df = pd.DataFrame([stadium_data])

    return stadium_df, columns_to_execlude 

def CreateCompetitionRoundDF(flat_match_data, columns_to_execlude):
    # Create competition_round_df
    competition_round_data = {
        'competition_round_id': flat_match_data['competition_round_id'],
        'name': flat_match_data['competition_round_name'],
        'round_number': flat_match_data['competition_round_round_number'],
        'potential_overtime': flat_match_data['competition_round_potential_overtime']
        }
    columns_to_execlude.append([
        'competition_round_name',
        'competition_round_round_number',
        'competition_round_potential_overtime'
        ])
    competition_round_df = pd.DataFrame([competition_round_data])

    return competition_round_df, columns_to_execlude 

def CreateCompetitionDF(flat_match_data, columns_to_execlude): 
    
    # Create competition_df
    competition_data = {
        'competition_id': flat_match_data['competition_edition_competition_id'],
        'area': flat_match_data['competition_edition_competition_area'],
        'name': flat_match_data['competition_edition_competition_name']

    }
    columns_to_execlude.append([
        'competition_edition_competition_area',
        'competition_edition_competition_name'
    ])
    competition_df = pd.DataFrame([competition_data])

    return competition_df, columns_to_execlude 

def CreateMatchDF(flat_match_data, columns_to_execlude): 
    # Create match_df
    match_df = pd.DataFrame([flat_match_data])

    exclusion_list = [item for sublist in columns_to_execlude for item in sublist]

    match_ref_columns = [col for col in match_df.columns if col not in exclusion_list]
    match_ref_df = match_df[match_ref_columns]

    return  match_ref_df, exclusion_list


