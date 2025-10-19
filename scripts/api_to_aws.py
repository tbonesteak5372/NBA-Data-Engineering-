from nba_api.stats.endpoints import leaguedashplayerstats
import pandas as pd
import json
import boto3
import boto3
import os
from dotenv import load_dotenv
from nba_api.stats.endpoints import commonteamroster
import requests
import time
import numpy as np

def get_player_json():

    endpoint = leaguedashplayerstats.LeagueDashPlayerStats(season='2025-26',
                                                    season_type_all_star='Pre Season') # send an HTTP web request
    
    json_data = endpoint.get_json() # get response as json from endpoint 

    data = json.loads(json_data) # parse json 
    headers = data['resultSets'][0]['headers'] # grab headers
    rows = data['resultSets'][0]['rowSet'] # grab rows

    df = pd.DataFrame(rows, columns=headers) # create df

    return df # return df 



def createtables(df):
    # dim_players
    dim_nba_players = (
        df[["PLAYER_ID", "PLAYER_NAME", "NICKNAME", "AGE"]]
        .drop_duplicates(subset=["PLAYER_ID"])
        .reset_index(drop=True)
    )

    # dim_teams 
    dim_nba_teams = (
        df[["TEAM_ID", "TEAM_ABBREVIATION"]]
        .drop_duplicates(subset=["TEAM_ID"])
        .reset_index(drop=True)
    )

    # fact_player_stats
    fact_nba_player_stats = (
        df[[
            "PLAYER_ID", "TEAM_ID", "W_PCT", "MIN", "FG_PCT",
            "FG3_PCT", "FT_PCT", "REB", "AST", "STL",
            "BLK", "TOV", "PTS", "PLUS_MINUS"
        ]]
        .drop_duplicates(subset=["PLAYER_ID", "TEAM_ID"])
        .reset_index(drop=True)
    )

    fact_nba_player_stats["loaded_at"] = np.nan

    local_dir = os.getcwd()

    fact_nba_player_stats.to_csv(os.path.join(local_dir, 'fact_nba_player_stats.csv'), index=False) # load tables locally, we cannot hardcode paths due to portability constraints
    dim_nba_players.to_csv(os.path.join(local_dir, 'dim_nba_players.csv'), index=False)
    dim_nba_teams.to_csv(os.path.join(local_dir,'dim_nba_teams.csv'), index=False)

    return dim_nba_teams

def get_coach_json(dim_nba_teams):

    """
    This code takes nba_team_ids, puts it in the list. 
    For each team_id in the list, it grabs the row of data and adds.
    Then for each team_id, it grabs row data about the coach and adds it to a list.

    """

    team_ids = dim_nba_teams["TEAM_ID"].unique().tolist()
    all_headers = None 
    all_rows = []

    for team in team_ids:
        while True:  # keep retrying until the call succeeds
            try:
                endpoint = commonteamroster.CommonTeamRoster(
                    team_id=team,
                    season='2025-26',
                    timeout=90   
                )
                data = json.loads(endpoint.get_json())

                for rs in data["resultSets"]:
                    if rs["name"] == "Coaches":
                        headers = rs["headers"]
                        rows = rs["rowSet"]
                        all_rows.extend(rows)
                        if all_headers is None: # only grab headers once
                            all_headers = headers
                        break

                break  # exit while True for this team

            except requests.exceptions.ReadTimeout:
                print(f" Timeout fetching team {team}. Retrying in 10s...")
                time.sleep(10)

            except Exception as e:
                print(f" Error fetching team {team}: {e}. Retrying in 15s...")
                time.sleep(15)

    coach_df = pd.DataFrame(data=all_rows, columns=all_headers)
    return coach_df

def wrangle_coach_df(coach_df):
    head_coach_df = coach_df[coach_df["COACH_TYPE"] == "Head Coach"]
    dim_coach = head_coach_df[["TEAM_ID", "COACH_ID", "FIRST_NAME", "LAST_NAME"]]\
        .drop_duplicates(subset=["COACH_ID", "TEAM_ID"])\
        .reset_index(drop=True)

    current_dir = os.getcwd()
    dim_coach.to_csv(os.path.join(current_dir, 'dim_coaches.csv'), index=False)

    return dim_coach

def load_env():

    load_dotenv()

    aws_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_reg = os.getenv("AWS_REGION")


    session = boto3.Session(
        aws_access_key_id = aws_id,
        aws_secret_access_key = aws_secret,
        region_name = aws_reg
    )

    return session


def upload_files(session):

    load_dotenv()

    s3 = session.client("s3")

    files = ["fact_nba_player_stats.csv", "dim_nba_players.csv", "dim_nba_teams.csv","dim_coaches.csv"]
    
    aws_bucket= os.getenv("AWS_BUCKET_NAME")

    local_dir = os.getcwd()

    try :
        for file in files:

            local_path = os.path.join(local_dir, file)
            s3.upload_file(local_path, aws_bucket, f'raw/{file}')

            print(f"File: {local_path} loaded sucessfully to s3 bucket!")

    except Exception as e:
        print(f"Error loading files: {e}")


if __name__ == "__main__": # pipeline flow
    df = get_player_json()
    dim_nba_teams = createtables(df)
    coach_df = get_coach_json(dim_nba_teams)
    wrangle_coach_df(coach_df)
    session = load_env()
    upload_files(session)
