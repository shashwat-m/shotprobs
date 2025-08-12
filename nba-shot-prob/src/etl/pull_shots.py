from time import sleep
import tqdm as tqdm
import pandas as pd
import duckdb
from nba_api.stats.static import teams as static_teams
from nba_api.stats.static import players as static_players
from nba_api.stats.endpoints import commonteamroster, shotchartdetail
from nba_api.stats.endpoints import commonallplayers
from nba_api.stats.endpoints import leaguedashplayerstats
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import List, Dict


SLEEP = 0.6
SEASON = "2023-24"

def get_team_ids():
    """
    Function to get team IDs from nba-api
    """
    teams = static_teams.get_teams()
    return [t["id"] for t in teams]

def get_team_roster_player_ids(team_id, season):
    sleep(SLEEP) 
    df = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        team_id_nullable=team_id,
        per_mode_detailed="PerGame"
    ).get_data_frames()[0]
    return df["PLAYER_ID"].astype(int).tolist()

def get_team_roster(team_id, season):
    sleep(SLEEP)
    list = [] * len(get_team_roster_player_ids(team_id, season))
    for i in range(get_team_roster_player_ids(team_id, season)):
        list.append()
        

def get_player_name_from_id(player_id, season):
    sleep(SLEEP)
    df = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        per_mode_detailed="PerGame"
    ).get_data_frames()[0]
    row = df.loc[df["PLAYER_ID"] == player_id]
    return None if row.empty else row.iloc[0]["PLAYER_NAME"]

def build_player_dict(season):
    df = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        per_mode_detailed="PerGame"
    ).get_data_frames()[0]
    return dict[zip(df["PLAYER_ID"].astype(int), df("PLAYER_NAME"))]

def get_active_player_ids():
    return [p["id"] for p in static_players.get_active_players()]

def get_player_shots(player_id, season):
    sleep(SLEEP)
    sc = shotchartdetail.ShotChartDetail(
        team_id=0,
        player_id=player_id,
        season_nullable=season,
        season_type_all_star="Regular Season",
        context_measure_simple="FGA",
        timeout=30
    )
    df= sc.get_data_frames()[0]
    df["PLAYER_ID"] = player_id
    df["SEASON"] = season
    return df

def build_player_lookup(season):
    sleep(SLEEP)
    df = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season,
        per_mode_detailed="PerGame"
    ).get_data_frames()[0]

    cols = ["PLAYER_ID", "PLAYER_NAME", "GP"]
    df = df[[c for c in cols if c in df.columns]].copy()
    df = df.dropna(subset=["PLAYER_ID", "PLAYER_NAME"])
    df["PLAYER_NAME"] = df["PLAYER_NAME"].astype(str).str.strip()
    df = (
        df.sort_values(["PLAYER_NAME", "GP"], ascending=[True, False])
          .drop_duplicates(subset=["PLAYER_NAME"], keep="first")
    )

    name_to_id = {name.upper(): int(pid) for name, pid in zip(df["PLAYER_NAME"], df["PLAYER_ID"])}
    id_to_name = {int(pid): name for name, pid in zip(df["PLAYER_NAME"], df["PLAYER_ID"])}

    return name_to_id, id_to_name, df

def find_player_id(name, name_to_id):
    if not name:
        return None
    return name_to_id.get(name.strip().upper())


def search_players(partial: str, player_name_to_id: dict[str, int], limit: int = 10):
    """Substring search (case-insensitive): returns list[(name, id)]."""
    q = partial.strip().upper()
    hits = [(name, pid) for name, pid in player_name_to_id.items() if q in name]
    hits.sort(key=lambda x: (x[0].find(q), x[0]))
    return hits[:limit]

name_to_id, id_to_name, _ = build_player_lookup(SEASON)
print(get_player_shots(find_player_id("Malik Beasley", name_to_id), SEASON))
