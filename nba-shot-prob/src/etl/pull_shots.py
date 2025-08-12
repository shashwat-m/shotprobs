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
LIMIT_PLAYERS = 40

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
    return dict(zip(df["PLAYER_ID"].astype(int), df["PLAYER_NAME"]))

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

def main():
    pid2name = build_player_dict(SEASON)
    pids = get_active_player_ids()
    if LIMIT_PLAYERS:
        pids = pids[:LIMIT_PLAYERS]
    print(f"Fetching shots for {len(pids)} players. . .")
    all_parts: List[pd.DataFrame] = []
    for i, pid in enumerate(pids, 1):
        try:
            df = get_player_shots(pid, SEASON)
            if not df.empty:
                pname = pid2name.get(pid,"Unknown")
                df["PLAYER_NAME"] = pname
                all_parts.append(df)
                if i % 25 == 0:
                    print(f". . .{i} players processed")
        except Exception as e:
            print(f"[WARN] Player {pid2name.get(pid, pid)} ({pid}) failed : {e}")
    if not all_parts:
        print("No shots pulled")
        return
    
    shots = pd.concat(all_parts, ignore_index=True)

    # Useful columns to keep
    keep = [
        "GAME_ID","GAME_EVENT_ID","PLAYER_ID","PLAYER_NAME","TEAM_ID","TEAM_NAME",
        "LOC_X","LOC_Y","SHOT_DISTANCE","SHOT_TYPE",
        "SHOT_ZONE_BASIC","SHOT_ZONE_AREA","SHOT_ZONE_RANGE",
        "PERIOD","MINUTES_REMAINING","SECONDS_REMAINING","GAME_DATE",
        "ACTION_TYPE","EVENT_TYPE","HTM","VTM","SHOT_MADE_FLAG","SEASON",
    ]
    shots = shots[[c for c in keep if c in shots.columns]]

    # Persist command
    out_parquet = f"data_raw/shots_{SEASON.replace('-', '')}.parquet"
    shots.to_parquet(out_parquet, index=False)


    # Connect to DuckDB to process data
    con = duckdb.connect("data_proc/nba.duckdb")
    con.execute("CREATE TABLE IF NOT EXISTS shots AS SELECT * FROM read_parquet(?)", [out_parquet])
    con.execute("INSERT INTO shots SELECT * FROM read_parquet(?)", [out_parquet])
    con.close()

    print(f"Saved {len(shots):,} shots to {out_parquet} and DuckDB.")