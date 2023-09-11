import os
from pathlib import Path
import unicodedata
import pandas as pd

from PL.utils.constants import DATA_DIR
from PL.Data.api import PLAPIGetter
from PL.Data.db import get_team_staticinfo_df


def _matches(name, fp):
    str_fp = unicodedata.normalize('NFD', str(fp)).encode('ascii', 'ignore').decode("utf-8")
    decoded_name = unicodedata.normalize('NFD', name).encode('ascii', 'ignore').decode("utf-8")
    return decoded_name in str_fp

def get_players_raw_fp(szn):
    return DATA_DIR / f"PL/{szn}/players_raw.csv"

def get_player_info_fpl(player_name, szn):
    player_raw_df = pd.read_csv(get_players_raw_fp(szn))
    team_mapping = get_team_staticinfo_df()[["code", "name"]].set_index("code", drop=True).to_dict()["name"]
    player_raw_df["team"] = player_raw_df["team_code"].map(team_mapping)
    ## TODO: Incomplete
    return player_raw_df[["team", "web_name", "first_name", "second_name"]]

def find_player_gw_info_fp(player_name, szn):
    all_players_gw_info_fp = list((DATA_DIR / "PL").rglob("gw.csv"))
    player_subnames = player_name.split(" ")
    # can improve, probably need to add affiliation to club as well
    player_fp = [fp for fp in all_players_gw_info_fp if all(_matches(name, fp) for name in player_subnames) 
                and szn in str(fp)]
    if len(player_fp) != 1:
        print(f"Unexpected matches for pl_player_fp={player_name} info: {player_fp}") 
        return None
    return player_fp[0]

def merge_with_pl_fantasy_points(df, szn):
    PL_COLS = ['Date', 'total_points']
    player_name = df['Player'].iloc[0]
    fp = find_player_gw_info_fp(player_name, szn)
    if fp is None:
        return None
    pl_player_df = pd.read_csv(fp)
    pl_player_df['Date'] = pd.to_datetime(pl_player_df['kickoff_time']).dt.date
    return df.merge(pl_player_df[PL_COLS], on='Date')

def get_pl_player_df_for_season(season_str):
    EPL = "Premier League"

    fb_ref_player_log_df = pd.read_csv(DATA_DIR / "PL/fbref_player_logs_summary.csv")
    fb_ref_player_log_df = fb_ref_player_log_df.query("Comp == @EPL").copy()
    fb_ref_player_log_df["Date"] = pd.to_datetime(fb_ref_player_log_df["Date"]).dt.date
    fb_ref_player_log_df = fb_ref_player_log_df.sort_values("Date")

    merged_df_list = []
    season_year_start = int(season_str[:4])
    fbref_season = f'{season_year_start}-{season_year_start+1}'
    for player_name, df in fb_ref_player_log_df.query("Season == @fbref_season").groupby("Player"):
        merged_df = merge_with_pl_fantasy_points(df, season_str)
        fpl_player_info_df = get_player_info_fpl(player_name, season_str)
        if merged_df is None:
            print(f"skipping player_name={player_name} as there are errors in getting fantasy data")
            continue
        merged_df_list.append(merged_df)
    return pd.concat(merged_df_list).sort_values("Date")

def get_next_fixtures_for_model_input():
    FUT_FIXTURES_COLS = ['kickoff_time', 'event_name', 'is_home', 'difficulty', 'player_id', 'team', 'opponent']
    team_mapping = get_team_staticinfo_df()[["id", "name"]].set_index("id", drop=True).to_dict()["name"]
    getter = PLAPIGetter()
    fut_fixtures = getter.get_player_next_fixtures()
    fut_fixtures["team"] = fut_fixtures.apply(lambda row: row.team_h if row.is_home else row.team_a, axis=1)
    fut_fixtures["team"] = fut_fixtures["team"].map(team_mapping)
    fut_fixtures["opponent"] = fut_fixtures.apply(lambda row: row.team_a if row.is_home else row.team_h, axis=1) 
    fut_fixtures["opponent"] = fut_fixtures["opponent"].map(team_mapping)

    player_info = pd.DataFrame(getter.get_latest_player_info())
    player_info["team"] = player_info["team"].map(team_mapping)
    return player_info.rename({'id': 'player_id'}, axis=1).merge(fut_fixtures.loc[:, FUT_FIXTURES_COLS], on=["player_id", "team"]) 
