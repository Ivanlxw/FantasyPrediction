from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import unicodedata
import pandas as pd

from PL.utils.constants import DATA_DIR
from PL.Data.api import PLAPIGetter
from PL.Data.db import get_team_staticinfo_df


EPL = "Premier League"


def _matches(name, fp):
    str_fp = (
        unicodedata.normalize("NFD", str(fp)).encode("ascii", "ignore").decode("utf-8")
    )
    decoded_name = (
        unicodedata.normalize("NFD", name).encode("ascii", "ignore").decode("utf-8")
    )
    return decoded_name in str_fp


####    TRAINING PURPOSES   ####
def get_players_raw_fp(szn):
    return DATA_DIR / f"PL/{szn}/players_raw.csv"


def get_player_info_fpl(player_name, szn):
    player_raw_df = pd.read_csv(get_players_raw_fp(szn))
    team_mapping = (
        get_team_staticinfo_df()[["code", "name"]]
        .set_index("code", drop=True)
        .to_dict()["name"]
    )
    player_raw_df["team"] = player_raw_df["team_code"].map(team_mapping)
    ## TODO: Incomplete
    return player_raw_df[["team", "web_name", "first_name", "second_name"]]


def find_player_gw_info_fp(player_name, szn):
    all_players_gw_info_fp = list((DATA_DIR / "PL").rglob("gw.csv"))
    player_subnames = player_name.split(" ")
    # can improve, probably need to add affiliation to club as well
    player_fp = [
        fp
        for fp in all_players_gw_info_fp
        if all(_matches(name, fp) for name in player_subnames) and szn in str(fp)
    ]
    if len(player_fp) != 1:
        print(f"Unexpected matches for pl_player_fp={player_name} info: {player_fp}")
        return None
    return player_fp[0]


def merge_fbref_with_fpl(player_name, fbref_df, szn):
    PL_COLS = ["Date", "fixture_id", "was_home", "total_points"]
    fp = find_player_gw_info_fp(player_name, szn)
    if fp is None:
        return None

    pl_player_df = pd.read_csv(fp).rename({"fixture": "fixture_id"}, axis=1)
    pl_player_df["Date"] = pd.to_datetime(pl_player_df["kickoff_time"]).dt.date
    return fbref_df.merge(pl_player_df[PL_COLS], on="Date")


def merge_with_pl_fantasy_points_train(fbref_df, szn):
    player_name = fbref_df["Player"].iloc[0]
    merged_df = merge_fbref_with_fpl(player_name, fbref_df, szn)
    if merged_df is None:
        return None
    # merge with fixtures df to get team difficulty
    fixtures_df = pd.read_csv(DATA_DIR / f"PL/{szn}/fixtures.csv")
    fixtures_df = fixtures_df.rename({"id": "fixture_id"}, axis=1)
    FIXTURES_COLS = [
        "Date",
        "team_a",
        "team_h",
        "team_a_difficulty",
        "team_h_difficulty",
        "fixture_id",
    ]
    fixtures_df["Date"] = pd.to_datetime(fixtures_df["kickoff_time"]).dt.date
    merged_df = merged_df.merge(fixtures_df[FIXTURES_COLS], on=["Date", "fixture_id"])
    return merged_df.drop("fixture_id", axis=1)


def get_latest_pl_player_stats_df():
    fb_ref_player_log_df = pd.read_csv(DATA_DIR / "PL/fbref_player_logs_summary.csv")
    fb_ref_player_log_df = fb_ref_player_log_df.query("Comp == @EPL").copy()
    fb_ref_player_log_df["Date"] = pd.to_datetime(fb_ref_player_log_df["Date"]).dt.date
    fb_ref_player_log_df = fb_ref_player_log_df.sort_values("Date")
    return fb_ref_player_log_df


def get_pl_player_df_for_season(season_str):
    """final data to get fpl data for training"""
    fb_ref_player_log_df = get_latest_pl_player_stats_df()
    merged_df_list = []
    season_year_start = int(season_str[:4])
    fbref_season = f"{season_year_start}-{season_year_start+1}"
    for player_name, df in fb_ref_player_log_df.query(
        "Season == @fbref_season"
    ).groupby("Player"):
        merged_df = merge_with_pl_fantasy_points_train(df, season_str)
        # fpl_player_info_df = get_player_info_fpl(player_name, season_str)
        if merged_df is None:
            print(
                f"skipping player_name={player_name} as there are errors in getting fantasy data"
            )
            continue
        merged_df_list.append(merged_df)
    merged_df = pd.concat(merged_df_list).sort_values("Date")
    # for col in ["team_a", "team_h"]:
    #     merged_df[col] = merged_df[col].astype('category')
    merged_df["opponent_difficulty"] = merged_df.apply(
        lambda row: row.team_h_difficulty if row.was_home else row.team_a_difficulty,
        axis=1,
    )
    merged_df["team_difficulty"] = merged_df.apply(
        lambda row: row.team_a_difficulty if row.was_home else row.team_h_difficulty,
        axis=1,
    )
    merged_df.drop(["team_a", "team_h", "team_a_difficulty", "team_h_difficulty"], axis=1, inplace=True)
    return merged_df


####    INTFERENCE PURPOSES    ####
def get_player_next_fixtures():
    ## Currently not used
    FUT_FIXTURES_COLS = [
        "kickoff_time",
        "event_name",
        "is_home",
        "difficulty",
        "player_id",
        "team",
        "opponent",
    ]
    teams_df = get_team_staticinfo_df()
    team_mapping = (
        teams_df[["id", "name"]]
        .set_index("id", drop=True)
        .to_dict()["name"]
    )
    getter = PLAPIGetter()
    fut_fixtures = getter.get_player_next_fixtures()
    fut_fixtures["team"] = fut_fixtures.apply(
        lambda row: row.team_h if row.is_home else row.team_a, axis=1
    )
    fut_fixtures["team"] = fut_fixtures["team"].map(team_mapping)
    fut_fixtures["opponent"] = fut_fixtures.apply(
        lambda row: row.team_a if row.is_home else row.team_h, axis=1
    )
    fut_fixtures["opponent"] = fut_fixtures["opponent"].map(team_mapping)

    player_info = pd.DataFrame(getter.get_latest_player_info())
    player_info["team"] = player_info["team"].map(team_mapping)
    player_info.drop(["code", "minutes"], axis=1, inplace=True)
    merged_df = fut_fixtures.merge(player_info.rename({"id": "player_id"}, axis=1))
    merged_df = merged_df.rename({"difficulty": "opponent_difficulty"}, axis=1)
    teams_difficulty_map = teams_df[["id", "strength"]].set_index("id", drop=True).to_dict()["strength"]
    merged_df["team_difficulty"] = merged_df.apply(
        lambda row: teams_difficulty_map[row.team_h] if row.is_home else teams_difficulty_map[row.team_a], axis=1)
    return merged_df


def get_hist_player_info_from_player_id(row, getter):
    player_id: int = row.id
    player_hist_df = pd.DataFrame(getter.get_player_game_logs(player_id))
    player_hist_df["team"] = row.team
    player_hist_df["team_id"] = player_hist_df["team"].copy()
    # player_hist_df['team_score'] = player_hist_df.transform(lambda row: row.team_h_score if row.was_home else row.team_a_score, axis=1)
    # player_hist_df['opponent_score'] = player_hist_df.transform(lambda row: row.team_a_score if row.was_home else row.team_h_score, axis=1)
    player_hist_df["first_name"] = row.first_name
    player_hist_df["second_name"] = row.second_name
    player_hist_df["web_name"] = row.web_name
    return player_hist_df


def get_fpl_hist_info():
    # for current season
    getter = PLAPIGetter()
    player_info_df = pd.DataFrame(getter.get_latest_player_info())
    assert len(player_info_df) == player_info_df.id.nunique()
    hist_info_df = []
    with ThreadPoolExecutor(8) as exec:
        hist_info_df = [
            exec.submit(get_hist_player_info_from_player_id, row, getter)
            for _, row in player_info_df.iterrows()
        ]
        hist_info_df = [r.result() for r in hist_info_df]
    hist_info_df = pd.concat(hist_info_df)

    team_df = get_team_staticinfo_df()
    team_strength = (
        team_df[["id", "strength"]]
        .set_index("id", drop=True)
        .to_dict()["strength"]
    )
    hist_info_df["opponent_difficulty"] = hist_info_df["opponent_team"].map(team_strength)
    hist_info_df["team_difficulty"] = hist_info_df["team_id"].map(team_strength)

    team_mapping = (
        team_df[["id", "name"]]
        .set_index("id", drop=True)
        .to_dict()["name"]
    )
    hist_info_df["opponent_team"] = hist_info_df["opponent_team"].map(team_mapping)
    hist_info_df["team"] = hist_info_df["team"].map(team_mapping)
    
    hist_info_df["Player1"] = (
        hist_info_df["first_name"] + " " + hist_info_df["second_name"]
    )
    hist_info_df["Player2"] = (
        hist_info_df["first_name"]
        + " "
        + hist_info_df["second_name"].str.split(" ").str[0]
    )
    hist_info_df["Date"] = pd.to_datetime(hist_info_df["kickoff_time"]).dt.date
    return hist_info_df


def get_inference_df():
    """final function to generate raw data for inference"""
    PL_COLS = ["Player", "Date", "element", "team_difficulty", "opponent_difficulty", "total_points"]
    inference_df = get_latest_pl_player_stats_df()
    fpl_hist_info_df = get_fpl_hist_info()

    merged_df1 = inference_df.merge(
        fpl_hist_info_df.rename(dict(Player1="Player"), axis=1).loc[:, PL_COLS],
        on=["Player", "Date"],
    )
    merged_df2 = inference_df.merge(
        fpl_hist_info_df.rename(dict(Player2="Player"), axis=1).loc[:, PL_COLS],
        on=["Player", "Date"],
    )
    merged_df3 = inference_df.merge(
        fpl_hist_info_df.rename(dict(web_name="Player"), axis=1).loc[:, PL_COLS],
        on=["Player", "Date"],
    )

    merged_df1 = merged_df1[[col for col in merged_df1.columns if "name" not in col]]
    merged_df2 = merged_df2[[col for col in merged_df2.columns if "name" not in col]]
    merged_df3 = merged_df3[[col for col in merged_df3.columns if "name" not in col]]
    return (
        pd.concat([merged_df1, merged_df2, merged_df3])
        .rename(dict(element="player_id"), axis=1)
        .drop_duplicates(keep="first")
    )
