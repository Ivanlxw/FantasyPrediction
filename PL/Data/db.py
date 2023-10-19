import time
import pandas as pd
import soccerdata as sd

from NBA.Data.db_connection import get_connection
from PL.Data.api import PLAPIGetter
from PL.prediction.utils import get_season_name


def get_team_staticinfo_df():
    return pd.read_sql('select * from fantasy_pl.team_staticinfo', con=get_connection())


def get_team_mapping():
    team_df = get_team_staticinfo_df()
    team_mapping = team_df[["id", "short_name"]].set_index("id", drop=True).to_dict()["short_name"]
    return team_mapping


def update_fixtures():
    # TODO: Update after seeing how gameweek 1 data is like. Gameweek 0 is on db right now
    pass


def get_events_df():
    return pd.read_sql('select * from fantasy_pl.fixtures', con=get_connection())


def get_past_season_stats_df():
    try:
        return pd.read_sql('select * from fantasy_pl.season_stats', con=get_connection())
    except Exception as e:
        print("error from get_past_season_stats_df() :", e)
        return pd.DataFrame()


def get_gameweek_info():
    try:
        gameweek_info_df = pd.read_sql('select * from fantasy_pl.gameweek_info', con=get_connection())
    except Exception as e:
        print("get_gameweek_info() error: ", e)
        return pd.DataFrame()
    if gameweek_info_df.empty:
        return gameweek_info_df
    gameweek_info_df["deadline_time"] = pd.to_datetime(gameweek_info_df["deadline_time"])
    return gameweek_info_df


def update_gameweek_info(curr_season_year):
    static_data = PLAPIGetter().get_static_data()
    gameweek_info = pd.DataFrame(static_data["events"])[["name", "deadline_time"]]
    gameweek_info["season_name"] = get_season_name(curr_season_year)
    gameweek_info["deadline_time"] = pd.to_datetime(gameweek_info["deadline_time"])
    existing_gw_info = get_gameweek_info()
    if not existing_gw_info.empty:
        gameweek_info = pd.concat([gameweek_info, existing_gw_info]).drop_duplicates(keep=False)
    if gameweek_info.empty:
        return
    gameweek_info.to_sql("gameweek_info", con=get_connection(), schema="fantasy_pl", if_exists="replace", index=False)


def get_player_fpl_player_info_df():
    try:
        player_info_df = pd.read_sql('select * from fantasy_pl.fpl_player_info', con=get_connection())
    except Exception as e:
        print("get_gameweek_info() error: ", e)
        return pd.DataFrame()
    return player_info_df


def update_player_fpl_player_info_df():
    getter = PLAPIGetter()
    player_fpl_df = pd.DataFrame(getter.get_latest_player_info())
    player_fpl_df["deadline_time"] = pd.to_datetime(time.time_ns(), unit="ns", utc=True)
    gw_info_df = get_gameweek_info()
    assert not gw_info_df.empty, "gameweek info has to be present to update fpl player info" 

    gw_info_df = gw_info_df.sort_values("deadline_time")
    player_fpl_df = pd.merge_asof(player_fpl_df, gw_info_df, on=["deadline_time"], direction="forward")
    player_fpl_df["deadline_time"] = player_fpl_df["name"].map(gw_info_df.set_index("name")["deadline_time"].to_dict())
    existing_player_fpl_df = get_player_fpl_player_info_df()
    if not existing_player_fpl_df.empty:
        player_fpl_df = pd.concat([player_fpl_df, existing_player_fpl_df]).drop_duplicates(keep=False)
    if player_fpl_df.empty:
        return
    player_fpl_df.to_sql("fpl_player_info", con=get_connection(), schema="fantasy_pl", if_exists="append", index=False)


def update_past_season_stats():
    player_fpl_df = get_player_fpl_player_info_df()
    unique_player_ids = player_fpl_df.id.unique()
    season_info = []
    getter = PLAPIGetter()
    for player_id in unique_player_ids:
        season_info.extend(getter.get_player_season_info(player_id))
    season_info = pd.DataFrame(season_info)
    existing_szn_stats_df = get_past_season_stats_df()
    if not existing_szn_stats_df.empty: 
        season_info = pd.concat([season_info, existing_szn_stats_df]).drop_duplicates(keep=False)
    if season_info.empty:
        return
    season_info.to_sql("season_stats", con=get_connection(), schema="fantasy_pl", if_exists="append", index=False)


def get_team_staticinfo_df():
    return pd.read_sql('SELECT * FROM fantasy_pl.team_staticinfo', con=get_connection())


def update_team_staticinfo():
    team_df = pd.DataFrame(PLAPIGetter().get_teams())
    if team_df.empty:
        return
    team_df.to_sql("team_staticinfo", con=get_connection(), schema="fantasy_pl", index=False, if_exists="replace")


def get_team_fixtures_df():
    try:
        return pd.read_sql('SELECT * FROM fantasy_pl.team_fixtures', con=get_connection())
    except Exception as e:
        print("get_gameweek_info() error: ", e)
        return pd.DataFrame()


def update_fixtures():
    fixtures_df = pd.DataFrame(PLAPIGetter().get_team_fixtures())
    fixtures_df.drop("stats", inplace=True, axis=1)
    existing_fixtures_df = get_team_fixtures_df()
    if not existing_fixtures_df.empty:
        existing_fixtures_df = existing_fixtures_df.loc[:, fixtures_df.columns]
        fixtures_df = pd.concat([fixtures_df, existing_fixtures_df]).drop_duplicates(keep='last')
    if fixtures_df.empty:
        return
    fixtures_df.to_sql("team_fixtures", schema="fantasy_pl", con=get_connection(), if_exists="replace", index=False)


def get_fbref_player_logs(match_type="summary"):
    return pd.read_sql(f'select * from fantasy_pl.fb_ref_player_log_{match_type}', con=get_connection())


def read_match_stats(fbref, match_id, match_type="summary"):
    try:
        match_logs_df = fbref.read_player_match_stats(match_type, match_id=match_id)
    except Exception as e:
        print(f"Could not get df for match_id={match_id}\n{e}")
        return pd.DataFrame()
    return match_logs_df


def get_match_scores_by_player_for_season(league, season_year, match_type="summary"):
    season_id = f"{season_year % 100}-{(season_year+1) % 100}"
    fbref = sd.FBref(leagues=league, seasons=season_id)
    schedule_df = fbref.read_schedule()
    # .query("@INTERESTED_TEAM in home_team or @INTERESTED_TEAM in away_team")
    available_matches = schedule_df.game_id.unique().tolist()
    res = []
    for match_id in available_matches:
        res.append(read_match_stats(fbref, match_id, match_type))
        time.sleep(2)
    club_logs_df = pd.concat(res)
    club_logs_df.columns = [a if b == "" else f"{b}-{a}" for a, b in club_logs_df.columns]
    club_logs_df = club_logs_df.droplevel(["league", "season"])
    return club_logs_df


def update_player_log_from_fb_ref(league, season_year, match_type="summary"):
    df = get_match_scores_by_player_for_season(league, season_year, match_type)
    date_df = pd.to_datetime(df.index.get_level_values(0).str.extract(r"([0-9]{4}-[0-9]{2}-[0-9]{2})*").squeeze())
    df["date"] = date_df.values
    df = df.reset_index()
    df.drop("game", axis=1, inplace=True)

    existing_sql_df = pd.read_sql('select * from fantasy_pl.fb_ref_player_log_summary', con=get_connection())
    sql_df = pd.concat([existing_sql_df, df]).drop_duplicates(subset=["team", "player", "game_id"], keep=False)
    # updating csv is easy, just read_sql and then write to fp
    sql_df.to_sql("fb_ref_player_log_summary", schema="fantasy_pl", con=get_connection(), if_exists="append", index=False)
