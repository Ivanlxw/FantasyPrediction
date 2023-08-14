import pandas as pd

from NBA.Data.db_connection import get_connection 
from PL.Data.api import PLAPIGetter
from PL.prediction.utils import get_season_name

def get_team_staticinfo_df():
    return pd.read_sql('select * from "PL".team_staticinfo', con=get_connection())

def get_team_mapping():
    team_df = get_team_staticinfo_df()
    team_mapping = team_df[['id', 'short_name']].set_index('id',drop=True).to_dict()['short_name']
    return team_mapping

def update_fixtures():
    # TODO: Update after seeing how gameweek 1 data is like. Gameweek 0 is on db right now
    pass

def get_events_df():
    return pd.read_sql('select * from "PL".fixtures', con=get_connection())

def get_past_season_stats_df():
    return pd.read_sql('select * from "PL".season_stats', con=get_connection())


def get_gameweek_info():
    gameweek_info_df = pd.read_sql('select * from "PL".gameweek_info', con=get_connection())
    gameweek_info_df['deadline_time'] = pd.to_datetime(gameweek_info_df['deadline_time'])
    return gameweek_info_df

def update_gameweek_info(curr_season_year):
    static_data = PLAPIGetter().get_static_data()
    gameweek_info = pd.DataFrame(static_data['events'])[['name', 'deadline_time']]
    gameweek_info['season_name'] = get_season_name(curr_season_year)
    new_info = pd.concat([gameweek_info, get_gameweek_info()]).drop_duplicates()
    new_info.to_sql("gameweek_info", con=get_connection(), schema="PL", if_exists="append", index=False)

def get_player_fpl_logs_df():
    return pd.read_sql('select * from "PL".fpl_logs', con=get_connection())

def update_player_fpl_logs_df():
    getter = PLAPIGetter()
    player_fpl_df = pd.DataFrame(getter.get_static_data()['elements'])
    player_fpl_df['deadline_time'] = pd.Timestamp.utcnow()
    player_fpl_df = pd.merge_asof(player_fpl_df, get_gameweek_info(), on=['deadline_time'], direction='forward')
    new_player_fpl_df = pd.concat([player_fpl_df, get_player_fpl_logs_df()]).drop_duplicates()
    new_player_fpl_df.to_sql("fpl_logs", con=get_connection(), schema="PL", if_exists="replace", index=False)


def update_past_season_stats():
    player_fpl_df = get_player_fpl_logs_df()
    unique_player_ids = player_fpl_df.id.unique()
    season_info = []
    getter = PLAPIGetter()
    for player_id in unique_player_ids:
        season_info.extend(getter.get_player_season_info(player_id))
    season_info = pd.DataFrame(season_info)
    new_szn_info = pd.concat([season_info, get_past_season_stats_df()]).drop_duplicates()
    new_szn_info.to_sql("season_stats", con=get_connection(), schema="PL", if_exists="replace", index=False)

def update_team_staticinfo():
    team_df = pd.DataFrame(PLAPIGetter().get_teams())
    team_df['deadline_time'] = pd.Timestamp.utcnow()
    team_df = pd.merge_asof(team_df, get_events_df(), on=['deadline_time'], direction="forward")
    team_df.to_sql("team_staticinfo", con=get_connection(), schema="PL", index=False, if_exists="append")


def update_fixtures():
    fixtures_df = pd.DataFrame(PLAPIGetter().get_fixtures())
    fixtures_df.to_sql("fixtures", schema="PL", con=get_connection())


def get_fixtures_df():
    return pd.read_sql('SELECT * FROM "PL".fixtures')

def update_fixtures_df():
    fixtures_df = pd.DataFrame(PLAPIGetter().get_fixtures())
    from_api = "left_only"
    unique_fixtures_df = pd.merge(fixtures_df, get_fixtures_df(), indicator=True).query("_merge == @from_api")
    if not unique_fixtures_df.empty:
        unique_fixtures_df.to_sql("fixtures", con=get_connection(), schema="PL", index=False, if_exists="append")

