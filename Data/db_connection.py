from multiprocessing import Pool
import os

from sqlalchemy import create_engine
import numpy as np
import pandas as pd

import nba_api.stats.endpoints as nba_stats
from nba_api.stats.static import players, teams
from prediction.utils import get_season_id


def get_connection():
    db = create_engine(os.environ["DB_URL"])
    return db.connect()


def get_team_df():
    return pd.read_sql("select * from nba.teams", get_connection())


def update_team_db():
    team_df = pd.DataFrame(teams.get_teams()).set_index("id", drop=True)
    team_df.to_sql("teams", get_connection(), schema="nba", if_exists="replace")


def update_player_db():
    player_df = pd.DataFrame(players.get_players()).set_index("id", drop=True)
    player_df.to_sql("players", get_connection(), schema="nba", if_exists="replace")


def get_player_df():
    sql = "select * from nba.players"
    return pd.read_sql(sql, con=get_connection())


def get_player_info_df():
    return pd.read_sql("select * from nba.player_info", con=get_connection())


def _get_player_info(player_id):
    player_info = nba_stats.commonplayerinfo.CommonPlayerInfo(player_id)
    df = player_info.get_data_frames()[0]
    df["PLAYER_ID"] = player_id
    return df


def update_player_info_df():
    player_info_df = get_player_info_df()
    player_df = get_player_df().query("is_active")
    df = pd.concat(
        [
            _get_player_info(player_id)
            for player_id in player_df.id.unique()
            if player_id not in player_info_df["PLAYER_ID"]
        ]
    )
    df.to_sql("player_info", get_connection(), schema="nba", if_exists="append")


def get_team_roster_df():
    return pd.read_sql("select * from nba.team_roster", get_connection())


def _get_team_roster(team_id, season):
    roster = nba_stats.CommonTeamRoster(team_id, season=get_season_id(season))
    return roster.common_team_roster.get_data_frame()


def update_team_roster():
    team_ids = get_team_roster_df().id.unique()
    args = [(team_id, season) for season in np.arange(2008, 2023) for team_id in team_ids]

    with Pool(8) as p:
        L = p.starmap(_get_team_roster, args)
    team_roster_df = pd.concat(L)

    from_api = "left_only"
    team_roster_df = pd.merge(
        team_roster_df, pd.read_sql("select * from nba.team_roster", get_connection()), indicator=True
    ).query("_merge == @from_api")
    if not team_roster_df.empty:
        team_roster_df.to_sql("team_roster", con=get_connection(), schema="nba", if_exists="append", index=False)


def _get_player_game_logs_df(start_year):
    return nba_stats.PlayerGameLogs(season_nullable=get_season_id(start_year)).player_game_logs.get_data_frame()


def update_box_scores(years: list):
    conn = get_connection()
    box_scores_df = pd.concat([_get_player_game_logs_df(year) for year in years])

    box_score_from_db = pd.read_sql("SELECT * from nba.box_score", conn)
    box_scores_df.astype(box_score_from_db.dtypes)

    unique_box_scores_df = pd.concat([box_scores_df, box_score_from_db]).drop_duplicates(
        subset=["season_year", "player_id", "game_id", "team_id"], keep=False
    )
    rows_affected = unique_box_scores_df.to_sql(
        "box_score", conn, schema="nba", if_exists="append", chunksize=10000, index=False
    )
    print(f"{rows_affected} rows affected")


def get_league_log_df():
    return pd.read_sql("SELECT * from nba.league_game_logs", get_connection())


def update_league_game_log(years: list):
    def _get_league_log_df(szn_id):
        return nba_stats.leaguegamelog.LeagueGameLog(season=szn_id).league_game_log.get_data_frame()

    conn = get_connection()
    league_log_df = pd.concat([_get_league_log_df(get_season_id(year)) for year in years])
    league_log_from_db = get_league_log_df()
    league_log_df.astype(league_log_from_db.dtypes)

    unique_box_scores_df = pd.concat([league_log_df, league_log_from_db]).drop_duplicates(
        subset=["SEASON_ID", "GAME_ID", "TEAM_ID"], keep=False
    )
    rows_affected = unique_box_scores_df.to_sql(
        "league_game_logs", conn, schema="nba", if_exists="append", chunksize=10000, index=False
    )
    print(f"{rows_affected} rows affected")


def _get_game_logs_df(start_year, team_id):
    return nba_stats.TeamGameLogs(
        season_nullable=get_season_id(start_year), team_id_nullable=team_id
    ).team_game_logs.get_data_frame()


def update_team_game_log(years: list):
    teams_df = pd.read_sql("SELECT * FROM nba.teams", conn)
    with Pool(8) as p:
        dfs = p.starmap(_get_game_logs_df, [(year, team_id) for team_id in teams_df.id.unique() for year in years])
    game_logs_df = pd.concat([dfs])

    conn = get_connection()
    game_logs_from_db = pd.read_sql("SELECT * from nba.team_game_logs", conn)
    unique_game_logs_df = pd.concat([game_logs_df, game_logs_from_db]).drop_duplicates(
        subset=["SEASON_YEAR", "TEAM_ID", "GAME_ID"], keep=False
    )
    rows_affected = unique_game_logs_df.to_sql(
        "team_game_logs", conn, schema="nba", if_exists="append", chunksize=10000, index=False
    )
    print(f"{rows_affected} rows affected")
