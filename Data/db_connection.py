import os

from sqlalchemy import create_engine
import pandas as pd

import nba_api.stats.endpoints as nba_stats
from nba_api.stats.static import players, teams
from prediction.utils import get_season_id


def get_connection():
    db = create_engine(os.environ["DB_URL"])
    return db.connect()


def update_team_db():
    team_df = pd.DataFrame(teams.get_teams()).set_index("id", drop=True)
    team_df.to_sql("teams", get_connection(), schema="nba", if_exists="replace")


def update_player_db():
    player_df = pd.DataFrame(players.get_players()).set_index("id", drop=True)
    player_df.to_sql("players", get_connection(), schema="nba", if_exists="replace")


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


def update_league_game_log(years: list):
    def get_league_log_df(szn_id):
        return nba_stats.leaguegamelog.LeagueGameLog(season=szn_id).league_game_log.get_data_frame()

    conn = get_connection()
    league_log_df = pd.concat([get_league_log_df(get_season_id(year)) for year in years])
    league_log_from_db = pd.read_sql("SELECT * from nba.league_game_stats", conn)
    league_log_df.astype(league_log_from_db.dtypes)

    unique_box_scores_df = pd.concat([league_log_df, league_log_from_db]).drop_duplicates(
        subset=["SEASON_ID", "GAME_ID", "TEAM_ID"], keep=False
    )
    rows_affected = unique_box_scores_df.to_sql(
        "league_game_stats", conn, schema="nba", if_exists="append", chunksize=10000, index=False
    )
    print(f"{rows_affected} rows affected")


def _get_game_logs_df(start_year, team_id):
    return nba_stats.TeamGameLogs(
        season_nullable=get_season_id(start_year), team_id_nullable=team_id
    ).team_game_logs.get_data_frame()


def update_team_game_log(years: list):
    conn = get_connection()
    teams_df = pd.read_sql("SELECT * FROM nba.teams", conn)
    game_logs_df = pd.concat(
        [_get_game_logs_df(start_year, team_id) for team_id in teams_df.id.unique() for start_year in years]
    )

    game_logs_from_db = pd.read_sql("SELECT * from nba.team_game_stats", conn)
    unique_game_logs_df = pd.concat([game_logs_df, game_logs_from_db]).drop_duplicates(
        subset=["SEASON_ID", "TEAM_ID", "GAME_ID"], keep=False
    )
    rows_affected = unique_game_logs_df.to_sql(
        "team_game_stats", conn, schema="nba", if_exists="append", chunksize=10000, index=False
    )
    print(f"{rows_affected} rows affected")
