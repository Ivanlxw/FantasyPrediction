from datetime import datetime
import subprocess

from PL.Data.db import update_fixtures, update_gameweek_info, update_past_season_stats, update_player_fpl_player_info_df, update_team_staticinfo


if __name__ == "__main__":
    current_szn_year = datetime.now().year
    update_gameweek_info(current_szn_year)
    update_player_fpl_player_info_df()
    update_fixtures()
    update_past_season_stats()
    update_team_staticinfo()
    subprocess.run("Rscript /mnt/HDD/Ivan/projects/FantasyPrediction/PL/get_fbref_data.R", shell=True, check=True)