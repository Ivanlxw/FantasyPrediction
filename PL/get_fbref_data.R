library(worldfootballR)
library(polars)

idx = 0
szn_end_year = 2024
league_url <- fb_league_urls(country = "ENG", gender = "M", 
                             season_end_year = szn_end_year, tier = '1st')
team_stats_list = fb_teams_urls(league_url)
stat_type <- 'summary'

player_log_fp = paste(
  "/mnt/HDD/Data/FantasyPrediction/PL/fbref_player_logs_", 
  stat_type, 
  ".csv", 
  sep=""
)

for (szn_team_url in team_stats_list) {
    player_stat_df_list = c()
    player_urls = fb_player_urls(szn_team_url)
    for (player_url in player_urls) {
        player_stat_df <- tryCatch(
          {
            pl$DataFrame(fb_player_match_logs(
              player_url, season_end_year=szn_end_year, stat_type=stat_type))
          },
          error=function(cond) {
            return(c())
          }
        )
        if (!is.null(player_stat_df)) {
          player_stat_df_list <- c(player_stat_df_list, player_stat_df$fill_null(NaN))
        }
        Sys.sleep(4)
    }
    filtered_player_stat_df_list <- c()
    for (df in player_stat_df_list) {
      if (df$shape[2] != 38) {
        next
      }
      filtered_player_stat_df_list <- c(filtered_player_stat_df_list, df)
    }
    # write_df$fill_null(NaN)
    read_df = pl$read_csv(player_log_fp)$fill_null(NaN)
    write_df <- pl$concat(c(read_df, pl$concat(filtered_player_stat_df_list)))$unique()
    write.csv(write_df, player_log_fp, row.names=FALSE, na="")
    print(paste("Written player url's csv for", szn_team_url))
}
