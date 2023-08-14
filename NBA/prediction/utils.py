import os

#### CONSTANTS ####


#### FUNCTIONS ####
def get_season_id(start_year:int):
    return f"{start_year}-{(start_year + 1) % 100}"