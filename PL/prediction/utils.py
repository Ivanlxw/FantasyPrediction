def get_season_name(year: int):
    return f'{year}/{(year+1) % 100}'