import requests
import pandas as pd


class PLAPIGetter:
    API_BASE_URL = "https://fantasy.premierleague.com/api/"
    _instances = {}

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(PLAPIGetter, cls).__new__(cls, *args, **kwargs)
        return cls.instance

    def get_static_data(
        self,
    ):
        res = requests.get(self.API_BASE_URL + "bootstrap-static")
        if res.ok:
            return res.json()
        raise Exception(f"Error when getting api call: {res}")

    def get_events(self):
        return self.get_static_data()["events"]

    def get_teams(self):
        return self.get_static_data()["teams"]

    def get_fixtures(self):
        res = requests.get(self.API_BASE_URL + "fixtures")
        if res.ok:
            return res.json()
        raise Exception(f"Error when getting api call: {res}")

    def get_latest_player_info(self):
        return self.get_static_data()["elements"]

    def _get_player_info_all(self, player_id):
        res = requests.get(self.API_BASE_URL + f"element-summary/{player_id}/")
        if res.ok:
            return res.json()
        raise Exception("Cannot get player season info")

    def get_player_season_info(self, player_id):
        past_szn_stats = self._get_player_info_all(player_id)["history_past"]
        for d in past_szn_stats:
            d["player_id"] = player_id
        return past_szn_stats

    def get_player_game_logs(self, player_id):
        past_szn_stats = self._get_player_info_all(player_id)["history"]
        for d in past_szn_stats:
            d["player_id"] = player_id
        return past_szn_stats

    def get_player_fixtures(self, player_id):
        # TODO: multi threading
        past_szn_stats = self._get_player_info_all(player_id)["fixtures"]
        for d in past_szn_stats:
            d["player_id"] = player_id
        return past_szn_stats
    
    def get_player_next_fixtures(self):
        player_fixtures = []
        for player_info in self.get_latest_player_info():
            player_id = player_info["id"]
            player_dict = self.get_player_fixtures(player_id)[0]
            player_fixtures.append(player_dict)
        return pd.DataFrame(player_fixtures)


if __name__ == "__main__":
    pl_getter = PLAPIGetter()
    print(pl_getter.get_fixtures())
