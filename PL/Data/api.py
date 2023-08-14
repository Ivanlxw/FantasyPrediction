import requests

class PLAPIGetter:
    API_BASE_URL = "https://fantasy.premierleague.com/api/"
    _instances = {}
    def __new__(class_, *args, **kwargs):
        if class_ not in class_._instances:
            class_._instances[class_] = super(PLAPIGetter, class_).__new__(class_, *args, **kwargs)
        return class_._instances[class_]

    def get_static_data(self,):
        res = requests.get(self.API_BASE_URL + "bootstrap-static")
        if res.ok:
            return res.json()
        raise Exception(f"Error when getting api call: {res}")

    def get_events(self):
        return self.get_static_data()['events']
    
    def get_fixtures(self):
        res = requests.get(self.API_BASE_URL + "fixtures")
        if res.ok:
            return res.json()
        raise Exception(f"Error when getting api call: {res}")

    def get_latest_player_info(self):
        return self.get_static_data()['elements']
    
    def _get_player_info_all(self, player_id):
        res = requests.get(self.API_BASE_URL + f"element-summary/{player_id}/")
        if res.ok:
            return res.json()
        raise Exception("Cannot get player season info")
    
    def get_player_season_info(self, player_id):
        past_szn_stats = self._get_player_info_all(player_id)['history_past']
        for d in past_szn_stats:
            d['player_id'] = player_id
        return past_szn_stats
    
    def get_player_game_logs(self, player_id):
        past_szn_stats = self._get_player_info_all(player_id)['history']
        for d in past_szn_stats:
            d['player_id'] = player_id
        return past_szn_stats

    def get_player_fixtures(self, player_id):
        past_szn_stats = self._get_player_info_all(player_id)['fixtures']
        for d in past_szn_stats:
            d['player_id'] = player_id
        return past_szn_stats
        
            
if __name__ == "__main__":
    pl_getter = PLAPIGetter()
    print(pl_getter.get_fixtures())