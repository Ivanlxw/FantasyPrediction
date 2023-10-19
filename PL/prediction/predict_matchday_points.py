import argparse
from difflib import SequenceMatcher
from pathlib import Path

import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.preprocessing import LabelEncoder

from DataSci.features import lag_features, rolling_cols
from PL.Data.api import PLAPIGetter
from PL.utils.constants import DATA_DIR
from PL.Data.db import get_team_staticinfo_df, get_player_fpl_player_info_df
from PL.Data.preprocessing import get_player_next_fixtures, get_inference_df



def similar(a, b):
    # similar naming to find opponent team
    return SequenceMatcher(None, a, b).ratio()


def generate_matchday_data():
    inference_df = get_inference_df()
    team_df = get_team_staticinfo_df()
    TEAM_MAPPING = team_df[["id", "name"]].set_index("id", drop=True).to_dict()['name']
    
    le = LabelEncoder()
    inference_df["Start"] = le.fit_transform(inference_df["Start"])

    numerical_cols = inference_df.select_dtypes(
        include=np.number, exclude=None
    ).columns.to_list()
    col_subset = numerical_cols[2:]
    numerical_cols = inference_df.select_dtypes(
        include=np.number, exclude=None
    ).columns.to_list()
    numerical_cols.remove("player_id")
    col_subset = numerical_cols[2:]
    essential_cols = [
        "Player",
        "Season",
        "Squad",
        "Opponent",
        "player_id",
        "opponent_difficulty",
        "team_difficulty",
    ]  # , "Date", "total_points"
    team_strength_map = (
        team_df[["name", "strength"]].set_index("name", drop=True).to_dict()["strength"]
    )
    
    next_fixture_df = get_player_next_fixtures()
    dfs = []
    for idx, (player, player_df) in enumerate(inference_df.groupby("Player")):
        raw_df = player_df.fillna(0).sort_values("Date")
        lagged_df = lag_features(raw_df, [1, 2], subset=col_subset, inference=True)
        average_df = rolling_cols(
            raw_df, 5, np.average, subset=col_subset, inference=True
        )
        df = pd.concat([lagged_df, average_df, player_df[essential_cols]], axis=1)
        df["Opponent"] = df["Opponent"].shift(-1)
        player_id = player_df["player_id"].unique()[0]
        team = player_df["Squad"].unique()[0]
        home = TEAM_MAPPING[
            next_fixture_df.query("player_id == @player_id").iloc[0].team_h
        ]
        away = TEAM_MAPPING[
            next_fixture_df.query("player_id == @player_id").iloc[0].team_a
        ]
        df.iloc[-1, df.columns.get_loc("Opponent")] = (
            home if (similar(away, team) > similar(home, team)) else away
        )
        df.iloc[-1, df.columns.get_loc("opponent_difficulty")] = team_strength_map[
            df.iloc[-1, df.columns.get_loc("Opponent")]
        ]
        dfs.append(df.iloc[-1])

    return pd.DataFrame(dfs)

def main(args):
    cat_cols = ["Squad", "Opponent"]
    feature_cols = ['Gls_Performance_lag_1',
        'Ast_Performance_lag_1',
        'PK_Performance_lag_1',
        'PKatt_Performance_lag_1',
        'Sh_Performance_lag_1',
        'SoT_Performance_lag_1',
        'CrdY_Performance_lag_1',
        'CrdR_Performance_lag_1',
        'Touches_Performance_lag_1',
        'Tkl_Performance_lag_1',
        'Int_Performance_lag_1',
        'Blocks_Performance_lag_1',
        'xG_Expected_lag_1',
        'npxG_Expected_lag_1',
        'xAG_Expected_lag_1',
        'SCA_SCA_lag_1',
        'GCA_SCA_lag_1',
        'Cmp_Passes_lag_1',
        'Att_Passes_lag_1',
        'Cmp_percent_Passes_lag_1',
        'PrgP_Passes_lag_1',
        'Carries_Carries_lag_1',
        'PrgC_Carries_lag_1',
        'Att_Take_Ons_lag_1',
        'Succ_Take_Ons_lag_1',
        'total_points_lag_1',
        'opponent_difficulty_lag_1',
        'team_difficulty_lag_1',
        'Gls_Performance_lag_2',
        'Ast_Performance_lag_2',
        'PK_Performance_lag_2',
        'PKatt_Performance_lag_2',
        'Sh_Performance_lag_2',
        'SoT_Performance_lag_2',
        'CrdY_Performance_lag_2',
        'CrdR_Performance_lag_2',
        'Touches_Performance_lag_2',
        'Tkl_Performance_lag_2',
        'Int_Performance_lag_2',
        'Blocks_Performance_lag_2',
        'xG_Expected_lag_2',
        'npxG_Expected_lag_2',
        'xAG_Expected_lag_2',
        'SCA_SCA_lag_2',
        'GCA_SCA_lag_2',
        'Cmp_Passes_lag_2',
        'Att_Passes_lag_2',
        'Cmp_percent_Passes_lag_2',
        'PrgP_Passes_lag_2',
        'Carries_Carries_lag_2',
        'PrgC_Carries_lag_2',
        'Att_Take_Ons_lag_2',
        'Succ_Take_Ons_lag_2',
        'total_points_lag_2',
        'opponent_difficulty_lag_2',
        'team_difficulty_lag_2',
        'rolling_average5_Gls_Performance',
        'rolling_average5_Ast_Performance',
        'rolling_average5_PK_Performance',
        'rolling_average5_PKatt_Performance',
        'rolling_average5_Sh_Performance',
        'rolling_average5_SoT_Performance',
        'rolling_average5_CrdY_Performance',
        'rolling_average5_CrdR_Performance',
        'rolling_average5_Touches_Performance',
        'rolling_average5_Tkl_Performance',
        'rolling_average5_Int_Performance',
        'rolling_average5_Blocks_Performance',
        'rolling_average5_xG_Expected',
        'rolling_average5_npxG_Expected',
        'rolling_average5_xAG_Expected',
        'rolling_average5_SCA_SCA',
        'rolling_average5_GCA_SCA',
        'rolling_average5_Cmp_Passes',
        'rolling_average5_Att_Passes',
        'rolling_average5_Cmp_percent_Passes',
        'rolling_average5_PrgP_Passes',
        'rolling_average5_Carries_Carries',
        'rolling_average5_PrgC_Carries',
        'rolling_average5_Att_Take_Ons',
        'rolling_average5_Succ_Take_Ons',
        'rolling_average5_total_points',
        'rolling_average5_opponent_difficulty',
        'rolling_average5_team_difficulty',
        'opponent_difficulty',
        'team_difficulty'] + cat_cols

    inference_df_final = generate_matchday_data()
    for col in cat_cols:
        inference_df_final[col] = inference_df_final[col].astype("category")

    model = lgb.Booster(model_file=DATA_DIR / 'PL/models/lightgbm_predict_pl.txt')  
    y_pred = model.predict(inference_df_final[feature_cols])
    inference_df_final["predicted_points"] = y_pred
    DISPLAY_COLS = cat_cols + ["Player", "predicted_points"]
    df = inference_df_final.sort_values("predicted_points", ascending=False)[DISPLAY_COLS]
    if args.output:
        df.to_csv(args.output, index=False)
    else:
        print(df.head(15).to_string())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Get ticker csv data via API calls to either AlphaVantage or Tiingo."
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        required=False,
        help="File path to trading universe",
    )
    # parser.add_argument("--symbol", required=False, help="File path to trading universe", nargs="+")
    # parser.add_argument("--inst-type", type=str, required=False, default="equity", choices=DATA_GETTER_INST_TYPES)
    args = parser.parse_args()
    main(args)
