### Usage
- assumes that db etc is already set up


1. Weekly after previous matchday: `Rscript /mnt/HDD/Ivan/projects/FantasyPrediction/PL/get_fbref_data.R`
2. Generate upcoming batches plus predictions for players: `python PL/prediction/predict_matchday_points.py -o $HOME_DIR/mw.csv`