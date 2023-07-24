# FantasyPrediction
Aiming to predict fantasy points for FPL and NBA to help my fantasy game :)

### Notebooks
#### [Relationship](relationship.ipynb): A notebook which reads some variables from nba box score and team data from MySQL database and checks for relationships and predictive power of fantasy points 
#### [Model](model.ipynb): A notebook which tries to predict and presents r2 and rmse as metrics. 
- Manages to outperform evaluation results reported in "NBA Fantasy Score Prediction" (see Existing works) despite different methods for generating test set. 
- Currently test set assumes that fantasy points do not follow time series. A separate test of predicting next season's fantasy points based on data from previous seasons does not show any significant difference

### Existing works
A quick google search yielded these works:
#### [Final Project: NBA Fantasy Score Prediction](https://www.connoryoung.com/resources/AML_FinalProject_Report.pdf)
- uses R2 as a metric to evaluate
#### [Learning to Turn Fantasy Basketball Into Real Money Introduction to Machine Learning](https://shreyasskandan.github.io/Old_Website/files/report-ChanHuShivakumar.pdf)
- evaluates with Rank Difference Error (RDE), which quantifies relative accuracy