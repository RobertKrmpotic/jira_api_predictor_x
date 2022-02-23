import random
import pandas as pd
import numpy as np
from loguru import logger
import preprocessing, features
from sklearn.model_selection import cross_validate
from sklearn.linear_model import LinearRegression,ElasticNet
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import TimeSeriesSplit, RandomizedSearchCV

def model_selection(X_trans,y):

    models = [LinearRegression(),RandomForestRegressor(), ElasticNet()]
    performance_dict = {}
    #test multiple models 
    for model in models:
        cv_results = cross_validate(model, X_trans, y, cv=TimeSeriesSplit(n_splits=5), scoring="neg_mean_absolute_error")
        performance_dict[model] = cv_results['test_score'].mean()
        logger.debug(cv_results['test_score'].mean())
    #lowest error
    winner_model = max(performance_dict, key=performance_dict.get)   
    winner_name = type(winner_model).__name__

    

    return (winner_model,winner_name)

def hyperparameter_tuning(winner_model, winner_name, X_trans,y):

    rf_param_grid = [
                    {
                        'max_depth': random.sample(range(3,100), 10),
                        'min_samples_leaf': random.sample(range(3,100), 10)
                    }                
                ]

    en_param_grid = [
                    {
                    "alpha": np.linspace(0.1,100,1000),
                    "l1_ratio": [0.1,0.5,0.7,0.9,0.95,0.99,1]
    }]

    lr_param_grid={}

    param_dict = { "ElasticNet":en_param_grid,
                    "RandomForestRegressor":rf_param_grid,
                    "LinearRegression": lr_param_grid
    }

    search = RandomizedSearchCV(
        winner_model,
        param_dict[winner_name],
        cv = TimeSeriesSplit(n_splits=8),
        scoring="neg_mean_absolute_error",
        n_jobs=-1).fit(X_trans,y)

    search.cv_results_["mean_test_score"].mean()
    return search.best_estimator_

def merge_predictions(predictions,issues_prep_full):
    predictions = pd.Series(predictions, name="predicted_hours")
    database_df = pd.concat([predictions,issues_prep_full.loc[:,["key", "created", "status",  ]]], axis=1).rename(columns={"key":"issue_key"})
    database_df['time_added'] = pd.to_timedelta(database_df['predicted_hours'],'h')
    database_df["predicted_resolved"] = database_df["created"] + database_df['time_added']
    return database_df.loc[:,["issue_key", "created", "status", "predicted_resolved"]]


def model_workflow(issues:pd.DataFrame, transitions:pd.DataFrame, day_count:pd.DataFrame):
    logger.debug("Preprocessing data")

    issues_prep_full = preprocessing.run_issues_pipeline(issues,day_count)
    issues_prep = issues_prep_full.pipe(preprocessing.filter_values_is, "status", ["Done"]).reset_index(drop=True)
    issues_prep = preprocessing.remove_reopened(issues_prep,transitions)
    issues_prep = preprocessing.remove_outliers(issues_prep, z_treshold=1)

    logger.debug("Generating X and Y")
    X_train,y_train = preprocessing.get_X_y(issues_prep)
    X_test,y_test = preprocessing.get_X_y(issues_prep_full)

    logger.debug("Generating features")
    X_trans, X_test_trans =  features.generate_features(X_train,y_train, X_test)

    logger.debug("Selecting features")
    X_trans = features.select_features(X_trans,y_train, threshold=0.2)
    X_test_trans = X_test_trans.loc[:,list(X_trans.columns)]

    logger.debug("Selecting model")
    winner_model,winner_name  = model_selection(X_trans,y_train)
    tuned_model = hyperparameter_tuning(winner_model,winner_name,X_trans,y_train)
    tuned_model.fit(X_trans,y_train)
    logger.debug("Predicting future")
    predictions = tuned_model.predict(X_test_trans)
    database_df = merge_predictions(predictions,issues_prep_full)
    print(database_df.head())

    return database_df