import os 
import sys
import logging
import json

PATH_CONFIG = {}


EXPERIMENT_CONFIG = {
    "experiment_name" : "with_forecast_15d_with_LR",
    "prediction_horizon" : 30,
    "offset" : None,
    "snow_vars" : ['SWE',],
    "feature_cols" : ['discharge', 'P', 'T', 'SWE'],
    "static_features": ['LAT', 'LON', 'gl_fr', 'h_mean', 'h_min', 'h_max', 'slope', 'area_km2'],
    "cat_features" : ['code_str'], #only relevant for catboost
    "model_type" : 'catboost',
    "hyperparam_tuning_year_limit" : 2022,
    "hyperparam_tuning" : True,
    "use_lr_predictors" : True,
    "use_mutual_info" : False,
    "remove_correlated_features" : False,
    "number_of_features" : 50,
    "handle_na": "impute",
    "impute_method" : "mean",
    "normalize" : True,
    "normalize_per_basin" : False,
    'mode' : 'normal',
    'num_elevation_zones' : 1,
    'unnatural_rivers': [15069, 15070, 15102, 15259, 17462, 16135, 16143, 16153, 16159],
}


MODEL_CONFIG = {
    'xgb': {
        'n_estimators': 2000, 
        'max_depth': 6,
        'gamma': 0.1, 
        'learning_rate': 0.005, 
        'objective': 'reg:squarederror',
        'min_child_weight': 10,
        'subsample': 0.8,
        'colsample_bytree': 0.8,
        'reg_alpha': 0.5,
        'reg_lambda': 0.5,
    },
    'lgbm': {
        'n_estimators': 1000, 
        'max_depth': 10,
        'learning_rate': 0.01, 
        'objective': 'regression',
        'min_child_weight': 20,
        'subsample': 0.5,
        'colsample_bytree': 0.5,
        'reg_alpha': 0.5,
        'reg_lambda': 0.5,
    },
    'catboost': {
        'iterations': 1000,
        'depth': 10,
        'learning_rate': 0.01,
        'l2_leaf_reg': 3,
        'border_count': 254,
        'bagging_temperature': 1,
        'random_strength': 1,
        'allow_writing_files': False,
    },
    'mlp': {
        'hidden_layer_sizes': (16),
        'activation': 'relu',
        'solver': 'adam',
        'alpha': 0.0001,
        'batch_size':  512,
        'learning_rate': 'adaptive',
        'learning_rate_init': 0.01,
        'max_iter': 100,
        'shuffle': True,
        'random_state': 42,
        'tol': 0.0001,
        'verbose': False,
    },
    'knn': {
        'n_neighbors': 20,
        'weights': 'uniform',
        'algorithm': 'auto',
        'leaf_size': 30,
        'p': 2,
        'metric': 'minkowski',
        'metric_params': None,
        'n_jobs': None,
    },
    'svr': {
        'kernel': 'rbf',
        'degree': 3,
        'gamma': 'scale',
        'coef0': 0.0,
        'tol': 0.001,
        'C': 1.0,
        'epsilon': 0.1,
        'shrinking': True,
        'cache_size': 200,
        'verbose': False,
        'max_iter': -1,
    },
}


FEATURE_CONFIG = {
            'discharge': [{
                'operation': 'mean',
                'windows': [3, 7, 15, 30],
                #'lags' : {30: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]}
                'lags': {}
            },
            {
                'operation' : 'slope',
                'windows': [7],
                'lags' : {}
            },
            {
                'operation' : 'peak_to_peak',
                'windows': [30],
                'lags' : {}
            },
            {
                'operation' : 'max',
                'windows': [15],
                'lags' : {}
            },
            {
                'operation' : 'median_abs_deviation',
                'windows': [15],
                'lags' : {}
            },
            {
                'operation' : 'std',
                'windows': [15],
                'lags' : {}
            },],
            'P': [{
                'operation': 'sum',
                'windows': [15, 30],
                'lags' : {15 : [-15]}
            },],
            'T': [{
                'operation': 'mean',
                'windows': [5, 15, 30],
                'lags' : {15: [-15]}
            },
            {
                'operation': 'slope',
                'windows': [7],
                'lags' : {7: [-7]}
            },
            {
                'operation': 'max',
                'windows': [15],
                'lags' : {}
            },
            {
                'operation': 'min',
                'windows': [15],
                'lags' : {}
            },],
            'ROF': [{
                'operation': 'sum',
                'windows': [3, 30],
                'lags' : {}
            },],
            'HS': [{
                'operation': 'mean',
                'windows': [3, 30],
                'lags' : {}
            },],
            'SWE': [{
                'operation': 'mean',
                'windows': [3, 10, 30],
                #'lags' : {10: [ 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150]}
                'lags' : {}
            },
            {
                'operation': 'slope',
                'windows': [3, 7, 15],
                'lags' : {}
            },
            {
                'operation': 'max',
                'windows': [30],
                'lags' : {}
            },
            {
                'operation': 'peak_to_peak',
                'windows': [15],
                'lags' : {}
            },
            {
                'operation': 'std',
                'windows': [15],
                'lags' : {}
            },
            {
                'operation': 'time_distance_from_peak',
                'windows': [180],
                'lags' : {}
            },],
            'SCA': [{
                'operation': 'mean',
                'windows': [3, 15,  30],
                'lags' : {}
            },
            {
                'operation': 'slope',
                'windows': [7],
                'lags' : {}
            },
            {
                'operation': 'max',
                'windows': [30],
                'lags' : {}
            },
            {
                'operation': 'peak_to_peak',
                'windows': [15, 30],
                'lags' : {}
            },],
        }