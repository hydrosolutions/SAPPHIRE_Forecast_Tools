import os
import sys
import json

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import geopandas as gpd

# ML libraries
from sklearn.linear_model import LinearRegression
from sklearn.linear_model import Ridge
from sklearn.svm import SVR
from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.neural_network import MLPRegressor

from xgboost import XGBRegressor
import xgboost as xgb

from lightgbm import LGBMRegressor
import lightgbm as lgb

from catboost import CatBoostRegressor
from catboost import CatBoost

from sklearn.feature_selection import SelectKBest, mutual_info_regression

from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer, KNNImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import r2_score, root_mean_squared_error
from tqdm import tqdm
import datetime
from joblib import dump, load

# Custom modules
from scr import data_loading as dl
from scr import data_utils as du
import plotter 
from scr import FeatureExtractor as FE
from scr import pca_utils

import optuna

#setup a logger
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from config import MODEL_CONFIG, PATH_CONFIG, EXPERIMENT_CONFIG



def get_model(model_type, params):
    if model_type == 'xgb':
        return XGBRegressor(**params)

    elif model_type == 'lgbm':
        return LGBMRegressor(**params)
    
    elif model_type == 'svr':
        return SVR(**params)
    
    elif model_type == 'catboost':
        return CatBoostRegressor(**params, cat_features=EXPERIMENT_CONFIG['cat_features'])

    elif model_type == 'rf':
        return RandomForestRegressor(**params)
    
    elif model_type == 'gradient_boosting':
        return GradientBoostingRegressor(**params)
    
    elif model_type == 'mlp':
        return MLPRegressor(**params)
    else:
        raise ValueError("Invalid model type")

    
def fit_cascade(model1, model2, X1, X2, y):
    model1.fit(X1, y)
    y_pred1 = model1.predict(X1)
    residuals1 = y - y_pred1

    model2.fit(X2, residuals1)

    pred_y = model1.predict(X1)
    pred_residuals = model2.predict(X2)
    y_pred = pred_y + pred_residuals

    print("R2: ", r2_score(y, y_pred))
    return model1, model2

def predict_cascade(model1, model2, X1, X2):
    # First prediction
    y_pred1 = model1.predict(X1)
    
    # Predicted relative error
    y_pred2 = model2.predict(X2)
    
    # Corrected prediction
    return y_pred1 + y_pred2


def fit_normal(model, X, y):
    model.fit(X, y)
    y_pred = model.predict(X)
    print("R2: ", r2_score(y, y_pred))
    return model

def predict_normal(model, X):
    return model.predict(X)


def process_features(df_train, df_test, features_1, features_2, target, pca_groups, variance_threshold): 
    df_train = df_train.copy()
    df_test = df_test.copy()

    if EXPERIMENT_CONFIG['handle_na'] == 'drop':
        all_cols = list(set(features_1 + features_2 + [target]))
        df_train = df_train.dropna(subset=all_cols)
        df_test = df_test.dropna(subset=all_cols)
    elif EXPERIMENT_CONFIG['handle_na'] == 'impute':
        impute_cols = list(set(features_1 + features_2))
        #only numeric columns
        impute_cols = [col for col in impute_cols if df_train[col].dtype.kind in 'ifc']
        if EXPERIMENT_CONFIG['impute_method'] == 'knn':
            imputer = KNNImputer(n_neighbors=5)
        else:
            imputer = SimpleImputer(strategy=EXPERIMENT_CONFIG['impute_method'])
        if df_train[impute_cols].isna().any().any():
            df_train[impute_cols] = imputer.fit_transform(df_train[impute_cols])
        if df_test[impute_cols].isna().any().any():
            df_test[impute_cols] = imputer.transform(df_test[impute_cols])

    features_2_num = [col for col in features_2 if df_train[col].dtype.kind in 'ifc']
    cat_features = [col for col in features_2 if col not in features_2_num]

    if EXPERIMENT_CONFIG['normalize'] and EXPERIMENT_CONFIG['normalize_per_basin']:
        df_train, df_test, scaler = du.normalize_features_per_basin(df_train, df_test, features_2_num, target)
    elif EXPERIMENT_CONFIG['normalize'] and not EXPERIMENT_CONFIG['normalize_per_basin']:
        df_train, df_test, scaler = du.normalize_features(df_train, df_test, features_2_num, target)
    else:
        scaler = None

    if pca_groups:
        df_train_pca, df_test_pca, pca_models, replaced_columns = pca_utils.apply_pca_groups(
            df_train, df_test, pca_groups, variance_threshold
        )
        features_1_pca = [col for col in features_1 if col not in replaced_columns]
        features_2_pca = [col for col in features_2 if col not in replaced_columns]
        for group_name in pca_groups.keys():
            if group_name in pca_models:
                n_components = pca_models[group_name].n_components_
                pca_cols = [f"{group_name}_PC{i+1}" for i in range(n_components)]
                if any(col in features_1 for col in pca_groups[group_name]):
                    features_1_pca.extend(pca_cols)
                if any(col in features_2 for col in pca_groups[group_name]):
                    features_2_pca.extend(pca_cols)
        features_1 = features_1_pca
        features_2 = features_2_pca

    if EXPERIMENT_CONFIG['use_mutual_info']:
        X2_train = df_train[features_2_num]
        y_train = df_train[target]

        if EXPERIMENT_CONFIG['remove_correlated_features']:
            # Step 1: Calculate the correlation matrix
            corr_matrix = X2_train.corr().abs()  # Absolute correlation values
            
            # Step 2: Identify highly correlated features
            upper_triangle = corr_matrix.where(np.triu(np.ones(corr_matrix.shape), k=1).astype(bool))
            highly_correlated = [column for column in upper_triangle.columns if any(upper_triangle[column] > 0.95)]  # Threshold: 0.9
            
            # Step 3: Drop highly correlated features
            X2_train = X2_train.drop(columns=highly_correlated)
            logger.info(f"Removed highly correlated features: {highly_correlated}")
        
        selector = SelectKBest(mutual_info_regression, k=EXPERIMENT_CONFIG['number_of_features'])
        selector.fit(X2_train, y_train)
        features_2 = X2_train.columns[selector.get_support()]
        logger.info(f"Selected features from mutual information: {features_2}")

        #add the catogorical features back
        features_2 = list(set(features_2) | set(cat_features))


    return df_train, df_test, features_1, features_2, scaler

def post_process_target(df_predictions, target, scaler):
    
    df_predictions = df_predictions.copy()
    
    if EXPERIMENT_CONFIG['normalize']:
        if EXPERIMENT_CONFIG['normalize_per_basin']:
            for code in df_predictions.code.unique():
                mean_, std_ = scaler[code][target]
                df_predictions.loc[df_predictions['code'] == code, 'Q_pred'] = df_predictions.loc[df_predictions['code'] == code, 'Q_pred'] * std_ + mean_
        else:
            mean_, std_ = scaler[target]
            df_predictions['Q_pred'] = df_predictions['Q_pred'] * std_ + mean_
    
    return df_predictions


def loo_cv(
        df : pd.DataFrame, 
        features_1 : list[str], 
        features_2 : list[str],
        target : str, 
        model_type : str,
        params : dict = None,
        mode: str = 'normal', 
        pca_groups: dict = None, 
        variance_threshold: float = 0.95):
    
    """ 
    Leave-one-out cross validation for time series data

    Parameters:
    -----------
    df : pd.DataFrame
        Dataframe containing all data
    features_1 : list[str]
        Features to use in the first model (e.g. linear regression)
    features_2 : list[str]
        Features to use in the second model (e.g. XGBoost)
    target : str
        Target variable
    model_type : str
        Type of model to use - xgboost, lgbm, random_forest ... (the model need to have a fit and predict method)
    params : dict
        Parameters for the model
    pca_groups : dict, default=None
        Dictionary mapping group names to lists of feature columns to apply PCA to
    variance_threshold : float, default=0.95
        Variance threshold for PCA

    return:
    --------
    df_predictions : pd.DataFrame
        Dataframe containing the predictions
    """

    df = df.copy()
    df['year'] = df['date'].dt.year

    years = df['year'].unique()
    df_predictions = pd.DataFrame()
    
    for year in tqdm(years, desc="Processing years", leave=True):

        df_train = df[df['year'] != year].dropna(subset=[target])
        df_test = df[df['year'] == year].dropna(subset=[target])
        df_predictions_year = df_test[['date', 'code', target]].copy()
        
        df_train, df_test, features_m1, features_m2, scaler = process_features(df_train, df_test, features_1, features_2, target, pca_groups, variance_threshold)

        X1_train = df_train[features_m1]
        X2_train = df_train[features_m2]
        y_train = df_train[target]
        y_test = df_test[target]

        X1_test = df_test[features_m1]
        X2_test = df_test[features_m2]
        
        model1 = Ridge()
        if params:
            model2 = get_model(model_type, params)
        else:
            params = MODEL_CONFIG[model_type]
            model2 = get_model(model_type, params)

        if mode == 'normal':
            model2 = fit_normal(model2, X2_train, y_train)
            y_pred = predict_normal(model2, X2_test)
        elif mode == 'cascade':
            model1, model2 = fit_cascade(model1, model2, X1_train, X2_train, y_train)
            y_pred = predict_cascade(model1, model2, X1_test, X2_test)
        else:
            raise ValueError("Invalid mode")

        df_predictions_year['Q_pred'] = y_pred

        df_predictions_year = post_process_target(df_predictions_year, target, scaler)
        
        df_predictions = pd.concat([df_predictions, df_predictions_year])

    #rename prediction to Q_pred and target to Q_obs
    df_predictions.rename(columns={target: 'Q_obs'}, inplace=True)

    return df_predictions


def fit_on_all(
        df : pd.DataFrame, 
        features_1 : list[str], 
        features_2 : list[str],
        target : str, 
        model_type : str,
        params : dict = None,
        mode: str = 'normal', 
        pca_groups: dict = None, 
        variance_threshold: float = 0.95):
    
    """
    Fit a model on all data
    """
    df = df.copy()
    df['year'] = df['date'].dt.year

    df_train = df.dropna(subset=[target])
    df_test = df.dropna(subset=[target])
    df_predictions = pd.DataFrame()

    df_train, df_test, features_m1, features_m2, scaler = process_features(df_train, df_test, features_1, features_2, target, pca_groups, variance_threshold)

    X1_train = df_train[features_m1]
    X2_train = df_train[features_m2]
    y_train = df_train[target]
    y_test = df_test[target]

    model_1 = Ridge()
    if params:
        model_2 = get_model(model_type, params)
    else:
        params = MODEL_CONFIG[model_type]
        model_2 = get_model(model_type, params)

    if mode == 'normal':
        model_2 = fit_normal(model_2, X2_train, y_train)
        y_pred = predict_normal(model_2, X2_train)
    
    elif mode == 'cascade':
        model_1, model_2 = fit_cascade(model_1, model_2, X1_train, X2_train, y_train)
        y_pred = predict_cascade(model_1, model_2, X1_train, X2_train)

    else:
        raise ValueError("Invalid mode")
    
    return model_1, model_2, scaler


def get_feature_importance(model, feature_names=None):
    """
    Get feature importance from a model and return a sorted DataFrame
    """
    if model is None:
        logger.warning("Model is None. Returning an empty DataFrame.")
        return pd.DataFrame()

    # Handle feature importance for different model types
    if isinstance(model, (XGBRegressor)):
        importance = model.feature_importances_
        if feature_names is None:
            feature_names = model.get_booster().feature_names
    elif isinstance(model, LGBMRegressor):
        importance = model.feature_importances_
        if feature_names is None:
            feature_names = model.booster_.feature_name()
    else:
        logger.warning("Model type not supported for feature importance extraction.")
        return pd.DataFrame()

    # Create a DataFrame for feature importance
    feature_importance = pd.DataFrame({
        'feature': feature_names,
        'importance': importance
    })

    feature_importance = feature_importance.sort_values('importance', ascending=False)
    
    # plot feature importance
    plt.figure(figsize=(10, 6))
    sns.barplot(x='importance', y='feature', data=feature_importance)
    plt.title('Feature Importance')
    plt.xlabel('Importance')
    plt.ylabel('Feature')
    save_dir = PATH_CONFIG['output_dir']
    plt.savefig(os.path.join(save_dir, 'feature_importance.png'), dpi=250)
    plt.show()
    
    return feature_importance


def objective_mlp(trial, X_train, y_train, X_val, y_val):
    """
    Optuna objective function for MLP optimization.
    """
    param = {
        'hidden_layer_sizes': trial.suggest_int('hidden_layer_sizes', 50, 200),
        'activation': trial.suggest_categorical('activation', ['relu', 'tanh']),
        'solver': trial.suggest_categorical('solver', ['adam', 'sgd']),
        'alpha': trial.suggest_float('alpha', 1e-5, 1e-1, log=True),
        'learning_rate': trial.suggest_categorical('learning_rate', ['constant', 'adaptive']),
        'max_iter': trial.suggest_int('max_iter', 100, 1000),
        'random_state': 42
    }
    
    # Create and train the model
    model = MLPRegressor(**param)
    model.fit(X_train, y_train)
    
    # Predict and evaluate
    y_pred = model.predict(X_val)
    score = r2_score(y_val, y_pred)
    
    return score

def objective_xgb(trial, X_train, y_train, X_val, y_val):
    """
    Optuna objective function for XGBoost optimization.
    """
    param = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 2000),
        'learning_rate': trial.suggest_float('learning_rate', 0.001, 0.1),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'min_child_weight': trial.suggest_int('min_child_weight', 1, 30),
        'subsample': trial.suggest_float('subsample', 0.3, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.3, 1.0),
        'gamma': trial.suggest_float('gamma', 0.0, 1.0),
        'lambda': trial.suggest_float('lambda', 1e-4, 10.0, log=True),
        'alpha': trial.suggest_float('alpha', 1e-4, 10.0, log=True),
        'n_jobs': -1,
        'verbosity': 0
    }
    
    # Create and train the model
    model = XGBRegressor(**param)
    model.fit(X_train, y_train)
    
    # Predict and evaluate
    y_pred = model.predict(X_val)
    score = r2_score(y_val, y_pred)
    
    return score

def objective_lgbm(trial, X_train, y_train, X_val, y_val):
    """
    Optuna objective function for LightGBM optimization.
    """
    param = {
        'n_estimators': trial.suggest_int('n_estimators', 100, 2000),
        'learning_rate': trial.suggest_float('learning_rate', 0.001, 0.1),
        'num_leaves': trial.suggest_int('num_leaves', 20, 100),
        'max_depth': trial.suggest_int('max_depth', 3, 12),
        'subsample': trial.suggest_float('subsample', 0.5, 1.0),
        'gamma': trial.suggest_float('gamma', 0.0, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.5, 1.0),
        'min_child_samples': trial.suggest_int('min_child_samples', 1, 30),
        'lambda_l1': trial.suggest_float('lambda_l1', 1e-2, 10.0, log=True),
        'lambda_l2': trial.suggest_float('lambda_l2', 1e-2, 10.0, log=True),
        'n_jobs': -1,
        'verbose': -1
    }
    
    # Create and train the model
    model = LGBMRegressor(**param)
    model.fit(X_train, y_train)
    
    # Predict and evaluate
    y_pred = model.predict(X_val)
    score = r2_score(y_val, y_pred)
    
    return score



def objective_catboost(trial, X_train, y_train, X_val, y_val):
    """
    Optuna objective function for CatBoost optimization.
    """
    param = {
        'iterations': trial.suggest_int('iterations', 100, 2000),
        'depth': trial.suggest_int('depth', 3, 12),
        'learning_rate': trial.suggest_float('learning_rate', 1e-3, 0.1, log=True),
        'l2_leaf_reg': trial.suggest_float('l2_leaf_reg', 1e-4, 10.0, log=True),
        'border_count': 254,
        'bagging_temperature': trial.suggest_float('bagging_temperature', 1e-4, 1.0),
        'random_strength': trial.suggest_float('random_strength', 1e-4, 10.0, log=True),
        'allow_writing_files': False,
    }
    
    # Create and train the model
    model = CatBoostRegressor(**param, cat_features=EXPERIMENT_CONFIG['cat_features'])
    model.fit(X_train, y_train)
    
    # Predict and evaluate
    y_pred = model.predict(X_val)
    score = r2_score(y_val, y_pred)
    
    return score

def optimize_hyperparams(
        df,features_1, features_2, target='target',
        n_trials=100, pca_groups=None, variance_threshold=0.95,):
    """
    Optimize LightGBM hyperparameters using Optuna.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame
    features : list
        List of feature columns
    target : str
        Target column name
    n_trials : int, default=100
        Number of optimization trials
        
    Returns:
    --------
    dict
        Best hyperparameters
    """
    df = df.copy()
    # Get unique years for validation
    df.dropna(subset = [target], inplace=True)
    df['year'] = df['date'].dt.year
    hyperparam_tuning_year_limit = EXPERIMENT_CONFIG['hyperparam_tuning_year_limit']
    df = df[df['year'] < hyperparam_tuning_year_limit]
    years = df['year'].unique()
    # 20% for validation
    num_train_years = int(len(years) * 0.8)
    train_years = years[:num_train_years]
    val_years = years[num_train_years:]
    
    logger.info(f"Train years: {train_years}")
    logger.info(f"Validation years: {val_years}")
    
    # Split data
    train_data = df[df['year'].isin(train_years)]
    val_data = df[df['year'].isin(val_years)]

    train_data, val_data, features_m1, features_m2, scaler = process_features(
        train_data, val_data, features_1, features_2, target, pca_groups, variance_threshold)
    
    X_train = train_data[features_m2]
    y_train = train_data[target]
    X_val = val_data[features_m2]
    y_val = val_data[target]
    

    # Create study
    study = optuna.create_study(direction='maximize', )
    
    if EXPERIMENT_CONFIG['model_type'] == 'lgbm':
        objective = objective_lgbm
    elif EXPERIMENT_CONFIG['model_type'] == 'xgb':
        objective = objective_xgb
    elif EXPERIMENT_CONFIG['model_type'] == 'catboost':
        objective = objective_catboost
    else:
        raise ValueError("Invalid model type for hyperparameter optimization")
    # Run optimization
    study.optimize(
        lambda trial: objective(trial, X_train, y_train, X_val, y_val),
        n_trials=n_trials
    )
    
    logger.info("Best trial:")
    trial = study.best_trial
    logger.info("  Value: {}".format(trial.value))
    logger.info("  Params: ")
    for key, value in trial.params.items():
        logger.info("    {}: {}".format(key, value))


    save_dir = PATH_CONFIG['output_dir']
    #plot the optimization history
    fig = optuna.visualization.plot_optimization_history(study)
    #save the figure (plotly graph figure) as html
    fig.write_html(os.path.join(save_dir, 'optimization_history.html'))
    fig.show()

    # plot the param importance
    fig = optuna.visualization.plot_param_importances(study)
    #save the figure (plotly graph figure) as html
    fig.write_html(os.path.join(save_dir, 'param_importances.html'))
    fig.show()

    #plot contour
    fig = optuna.visualization.plot_contour(study)
    #save the figure (plotly graph figure) as html
    fig.write_html(os.path.join(save_dir, 'contour.html'))
    fig.show()
    
    return trial.params