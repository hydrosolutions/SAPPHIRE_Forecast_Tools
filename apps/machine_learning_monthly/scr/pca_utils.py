import os
import sys
import json
import pickle
import os
import pandas as pd
import numpy as np
from sklearn.decomposition import PCA
from tqdm import tqdm
import datetime
from joblib import dump, load

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def save_pca_components(pca_models, feature_groups, output_dir='models'):
    """
    Save PCA models and feature groups for later use in inference.
    
    Parameters:
    -----------
    pca_models : dict
        Dictionary of fitted PCA models for each feature group
    feature_groups : dict
        Dictionary mapping group names to lists of feature columns
    output_dir : str, default='models'
        Directory to save the models and metadata
    """
    # Create directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Save PCA models
    with open(os.path.join(output_dir, 'pca_models.pkl'), 'wb') as f:
        pickle.dump(pca_models, f)
    
    # Save feature groups
    with open(os.path.join(output_dir, 'feature_groups.pkl'), 'wb') as f:
        pickle.dump(feature_groups, f)
    
    logger.info(f"PCA models and feature groups saved to {output_dir}")

def load_pca_components(input_dir='models'):
    """
    Load saved PCA models and feature groups.
    
    Parameters:
    -----------
    input_dir : str, default='models'
        Directory containing the saved models and metadata
    
    Returns:
    --------
    tuple
        (pca_models, feature_groups)
    """
    # Load PCA models
    with open(os.path.join(input_dir, 'pca_models.pkl'), 'rb') as f:
        pca_models = pickle.load(f)
    
    # Load feature groups
    with open(os.path.join(input_dir, 'feature_groups.pkl'), 'rb') as f:
        feature_groups = pickle.load(f)
    
    return pca_models, feature_groups

def apply_saved_pca(df, pca_models, feature_groups):
    """
    Apply saved PCA transformations to a new dataframe.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Dataframe to transform with PCA
    pca_models : dict
        Dictionary of fitted PCA models for each feature group
    feature_groups : dict
        Dictionary mapping group names to lists of feature columns
    
    Returns:
    --------
    pandas.DataFrame
        Dataframe with PCA transformations applied
    """
    df_pca = df.copy()
    
    for group_name, pca in pca_models.items():
        # Get columns for this group
        columns = feature_groups[group_name]
        
        # Check if all columns exist in the dataframe
        valid_columns = [col for col in columns if col in df.columns]
        
        if not valid_columns:
            logger.warning(f"No columns found for group {group_name}")
            continue
            
        # Extract the feature group data
        X_group = df[valid_columns].values
        
        # Transform the data
        pca_features = pca.transform(X_group)
        
        # Add PCA components to the dataframe
        for i in range(pca.n_components_):
            df_pca[f"{group_name}_PC{i+1}"] = pca_features[:, i]
    
    return df_pca

def apply_pca_groups(df_train, df_test, feature_groups, variance_threshold=0.95):
    """
    Apply PCA to specified groups of features, with each group becoming a set of PCA components.
    
    Parameters:
    -----------
    df_train : pandas.DataFrame
        Training dataframe containing all features
    df_test : pandas.DataFrame
        Test dataframe containing all features
    feature_groups : dict
        Dictionary where keys are group names and values are lists of column names
        Example: {'SWE': ['swe_1', 'swe_2', ...], 'TEMP': ['temp_1', 'temp_2', ...]}
    variance_threshold : float, default=0.95
        Minimum cumulative explained variance ratio to determine number of components
        
    Returns:
    --------
    tuple
        (df_train_with_pca, df_test_with_pca, pca_models, original_columns)
        - DataFrames with original columns replaced by PCA components
        - Dictionary of fitted PCA models for each group
        - Set of original column names that were replaced by PCA
    """
    df_train_pca = df_train.copy()
    if df_test is not None:
        df_test_pca = df_test.copy()
    else:
        df_test_pca = None
        logger.warning("No test data provided, only transforming training data")
    
    pca_models = {}
    original_columns = set()
    
    for group_name, columns in feature_groups.items():
        # Check if all columns exist in the dataframe
        valid_columns = [col for col in columns if col in df_train.columns]
        
        if not valid_columns:
            print(f"Warning: No columns found for group {group_name}")
            continue
            
        # Extract the feature group data
        X_train_group = df_train[valid_columns].values
        
        # Skip if there's not enough samples
        if X_train_group.shape[0] <= 1 or X_train_group.shape[1] <= 1:
            logger.warning(f"Skipping PCA for group {group_name} due to insufficient samples or features")
            continue
            
        # Initialize and fit PCA
        pca = PCA(n_components=min(X_train_group.shape[0], X_train_group.shape[1]))
        pca.fit(X_train_group)
        
        # Determine number of components needed to explain variance_threshold of variance
        explained_variance_ratio = pca.explained_variance_ratio_
        cumulative_variance_ratio = np.cumsum(explained_variance_ratio)
        n_components = np.argmax(cumulative_variance_ratio >= variance_threshold) + 1
        
        # Refit PCA with selected number of components
        pca = PCA(n_components=n_components)
        pca.fit(X_train_group)
        pca_models[group_name] = pca
        
        # Transform the data
        train_pca_features = pca.transform(X_train_group)
        if df_test is not None:
            test_pca_features = pca.transform(df_test[valid_columns].values)
        
        # Add PCA components to the dataframes
        for i in range(n_components):
            df_train_pca[f"{group_name}_PC{i+1}"] = train_pca_features[:, i]
            if df_test is not None:
                df_test_pca[f"{group_name}_PC{i+1}"] = test_pca_features[:, i]
            
        # Keep track of columns that have been replaced by PCA
        original_columns.update(valid_columns)
        
        logger.info(f"Group {group_name}: {len(valid_columns)} features -> {n_components} PCA components "
                    f"({cumulative_variance_ratio[n_components-1]:.2%} variance explained)")
    
    return df_train_pca, df_test_pca, pca_models, original_columns