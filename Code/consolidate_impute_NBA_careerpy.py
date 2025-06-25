import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

# Function to read CSV files
def read_csv(filepath):
    return pd.read_csv(filepath)

# Read the CSV files
regular_df = read_csv("rg_1949_2024_v1.csv")
playoff_df = read_csv("po_1949_2024_v1.csv")

# Function to impute missing values by era
def impute_missing_values(df):
    # Create a copy to avoid modifying the original
    df_imputed = df.copy()
    
    # Extract year from season (assuming format like "2023-24")
    df_imputed['Year'] = df_imputed['Season'].apply(lambda x: int(x.split('-')[0]))
    
    # Define eras
    df_imputed['Era'] = pd.cut(df_imputed['Year'], 
                              bins=[1940, 1960, 1980, 2000, 2025],
                              labels=['Pre-modern', 'Early-modern', 'Modern', 'Contemporary'])
    
    # List of metrics to impute - including MP
    all_metrics = ['MP', 'PER', 'TS%', '3PAr', 'FTr', 'ORB%', 'DRB%', 'TRB%', 'AST%', 
                   'STL%', 'BLK%', 'TOV%', 'USG%', 'WS/48', 'OBPM', 'DBPM', 'BPM']
    
    # Ensure all metrics exist in the DataFrame
    metrics_to_impute = [col for col in all_metrics if col in df.columns]
    
    # Impute separately for each era
    for era in df_imputed['Era'].unique():
        era_df = df_imputed[df_imputed['Era'] == era]
        
        # Define base features for imputation (we'll always have these)
        base_features = ['G']  # Removed MP since we want to impute it too
        
        # Add non-missing stats to features
        features = base_features.copy()
        for metric in metrics_to_impute:
            if not era_df[metric].isna().all():
                features.append(metric)
        
        # For each metric with missing values, perform imputation
        for metric in metrics_to_impute:
            if era_df[metric].isna().any():  # Check if any values are missing
                # Create temporary feature list excluding current metric
                temp_features = [f for f in features if f != metric]
                
                if len(temp_features) == 0:
                    # If no features available, use at least G
                    temp_features = ['G']
                
                if len(era_df) > 1:  # Only perform KNN if we have more than one sample
                    # Use KNN imputation
                    imputer = KNNImputer(n_neighbors=min(5, len(era_df)))
                    
                    # Select only the relevant columns for imputation
                    impute_df = era_df[temp_features + [metric]].copy()
                    
                    # Impute
                    imputed_values = imputer.fit_transform(impute_df)
                    
                    # Update the original dataframe
                    df_imputed.loc[era_df.index, metric] = imputed_values[:, -1]
                else:
                    # If only one sample, we can't use KNN - use global average
                    global_avg = df_imputed[metric].mean()
                    if pd.isna(global_avg):  # If all values are missing, use a default
                        if metric == 'MP':
                            global_avg = 20.0  # Default minutes per game
                        else:
                            global_avg = 0.0  # Default for other metrics
                    df_imputed.loc[era_df.index, metric] = global_avg
    
    # Special handling for advanced stats that weren't calculated in early eras
    for metric in metrics_to_impute:
        for era in ['Pre-modern', 'Early-modern', 'Modern', 'Contemporary']:
            era_df = df_imputed[df_imputed['Era'] == era]
            if era_df[metric].isna().all():
                # Find earliest era with this stat
                for potential_era in ['Early-modern', 'Modern', 'Contemporary']:
                    potential_era_df = df_imputed[df_imputed['Era'] == potential_era]
                    if not potential_era_df[metric].isna().all():
                        # Use league average from this era
                        avg_value = potential_era_df[metric].mean()
                        df_imputed.loc[era_df.index, metric] = avg_value
                        break
                # If no era has this stat, use a default value
                if era_df[metric].isna().all():
                    if metric == 'MP':
                        df_imputed.loc[era_df.index, metric] = 20.0  # Default minutes per game
                    else:
                        df_imputed.loc[era_df.index, metric] = 0.0
    
    # Round all numeric columns to 3 decimal places
    numeric_cols = df_imputed.select_dtypes(include=[np.number]).columns
    df_imputed[numeric_cols] = df_imputed[numeric_cols].round(3)
    
    # Drop the temporary columns
    df_imputed = df_imputed.drop(columns=['Year', 'Era'])
    
    return df_imputed

# Function to calculate career stats from season-by-season data
def calculate_career_stats(df):
    # Initialize empty dataframe for career stats
    career_stats = pd.DataFrame()
    
    # Add player names
    career_stats['Player'] = df['Player'].unique()
    
    # Add HOF column (preserve the HOF indicator from the original data)
    if 'HOF' in df.columns:
        # Since HOF is the same for all seasons of a player, take the first one for each player
        hof_values = df.groupby('Player')['HOF'].first().reset_index()
        career_stats = career_stats.merge(hof_values, on='Player', how='left')
    
    # Sum metrics
    sum_metrics = ['G', 'MP', 'OWS', 'DWS', 'WS', 'VORP']
    
    for metric in sum_metrics:
        if metric in df.columns:
            temp = df.groupby('Player')[metric].sum().reset_index()
            career_stats = career_stats.merge(temp, on='Player', how='left')
    
    # Weighted averages by minutes played
    weighted_by_mp = ['PER', 'TS%', '3PAr', 'FTr', 'USG%', 'WS/48', 'ORB%', 'DRB%', 
                     'TRB%', 'AST%', 'STL%', 'BLK%', 'TOV%', 'OBPM', 'DBPM', 'BPM']
    
    for metric in weighted_by_mp:
        if metric in df.columns:
            # Define a safe weighted average function that handles zero weights
            def safe_weighted_average(group):
                try:
                    # Skip calculation if all values are NaN
                    if group[metric].isna().all():
                        return np.nan
                    
                    # Skip calculation if all weights are zero
                    if (group['MP'] == 0).all() or group['MP'].sum() == 0:
                        # Use simple average instead when weights are all zero
                        return group[metric].mean()
                    
                    # Calculate weighted average normally
                    return np.average(group[metric], weights=group['MP'])
                except Exception as e:
                    # Fallback to simple mean if any other error occurs
                    print(f"Warning: Error calculating weighted average for {group['Player'].iloc[0]} - {metric}: {e}")
                    return group[metric].mean()
            
            # Create a temporary dataframe with weighted values
            temp = df.groupby('Player').apply(safe_weighted_average).reset_index()
            temp.columns = ['Player', metric]
            career_stats = career_stats.merge(temp, on='Player', how='left')
    
    # Sum award counts
    award_columns = ['MVP', 'DPOY', 'ROY', 'MIP', 'CPOY', 'AS', 'DEF1', 'DEF2', 
                    'NBA1', 'NBA2', 'NBA3', '6MOY', 'Final MVP']
    
    for award in award_columns:
        if award in df.columns:
            temp = df.groupby('Player')[award].sum().reset_index()
            career_stats = career_stats.merge(temp, on='Player', how='left')
    
    # Round all numeric columns to 3 decimal places
    numeric_cols = career_stats.select_dtypes(include=[np.number]).columns
    career_stats[numeric_cols] = career_stats[numeric_cols].round(3)
    
    return career_stats

# Impute missing values for all NBA players
regular_df_imputed = impute_missing_values(regular_df)
playoff_df_imputed = impute_missing_values(playoff_df)

# Calculate career stats for all NBA players
regular_career = calculate_career_stats(regular_df_imputed)
playoff_career = calculate_career_stats(playoff_df_imputed)

# Save the results with the new naming convention
regular_df_imputed.to_csv('NBA_rg_imputed.csv', index=False)
playoff_df_imputed.to_csv('NBA_po_imputed.csv', index=False)
regular_career.to_csv('NBA_regular_career.csv', index=False)
playoff_career.to_csv('NBA_playoff_career.csv', index=False)

print("Career statistics have been consolidated for all NBA players.")
print("Results saved to:")
print("1. NBA_rg_imputed.csv - Regular season data with imputed values")
print("2. NBA_po_imputed.csv - Playoff data with imputed values")
print("3. NBA_regular_career.csv - Regular season career statistics")
print("4. NBA_playoff_career.csv - Playoff career statistics")

# Print some stats about the data
print("\nData statistics:")
print(f"Regular season input: {len(regular_df)} rows, {regular_df['Player'].nunique()} unique players")
print(f"Regular season imputed: {len(regular_df_imputed)} rows")
print(f"Regular season career stats: {len(regular_career)} unique players")
print(f"Playoff input: {len(playoff_df)} rows, {playoff_df['Player'].nunique()} unique players")
print(f"Playoff imputed: {len(playoff_df_imputed)} rows")
print(f"Playoff career stats: {len(playoff_career)} unique players")

# Print number of HOF players if the column exists
if 'HOF' in regular_df.columns:
    hof_players_rg = regular_df[regular_df['HOF'] == 1]['Player'].nunique()
    print(f"\nHall of Fame players in regular season data: {hof_players_rg}")
    
if 'HOF' in playoff_df.columns:
    hof_players_po = playoff_df[playoff_df['HOF'] == 1]['Player'].nunique()
    print(f"Hall of Fame players in playoff data: {hof_players_po}")