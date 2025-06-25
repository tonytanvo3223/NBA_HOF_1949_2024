import pandas as pd
import re
import os
import glob
import sys

def extract_season_from_filename(filename):
    """Extract season information from filename like po_1949_1950.csv -> 1949-1950"""
    # Look for pattern of two consecutive 4-digit numbers
    match = re.search(r'po_(\d{4})_(\d{4})', filename)
    # match = re.search(r'rg_(\d{4})_(\d{4})', filename)

    if match:
        return f"{match.group(1)}-{match.group(2)}"
    return None

def process_nba_csv(input_file):
    """
    Process a single NBA CSV file in place:
    1. Replace 'Rk' column with 'Season' and populate from filename
    2. Consolidate multiple team entries for the same player
    3. Remove Age, Team, Pos, and Player-additional columns
    """
    # Extract season from filename
    season = extract_season_from_filename(input_file)
    if not season:
        season = "Unknown"
    
    # Read the CSV file
    df = pd.read_csv(input_file)
    
    # Check if 'Rk' exists and replace with 'Season'
    if 'Rk' in df.columns:
        df = df.rename(columns={'Rk': 'Season'})
    
    # Set all values in 'Season' column to the extracted season
    df['Season'] = season
    
    # Remove specified columns
    columns_to_remove = ["Age", "Team", "Pos", "Player-additional", "GS"]
    for col in columns_to_remove:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Add award columns with default value 0
    
    # Regular Season Awards
    award_columns = ['6MOY', 'AS', 'DEF1', 'DEF2', 'NBA1', 'NBA2', 'NBA3', 'MVP', 'DPOY', 'ROY', 'MIP', 'CPOY']
    
    # Playoff Award
    # award_columns = ['Final MVP']
    
    for award in award_columns:
        if award not in df.columns:
            df[award] = 0
    
    # Process players with multiple teams
    # Group data by player name only since we removed Player-additional
    player_groups = df.groupby(['Player'])
    
    # Create a list to hold the processed rows
    new_rows = []
    
    # Process each player separately
    for player_name, group in player_groups:
        if len(group) == 1:
            # If player has only one row, keep it as is
            new_rows.append(group.iloc[0])
        else:
            # Find the row with the highest 'G' value
            main_row = group.loc[group['G'].idxmax()].copy()
            
            # Add the modified row to the new rows list
            new_rows.append(main_row)
    
    # Create a new dataframe from the processed rows
    new_df = pd.DataFrame(new_rows)
    
    # Reset index for the new dataframe
    new_df = new_df.reset_index(drop=True)
    
    # Save the processed data back to the original file
    new_df.to_csv(input_file, index=False)
    
    return input_file

def process_all_nba_files(input_pattern):
    """Process all NBA CSV files matching the given pattern"""
    # Find all files matching the pattern
    files = glob.glob(input_pattern)
    
    if not files:
        print(f"No files found matching pattern: {input_pattern}")
        return []
    
    processed_files = []
    for input_file in files:
        # Process the file
        try:
            processed_file = process_nba_csv(input_file)
            processed_files.append(processed_file)
            print(f"Processed: {input_file}")
        except Exception as e:
            print(f"Error processing {input_file}: {e}")
    
    return processed_files

if __name__ == "__main__":
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Change to that directory
    os.chdir(script_dir)
    
    # Hard-code the pattern to match your files
    input_pattern = "po_*.csv"
    # input_pattern = "rg_*.csv"

    
    # Process all files matching the pattern
    processed_files = process_all_nba_files(input_pattern)
    print(f"Processed {len(processed_files)} files.")






    
