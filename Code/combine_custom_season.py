import os
import glob
import pandas as pd
import re
import sys

def merge_csv_files(files, temp_output_file):
    """
    Merge all CSV files into one consolidated file with standardized columns.
    Missing columns will be filled with NA values.
    """
    print(f"\n===== MERGING FILES =====\n")
    print(f"Merging {len(files)} files into {temp_output_file}")
    
    # Define the standard columns
    # standard_columns = [
    #     "Season", "Player", "G", "MP", "PER", "TS%", "3PAr", "FTr", "ORB%", "DRB%", "TRB%", "AST%", 
    #     "STL%", "BLK%", "TOV%", "USG%", "OWS", "DWS", "WS", "WS/48", 
    #     "OBPM", "DBPM", "BPM", "VORP", "Awards", 
    #     "6MOY", "AS", "DEF1", "DEF2", "NBA1", "NBA2", "NBA3", "MVP", "DPOY", "ROY", "MIP", "CPOY"
    # ]

    standard_columns = [
        "Season", "Player", "G", "MP", "PER", "TS%", "3PAr", "FTr", "ORB%", "DRB%", "TRB%", "AST%", 
        "STL%", "BLK%", "TOV%", "USG%", "OWS", "DWS", "WS", "WS/48", 
        "OBPM", "DBPM", "BPM", "VORP", "Awards", "Finals MVP"
    ]
    
    print(f"Using standard columns: {', '.join(standard_columns)}")
    
    # Create a list to store all dataframes
    all_dfs = []
    
    # Process each file
    for file_path in sorted(files):
        filename = os.path.basename(file_path)
        
        try:
            # Read the file
            df = pd.read_csv(file_path)
            
            # Ensure all column names are strings
            df.columns = df.columns.astype(str)
            
            # Remove rows with "League Average" in the "Player" column
            if 'Player' in df.columns:
                df = df[df['Player'] != 'League Average']
            
            # Create a new DataFrame with standard columns
            new_df = pd.DataFrame(columns=standard_columns)
            
            # Copy data for columns that exist in both DataFrames
            for col in standard_columns:
                if col in df.columns:
                    new_df[col] = df[col]
                else:
                    # If column doesn't exist, fill with NA
                    new_df[col] = pd.NA
            
            # Add to the list of dataframes
            all_dfs.append(new_df)
            
            print(f"Processed: {filename} ({len(df)} rows)")
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
    
    # Combine all dataframes
    if all_dfs:
        merged_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save to temporary CSV
        merged_df.to_csv(temp_output_file, index=False)
        
        print(f"\nSuccessfully merged {len(all_dfs)} files into {temp_output_file}")
        print(f"Temporary file contains {len(merged_df)} rows and {len(merged_df.columns)} columns")
        
        return merged_df
    else:
        print("No files were successfully processed for merging.")
        return None

def process_awards(input_file, output_file):
    """
    Process the Awards column in a merged file to add slashes and set award columns.
    Efficiently handles comma-separated awards values like "MVP-1,AS,NBA1".
    """
    print(f"\n===== PROCESSING AWARDS =====\n")
    print(f"Processing awards in {input_file}")
    
    # Read the merged file
    df = pd.read_csv(input_file)
    
    # Define award prefixes and fixed awards
    award_prefixes = {"MVP-": "MVP", "ROY-": "ROY", "DPOY-": "DPOY", 
                     "6MOY-": "6MOY", "CPOY-": "CPOY", "MIP-": "MIP"}
    fixed_awards = ["AS", "DEF1", "DEF2", "NBA1", "NBA2", "NBA3"]
    
    # Initialize award columns with zeros (using vectorized operations)
    award_columns = ['6MOY', 'AS', 'DEF1', 'DEF2', 'NBA1', 'NBA2', 'NBA3', 
                    'MVP', 'DPOY', 'ROY', 'MIP', 'CPOY']
    for col in award_columns:
        df[col] = 0
    
    # Process the Awards column
    if 'Awards' in df.columns:
        print("Found Awards column, processing...")
        
        # Create a mask for rows that have awards
        awards_mask = df['Awards'].notna() & (df['Awards'].str.strip() != '')
        rows_with_awards = df[awards_mask]
        
        # Process each row with awards
        for idx, row in rows_with_awards.iterrows():
            original_text = str(row['Awards'])
            modified_text = ""
            
            # Split by commas to handle comma-separated awards
            award_parts = [part.strip() for part in original_text.split(',') if part.strip()]
            
            # Track which award columns to set to 1
            awards_to_set = set()
            
            # Process each award part
            for part in award_parts:
                # Check for fixed awards first
                award_found = False
                for award in fixed_awards:
                    if part == award:
                        modified_text += "/" + award + "/"
                        awards_to_set.add(award)
                        award_found = True
                        break
                
                # If not a fixed award, check for prefixed awards
                if not award_found:
                    for prefix, award_base in award_prefixes.items():
                        if part.startswith(prefix):
                            # Add award with slashes
                            modified_text += "/" + part + "/"
                            
                            # Check if it's a first-place award (ends with -1)
                            if part == f"{prefix}1":
                                awards_to_set.add(award_base)
                                
                            award_found = True
                            break
                
                # If no award pattern matched, just add with slashes
                if not award_found:
                    modified_text += "/" + part + "/"
            
            # If modified_text is empty but original had content, preserve the original
            if modified_text == "" and original_text != "":
                modified_text = "/" + original_text + "/"
            
            # Update the Awards column
            df.at[idx, 'Awards'] = modified_text
            
            # Set award columns to 1 for matched awards
            for award in awards_to_set:
                df.at[idx, award] = 1
    
    # Rename the Awards column to "Awards List" for clarity
    df = df.rename(columns={'Awards': 'Awards List'})
    
    # Save the processed file
    df.to_csv(output_file, index=False)
    
    print(f"Awards processing complete. Output saved to {output_file}")
    return df

def process_playoffs_awards(input_file, output_file):
    """
    Process the Awards column in a merged playoff file to set Final MVP column.
    Efficiently handles comma-separated awards values.
    """
    print(f"\n===== PROCESSING PLAYOFF AWARDS =====\n")
    print(f"Processing awards in {input_file}")
    
    # Read the merged file
    df = pd.read_csv(input_file)
    
    # Make sure the Final MVP column exists and initialize with zeros
    if 'Final MVP' not in df.columns:
        df['Final MVP'] = 0
    else:
        # Reset any existing values to 0
        df['Final MVP'] = 0
    
    # Process the Awards column
    if 'Awards' in df.columns:
        print("Found Awards column, processing...")
        
        # Create a mask for rows that have awards
        awards_mask = df['Awards'].notna() & (df['Awards'].str.strip() != '')
        
        # Process only rows with awards (more efficient)
        for idx, row in df[awards_mask].iterrows():
            original_text = str(row['Awards'])
            modified_text = ""
            
            # Split by commas to handle comma-separated awards
            award_parts = [part.strip() for part in original_text.split(',') if part.strip()]
            
            # Flag to track if Finals MVP was found
            finals_mvp_found = False
            
            # Process each award part
            for part in award_parts:
                if part == "Finals MVP-1":
                    # Set the Final MVP column to 1
                    df.at[idx, 'Final MVP'] = 1
                    # Add it with slashes
                    modified_text += "/Finals MVP-1/"
                    finals_mvp_found = True
                else:
                    # Add other awards with slashes
                    modified_text += "/" + part + "/"
            
            # If modified_text is empty but original had content, preserve the original
            if modified_text == "" and original_text != "":
                modified_text = "/" + original_text + "/"
            
            # Update the Awards column
            df.at[idx, 'Awards'] = modified_text
    
    # Rename the Awards column to "Awards List" for clarity
    df = df.rename(columns={'Awards': 'Awards List'})
    
    # Save the processed file
    df.to_csv(output_file, index=False)
    
    print(f"Playoff awards processing complete. Output saved to {output_file}")
    return df

# Main function for regular season data
def process_regular_season():
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script directory: {script_dir}")
    
    # Change to that directory
    os.chdir(script_dir)
    print(f"Changed working directory to: {os.getcwd()}")
    
    # List all CSV files in the directory for debugging
    all_csv_files = glob.glob("*.csv")
    print(f"All CSV files in directory: {all_csv_files}")
    
    # Set the pattern to match your files
    file_pattern = "rg_*.csv"
    
    # Find all files matching the pattern
    files = glob.glob(file_pattern)
    print(f"Files matching pattern '{file_pattern}': {files}")
    
    if not files:
        print(f"No files found matching pattern: {file_pattern}")
        # Ask user if they want to process all CSV files instead
        response = input("Would you like to process all CSV files instead? (y/n): ")
        if response.lower() == 'y':
            files = all_csv_files
            print(f"Processing all {len(files)} CSV files")
    
    if files:
        print(f"Found {len(files)} files to process")
        
        # First merge all files
        temp_output = "temp_merged.csv"
        merged_df = merge_csv_files(files, temp_output)
        
        if merged_df is not None:
            # Then process awards
            final_output = "rg_1949_2024_v1.csv"
            processed_df = process_awards(temp_output, final_output)
            
            # Show sample of the final file
            if processed_df is not None:
                print("\nFirst few rows of the final file:")
                sample_rows = processed_df.sample(min(3, len(processed_df)))
                for idx, row in sample_rows.iterrows():
                    if pd.notna(row['Awards List']) and row['Awards List'] != '':
                        print(f"Season: {row['Season']}, Player: {row['Player']}")
                        print(f"  Awards: {row['Awards List']}")
                        award_values = []
                        for col in ['6MOY', 'AS', 'DEF1', 'DEF2', 'NBA1', 'NBA2', 'NBA3', 'MVP', 'DPOY', 'ROY', 'MIP', 'CPOY']:
                            if row[col] == 1:
                                award_values.append(f"{col}={row[col]}")
                        print(f"  Award columns: {', '.join(award_values)}")
                        print()
                
                # Clean up temporary file
                try:
                    os.remove(temp_output)
                    print(f"Temporary file {temp_output} removed")
                except:
                    pass
            
            return processed_df
        else:
            print("Failed to merge regular season files.")
            return None

# Main function for playoff data
def process_playoffs():
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script directory: {script_dir}")
    
    # Change to that directory
    os.chdir(script_dir)
    print(f"Changed working directory to: {os.getcwd()}")
    
    # List all CSV files in the directory for debugging
    all_csv_files = glob.glob("*.csv")
    print(f"All CSV files in directory: {all_csv_files}")
    
    # Set the pattern to match playoff files only
    file_pattern = "po_*.csv"
    
    # Find all files matching the pattern
    files = glob.glob(file_pattern)
    print(f"Files matching pattern '{file_pattern}': {files}")
    
    if not files:
        print(f"No files found matching pattern: {file_pattern}")
        sys.exit("No playoff files found for processing.")
    print(f"Found {len(files)} playoff files to process")
    
    # First merge all files
    temp_output = "temp_merged.csv"
    merged_df = merge_csv_files(files, temp_output)
    
    if merged_df is not None:
        # Process the playoff awards
        final_output = "po_1949_2024_v1.csv"
        processed_df = process_playoffs_awards(temp_output, final_output)
        
        # Show the first few rows of the final file
        if processed_df is not None:
            print("\nFirst few rows of the final file:")
            sample_rows = processed_df.sample(min(3, len(processed_df)))
            for idx, row in sample_rows.iterrows():
                if pd.notna(row['Awards List']) and row['Awards List'] != '':
                    print(f"Season: {row['Season']}, Player: {row['Player']}")
                    print(f"  Awards: {row['Awards List']}")
                    
                    # Only show Final MVP for playoff data
                    if 'Final MVP' in row and row['Final MVP'] == 1:
                        print(f"  Final MVP: {row['Final MVP']}")
                    print()
            
            # Clean up temporary file
            try:
                os.remove(temp_output)
                print(f"Temporary file {temp_output} removed")
            except:
                pass
        
        return processed_df
    else:
        print("Failed to merge playoff files.")
        return None

if __name__ == "__main__":
    # Ask the user which type of data to process
    print("Which data would you like to process?")
    print("1. Regular Season (rg_*.csv)")
    print("2. Playoffs (po_*.csv)")
    print("3. Both")
    
    choice = input("Enter your choice (1/2/3): ")
    
    if choice == "1" or choice == "3":
        process_regular_season()
    
    if choice == "2" or choice == "3":
        process_playoffs()