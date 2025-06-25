import pandas as pd
import numpy as np
import re

def normalize_player_name(name):
    """
    Normalize a player name by removing special characters and standardizing format.
    This helps match player names across different datasets with varying formats.
    
    Args:
        name: Player name string
        
    Returns:
        Normalized player name
    """
    if not isinstance(name, str):
        return ""
    
    # Remove asterisks and other special characters
    normalized = re.sub(r'\*|\(|\)|\'|\"', '', name)
    
    # Remove any suffixes like "Jr." or "Sr."
    normalized = re.sub(r'\s+Jr\.|\s+Sr\.|\s+III|\s+II|\s+IV', '', normalized)
    
    # Standardize spacing
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized

def process_file(input_file, output_file, hof_players):
    """
    Process a file by:
    1. Removing columns with >30% missing values
    2. Add "HOF" column onto the original file, if players are in hof_players list then cell value = 1 and vice versa
    3. Extracting only HOF players to a new file with improved player matching
    
    Args:
        input_file: Input CSV file path
        output_file: Output CSV file path for HOF players
        hof_players: List of Hall of Fame players
    """
    print(f"\n===== PROCESSING {input_file} =====")
    
    # Read the input file
    print(f"Reading data from {input_file}...")
    df = pd.read_csv(input_file)
    original_cols = len(df.columns)
    original_rows = len(df)
    print(f"Loaded {original_rows} rows and {original_cols} columns")
    
    # Step 1: Remove columns with >30% missing values
    total_rows = len(df)
    columns_to_drop = []
    
    print("\nAnalyzing column sparsity:")
    for column in df.columns:
        missing_count = df[column].isna().sum()
        missing_percentage = missing_count / total_rows
        
        if missing_percentage > 0.3:  # 30% threshold
            columns_to_drop.append(column)
            print(f"Column '{column}' has {missing_percentage:.2%} missing values - dropping")
    
    if columns_to_drop:
        df = df.drop(columns=columns_to_drop)
        print(f"Removed {len(columns_to_drop)} columns with >30% missing values")
        print(f"Remaining columns: {len(df.columns)} (was {original_cols})")
    else:
        print("No columns with >30% missing values found")
    
    # Step 2: Add "HOF" column to the original file
    print(f"\nAdding HOF column to the original file...")
    
    # Create a set of normalized HOF player names for faster lookups
    normalized_hof_set = {normalize_player_name(name) for name in hof_players}
    
    # Add HOF column (1 for HOF players, 0 for others)
    df['HOF'] = df['Player'].apply(lambda x: 1 if normalize_player_name(x) in normalized_hof_set else 0)
    
    # Save the modified original file with HOF column
    df.to_csv(input_file, index=False)
    print(f"Added HOF column and saved changes to {input_file}")
    print(f"HOF players found: {df['HOF'].sum()} out of {len(df)} rows")
    
    # Step 3: Extract HOF players with improved matching
    print(f"\nExtracting Hall of Fame players...")
    
    # Create a mask for HOF players using the HOF column
    mask = df['HOF'] == 1
    
    # Apply the mask to filter the dataframe
    hof_df = df[mask].copy()
    
    # Check for any duplicate rows
    if hof_df.duplicated().any():
        dupe_count = hof_df.duplicated().sum()
        print(f"Found {dupe_count} duplicate rows - removing them")
        hof_df = hof_df.drop_duplicates()
    
    # Save to output file
    hof_df.to_csv(output_file, index=False)
    
    # Print statistics about the extraction
    found_players = hof_df['Player'].unique()
    normalized_found = {normalize_player_name(name) for name in found_players}
    
    print(f"Saved {len(hof_df)} rows to {output_file}")
    print(f"Data includes {len(found_players)} unique Hall of Fame players")
    
    # Show sample of players found (first 5)
    if len(found_players) > 0:
        print("\nSample of HOF players found:")
        for player in sorted(found_players)[:5]:
            print(f"  - {player}")
    
    # Return the dataframe and the set of normalized names found
    return hof_df, normalized_found

if __name__ == "__main__":
    # Input and output files
    regular_season_input = "rg_1949_2024_v1.csv"
    playoffs_input = "po_1949_2024_v1.csv"
    
    regular_season_output = "HOF_rg_1949_2024.csv"
    playoffs_output = "HOF_po_1949_2024.csv"
    
    # List of HOF players
    hof_players = [
        "Carmelo Anthony",
        "Dwight Howard",
        "Pau Gasol",
        "Dirk Nowitzki",
        "Tony Parker",
        "Dwyane Wade",
        "Manu Ginobili",
        "Tim Hardaway",
        "Chris Bosh",
        "Paul Pierce",
        "Ben Wallace",
        "Chris Webber",
        "Toni Kukoc",
        "Bob Dandridge",
        "Kobe Bryant",
        "Tim Duncan",
        "Kevin Garnett",
        "Carl Braun",
        "Chuck Cooper",
        "Vlade Divac",
        "Bobby Jones",
        "Sidney Moncrief",
        "Jack Sikma",
        "Paul Westphal",
        "Ray Allen",
        "Maurice Cheeks",
        "Grant Hill",
        "Jason Kidd",
        "Steve Nash",
        "Dino Radja",
        "Charlie Scott",
        "Tracy McGrady",
        "George McGinnis",
        "Zelmo Beaty",
        "Allen Iverson",
        "Yao Ming",
        "Shaquille O'Neal",
        "Louie Dampier",
        "Jo Jo White",
        "Alonzo Mourning",
        "Mitch Richmond",
        "Guy Rodgers",
        "Sarunas Marciulionis",
        "Gary Payton",
        "Bernard King",
        "Roger Brown",
        "Richie Guerin",
        "Chet Walker",
        "Reggie Miller",
        "Ralph Sampson",
        "Jamaal Wilkes",
        "Artis Gilmore",
        "Chris Mullin",
        "Dennis Rodman",
        "Arvydas Sabonis",
        "Dennis Johnson",
        "Gus Johnson",
        "Karl Malone",
        "Scottie Pippen",
        "Michael Jordan",
        "David Robinson",
        "John Stockton",
        "Adrian Dantley",
        "Patrick Ewing",
        "Hakeem Olajuwon",
        "Charles Barkley",
        "Joe Dumars",
        "Dominique Wilkins",
        "Clyde Drexler",
        "Robert Parish",
        "James Worthy",
        "Magic Johnson",
        "Moses Malone",
        "Isiah Thomas",
        "Bob McAdoo",
        "Kevin McHale",
        "Larry Bird",
        "Alex English",
        "George Gervin",
        "Gail Goodrich",
        "David Thompson",
        "George Yardley",
        "Kareem Abdul-Jabbar",
        "Julius Erving",
        "Bill Walton",
        "Calvin Murphy",
        "Walt Bellamy",
        "Dan Issel",
        "Dick McGuire",
        "Bob Lanier",
        "Connie Hawkins",
        "Dave Cowens",
        "Earl Monroe",
        "Dave Bing",
        "Elvin Hayes",
        "Neil Johnston",
        "Lenny Wilkens",
        "K.C. Jones",
        "Wes Unseld",
        "Rick Barry",
        "Walt Frazier",
        "Pete Maravich",
        "Tom Heinsohn",
        "Billy Cunningham",
        "Nate Thurmond",
        "John Havlicek",
        "Sam Jones",
        "Bill Bradley",
        "Dave DeBusschere",
        "Jack Twyman",
        "Willis Reed",
        "Frank Ramsey",
        "Slater Martin",
        "Hal Greer",
        "Oscar Robertson",
        "Jerry Lucas",
        "Jerry West",
        "Wilt Chamberlain",
        "Paul Arizin",
        "Joe Fulks",
        "Cliff Hagan",
        "Jim Pollard",
        "Elgin Baylor",
        "Ed Macauley",
        "Bob Cousy",
        "Bob Pettit",
        "Bob Davies",
        "George Mikan"
    ]
    
    # Create a set of normalized HOF player names for comparison
    normalized_hof_set = {normalize_player_name(name) for name in hof_players}
    
    # Process regular season data
    rg_hof_df, rg_normalized_found = process_file(regular_season_input, regular_season_output, hof_players)
    
    # Process playoff data
    po_hof_df, po_normalized_found = process_file(playoffs_input, playoffs_output, hof_players)
    
    # Print summary
    print("\n===== SUMMARY =====")
    print(f"Regular season HOF data: {len(rg_hof_df)} rows, {len(rg_normalized_found)} unique players")
    print(f"Playoff HOF data: {len(po_hof_df)} rows, {len(po_normalized_found)} unique players")
    
    # Find missing players
    rg_missing = normalized_hof_set - rg_normalized_found
    po_missing = normalized_hof_set - po_normalized_found
    
    # Report missing players
    if rg_missing:
        print(f"\n{len(rg_missing)} HOF players missing from regular season data:")
        missing_original = [p for p in hof_players if normalize_player_name(p) in rg_missing]
        for player in sorted(missing_original)[:10]:
            print(f"  - {player}")
        if len(missing_original) > 10:
            print(f"  ... and {len(missing_original) - 10} more")
    
    if po_missing:
        print(f"\n{len(po_missing)} HOF players missing from playoff data:")
        missing_original = [p for p in hof_players if normalize_player_name(p) in po_missing]
        for player in sorted(missing_original)[:10]:
            print(f"  - {player}")
        if len(missing_original) > 10:
            print(f"  ... and {len(missing_original) - 10} more")