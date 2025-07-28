import pandas as pd

def stamp_date(file, year, month):
    # Read the CSV
    df = pd.read_csv(file)
    
    # Add year and month columns
    df['year'] = year
    df['month'] = month
    
    # Save to a new CSV
    df.to_csv(file, index=False)