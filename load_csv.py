import argparse
import os
import pandas as pd
import shutil

# pre-qa csv
def preview_data(df):
    pd.set_option('display.max_columns', None)
    print(f"\n Showing preview of the first 10 rows: {df.head(10)}")
    if len(df)-1 > 10: # silly to show preview of exact same rows twice
        print(f"\n Showing preview of the last 10 rows: {df.tail(10)}")
        print(f"\n Showing 5 random rows: {df.sample(n=5)}") # and this errors if the dataset is too small
    print(f"\n Number of rows in this dataset: {len(df)}")
    print(f"\n Showing the column names and types: {df.info()}")
    print(f"\n Summarizing statistics for each individual column: {df.describe()}")
    print(f"\n Showing most common values for each quantitative column: {df.mode()}")


# do the minimal transformations to load the data as-is into postgres, that way later upserts can work easier

# find the oldest and newest transaction dates, then use those to rename the file
# minimal transformation to get it to be sql compatible
# load into the postgres db

def standardize_csv_filename(filepath, df):
    """
    Finds the oldest and newest transaction dates in the file, then uses those to rename the file.
    
    :param df: dataframe of transactions directly from Fidelity
    """
    target_dir = '/Users/elisachen/Data/transactions/'
    earliest_trx_date = df['Date'].min().strftime('%Y-%m-%d')
    newest_trx_date = df['Date'].max().strftime('%Y-%m-%d')
    new_filename = f"transactions_{earliest_trx_date}_to_{newest_trx_date}.csv"
    new_path = os.path.join(target_dir, new_filename)
    shutil.move(filepath, new_path)
    


def load_fidelity_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, skiprows=1, skipfooter=40)
    # Step 1: Ensure the column exists
    if 'Date' not in df.columns:
        raise ValueError("Expected a 'Date' column in the CSV")
    # standardize date column from str --> date so that df.describe() gives statistics on the dates in the file
    df['Date'] = pd.to_datetime(df['Date'])
    preview_data(df)
    return df

def transform_fidelity_csv(df):
    pass

# post-transform QA



if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='csv-loader')

    parser.add_argument("--csv_file", "-f", help="""File path to the csv file exported from Fidelity on local computer, including the filename. 
                        Tip if you get the unrecognized arguments error - make sure the quotes you're using for enclosing the filepath are straight quotes (' or \"), not curly quotes (’).""")
    args = parser.parse_args()

    df = load_fidelity_csv(args.csv_file)
    standardize_csv_filename(args.csv_file, df)