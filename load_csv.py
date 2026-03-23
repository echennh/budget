import argparse

import pandas as pd

# pre-qa csv
def preview_data(df):
    pd.set_option('display.max_columns', None)
    print(f"\n Showing preview of the first 15 rows: {df.head(15)}")
    print(f"\n Showing preview of the last 5 rows: {df.tail(5)}")
    print(f"\n Number of rows in this dataset: {len(df)}")
    print(f"\n Showing 5 random rows: {df.sample(n=5)}")
    print(f"\n Showing the column names and types: {df.info()}")
    print(f"\n Summarizing statistics for each individual column: {df.describe().T}")
    print(f"\n Showing most common values for each columns: {df.mode()}")

def load_fidelity_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath)
    preview_data(df)
    df.rename(columns={"Description": "original_description"}, inplace=True)
    df.insert(0, "owner", None)
    df.insert(df.columns.get_loc("Category") + 1, "Subcategory", None)
    df.insert(df.columns.get_loc("original_description"), "description", None)
    df.insert(df.columns.get_loc("Amount") + 1, "transaction_type", None)
    df["transaction_type"] = df["Amount"].apply(lambda x: "credit" if x < 0 else "debit")
    df["Amount"] = df["Amount"].abs()
    return df

def transform_fidelity_csv(df):
    pass

# post-transform QA



if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='csv-loader')

    parser.add_argument("--csv_file", "-f", help="File path to the csv file exported from Fidelity on local computer, including the filename.")
    args = parser.parse_args()

    df = load_fidelity_csv(args.csv_file)