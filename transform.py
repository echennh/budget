import os.path

import numpy as np

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

from google_utils import retrieve_credentials

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1jBdpJTFX2iVVTaa-WiUb-J_xwPM4EG4TFmN69HENvMQ"
SAMPLE_RANGE_NAME = "transactions!A:H"



def main():
  """Shows basic usage of the Sheets API.
  Prints values from a sample spreadsheet.
  """

  creds = retrieve_credentials(None)

  try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
        .execute()
    )
    values = result.get("values", [])

    if not values:
      print("No data found.")
      return
    # Add owner field
    df = pd.DataFrame(values[1:], columns=values[0])
    df.insert(0, 'Owner', None)
    # Now determine the owner
    account_owner_mapping = {'7331': 'Elisa', 'Cash Management (Individual - TOD)': 'Elisa', '3462':'Charles', 'EVERYDAY CHECKING':'Charles', 'PRIMARY SAVINGS': 'Charles'}
    df['Owner'] = df['Account'].map(account_owner_mapping).fillna(df['Owner'])
    # Rename the description to 'Original Description' and add a 'Description' to the left of it
    df.rename(columns={'Description': 'Original Description'}, inplace=True)
    df.insert(2, 'Description', None)

    # Now create the `Type` column
    df.insert(4, 'Type', None)
    # But first, must convert the 'Amount' column to a number
    df['Amount'] = df['Amount'].replace(r'[\$,]', '', regex=True).astype(float)

    # Now set "Type" = "debit" if the Amount is < 0, "credit" if it is > 0
    df['Type'] = np.where(df['Amount'] >= 0, 'credit', 'debit')

    # Now create the subcategory column
    df.insert(5, 'Subcategory', None)

    # Now create the Account Name column by concatenating the Institution and Account columns
    df.insert(6, 'Account Name', df['Institution'] + ' ' + df['Account'])

    new_columns = ['Itemized?', 'Labels', 'Merchant', 'Shared?', 'Tags', 'Automatic Actions']
    # Now create the Itemized, Labels, Merchant, Shared, and Tags columns
    for col_name in new_columns:
      df.insert(len(df.columns), col_name, None)


    # Credit card payments rule
    # If the Institution contains 'Discover Credit Card', and the Type='credit', then make the Subcategory = 'Credit Card Payment'
    # or if the Original Description.upper = 'DIRECT DEBIT DISCOVER EPAYMENT', and the Type='debit', then make the Subcategory = 'Credit Card Payment'
    


    # Categorize paychecks rule

    # Drop the Institution, Account, Is Hidden, and Is Pending columns
    df.drop(columns = ['Institution', 'Account', 'Is Hidden', 'Is Pending'], inplace=True)

    col_order = ['Owner', 'Date', 'Description', 'Original Description', 'Amount', 'Type',
       'Category', 'Subcategory', 'Account Name', 'Itemized?',
       'Labels', 'Merchant', 'Shared?', 'Tags', 'Automatic Actions']

    df = df[col_order]

    df.to_csv('transformed_data.csv', index=False)

    for row in values:
      # Print columns A and E, which correspond to indices 0 and 4.
      print(f"{row[0]}, {row[4]}")
  except HttpError as err:
    print(err)


if __name__ == "__main__":
  main()