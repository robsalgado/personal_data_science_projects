# Import required libraries
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from apiclient import discovery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import httplib2

# Get yesterday's date
today = datetime.today()
yesterday = today - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d') # Format it

# Define the url with the desired endpoint
base_url = 'https://newsapi.org/v2/everything'

# Define the query string parameters to get the data we need
params = {'q': 'artificial intelligence',
          'to': yesterday,
          'from':yesterday, 
          'apiKey':'<YOUR API KEY>', 
          'language' : 'en',
          'pageSize': 100}

# Construct the api call and make a GET request per the docs
response = requests.get(base_url, params=params)

# Store the results in a variable as a json
total_results = response.json()

# Add the data to a df
# Create a list for each field
content, title, url, name, date = [], [], [], [], []

# Loop through the json and add the data to the list
for each in total_results['articles']:
    # Some articles don't have content just a title so we check for that
    # if we don't find it we add in a NaN
    if 'content' in each:
        content.append(each['content'])
    else:
        content.append(np.nan)
    title.append(each['title'])
    url.append(each['url'])
    name.append(each['source']['name'])
    date.append(yesterday)

# Put the lists into a df and transpose them
df = pd.DataFrame([title, content, url, name, date]).T

# Add column names
df.columns = ['title', 'content', 'url', 'site', 'date']

# Put the data into the google sheet
# Define the scopes
scopes = ['https://www.googleapis.com/auth/spreadsheets']

# Define the credentials. Add your path to the credentials file
credentials = service_account.Credentials.from_service_account_file('<PATH TO CREDENTIALS JSON>',
                                                                    scopes=scopes)
# Build the service
service = discovery.build('sheets', 
                          'v4', 
                          credentials=credentials)

# Format the df to pass into the sheets api
# This will create a df with the headers as the first row
with_headers = pd.DataFrame(np.vstack([df.columns, df]))

# You only need to do include headers the first time
# After that you can just do:
# values = [df[each_col].tolist() for each_col in df]

# Then put each column into a list
values = [with_headers[each_col].tolist() for each_col in with_headers]

# Define the spreadhseet id
spreadsheet_id = '19uEbvGK1RxrI0BHHpgE8nMuGqrwqrowmo0pGngf25-c'

# Define the range for the data
range_ = sheet_name + '!A2:E'

# How the input data should be interpreted
value_input_option = 'RAW'  # Store values as they are

# How the input data should be inserted
insert_data_option = 'INSERT_ROWS' # Rows are inserted as opposed to overwriting

# Define the data fields and set major dimension to columns 
# The default is rows which will transpose each column as a row which you don't want
data = {'values': values,
        'majorDimension': 'COLUMNS'}

# Build the request and execute the api call
request = service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, 
                                                 range=range_, 
                                                 valueInputOption=value_input_option, 
                                                 insertDataOption=insert_data_option, 
                                                 body=data).execute()

# Print out the number of rows to verify
print('Number of rows inserted {}'.format(request['updates']['updatedRows']))