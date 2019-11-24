import pytz
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from google.cloud import bigquery
import pyarrow
from google.cloud import storage
import string
import time

def daily_equity_quotes(event, context):
    # Get the api key from cloud storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('<NAME OF YOUR CLOUD STORAGE BUCKET>')
    blob = bucket.blob('<NAME OF YOUR SECRET FILE>')
    api_key = blob.download_as_string()
    
    # Check if the market was open today. Cloud functions use UTC and I'm in
    # eastern so I convert the timezone
    today = datetime.today().astimezone(pytz.timezone("America/New_York"))
    today_fmt = today.strftime('%Y-%m-%d')

    # Call the td ameritrade hours endpoint for equities to see if it is open
    market_url = 'https://api.tdameritrade.com/v1/marketdata/EQUITY/hours'

    params = {
        'apikey': api_key,
        'date': today_fmt
        }
    
    request = requests.get(
        url=market_url,
        params=params
        ).json()

    try:
        if request['equity']['EQ']['isOpen'] is True:
            # Get a current list of all the stock symbols for the NYSE
            # Create a list of every letter in the alphabet
            # Each page has a letter for all those symbols
            # i.e. http://eoddata.com/stocklist/NYSE/A.htm'
            alpha = list(string.ascii_uppercase)

            symbols = []

            # Loop through the letters in the alphabet to get the stocks on each page
            # from the table and store them in a list
            for each in alpha:
                url = 'http://eoddata.com/stocklist/NYSE/{}.htm'.format(each)
                resp = requests.get(url)
                site = resp.content
                soup = BeautifulSoup(site, 'html.parser')
                table = soup.find('table', {'class': 'quotes'})
                for row in table.findAll('tr')[1:]:
                    symbols.append(row.findAll('td')[0].text.rstrip())       

            # Remove the extra letters on the end
            symbols_clean = []

            for each in symbols:
                each = each.replace('.', '-')
                symbols_clean.append((each.split('-')[0]))

            # The TD Ameritrade api has a limit to the number of symbols you can get data for
            # in a single call so we chunk the list into 200 symbols at a time 
            def chunks(l, n):
                """
                Takes in a list and how long you want
                each chunk to be
                """
                n = max(1, n)
                return (l[i:i+n] for i in range(0, len(l), n))

            symbols_chunked = list(chunks(list(set(symbols_clean)), 200))            

            # Function for the api request to get the data from td ameritrade
            def quotes_request(stocks):
                """
                Makes an api call for a list of stock symbols
                and returns a dataframe
                """
                url = r"https://api.tdameritrade.com/v1/marketdata/quotes"

                params = {
                'apikey': api_key,
                'symbol': stocks
                }

                request = requests.get(
                    url=url,
                    params=params
                    ).json()

                time.sleep(1)

                return pd.DataFrame.from_dict(
                    request,
                    orient='index'
                    ).reset_index(drop=True)

            # Loop through the chunked list of synbols
            # and call the api. Append all the resulting dataframes into one
            df = pd.concat([quotes_request(each) for each in symbols_chunked])

            # Add the date and fmt the dates for BQ
            df['date'] = pd.to_datetime(today_fmt)
            df['date'] = df['date'].dt.date
            df['divDate'] = pd.to_datetime(df['divDate'])
            df['divDate'] = df['divDate'].dt.date
            df['divDate'] = df['divDate'].fillna(np.nan)
            
            # Remove anything without a price
            df = df.loc[df['bidPrice'] > 0]
            
            # Rename columns and format for bq (can't start with a number)
            df = df.rename(columns={
                '52WkHigh': '_52WkHigh',
                 '52WkLow': '_52WkLow'
                })
            
            # Add to bigquery
            client = bigquery.Client()

            dataset_id = 'equity_data'
            table_id = 'daily_quote_data'

            dataset_ref = client.dataset(dataset_id)
            table_ref = dataset_ref.table(table_id)

            job_config = bigquery.LoadJobConfig()
            job_config.source_format = bigquery.SourceFormat.CSV
            job_config.autodetect = True
            job_config.ignore_unknown_values = True
            job = client.load_table_from_dataframe(
                df,
                table_ref,
                location='US',
                job_config=job_config
            )

            job.result()

            return 'Success'

        else:
            # Market Not Open Today
            pass
    except KeyError:
        # Not a weekday
        pass