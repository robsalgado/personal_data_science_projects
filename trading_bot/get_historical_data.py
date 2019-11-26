import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import string
import time
from datetime import datetime

# Price History API Documentation
# https://developer.tdameritrade.com/price-history/apis/get/marketdata/%7Bsymbol%7D/pricehistory

# Function to turn a datetime object into unix
def unix_time_millis(dt):
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)

# Get the historical dates you need.
# Only doing one day here as an example
date = datetime.strptime('2019-11-19', '%Y-%m-%d')

# Convert to unix for the API
date_ms = unix_time_millis(date)

# Get a current list of all the stock symbols for the NYSE
alpha = list(string.ascii_uppercase)

symbols = []

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

# Get the price history for each stock. This can take a while
consumer_key = '<YOUR CONSUMER KEY>'

data_list = []

for each in symbols_clean:
    url = r"https://api.tdameritrade.com/v1/marketdata/{}/pricehistory".format(each)

    # You can do whatever period/frequency you want
    # This will grab the data for a single day
    params = {
        'apikey': consumer_key,
        'periodType': 'month',
        'frequencyType': 'daily',
        'frequency': '1',
        'startDate': date_ms,
        'endDate': date_ms,
        'needExtendedHoursData': 'true'
        }

    request = requests.get(
        url=url,
        params=params
        )

    data_list.append(request.json())
    time.sleep(.5)

# Create a list for each data point and loop through the json, adding the data to the lists
symbl_l, open_l, high_l, low_l, close_l, volume_l, date_l = [], [], [], [], [], [], []

for data in data_list:
    try:
        symbl_name = data['symbol']
    except KeyError:
        symbl_name = np.nan
    try:
        for each in data['candles']:
            symbl_l.append(symbl_name)
            open_l.append(each['open'])
            high_l.append(each['high'])
            low_l.append(each['low'])
            close_l.append(each['close'])
            volume_l.append(each['volume'])
            date_l.append(each['datetime'])
    except KeyError:
        pass

# Create a df from the lists
df = pd.DataFrame(
     {
        'symbol': symbl_l,
        'open': open_l,
        'high': high_l,
        'low': low_l,
        'close': close_l, 
        'volume': volume_l,
        'date': date_l
    }
 )

# Format the dates
df['date'] = pd.to_datetime(df['date'], unit='ms')
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

# Save to csv
df.to_csv(r'<YOUR PATH>\back_data.csv')