from google.cloud import bigquery
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from dateutil import tz

def del_tables(event):
    client = bigquery.Client()
    project_id = '<YOUR PROJECT ID>'
    dataset_id = '<YOUR DATASET ID>'

    #Change the timezone from UTC to Eastern
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    utc = datetime.datetime.utcnow()
    utc = utc.replace(tzinfo=from_zone)
    est = utc.astimezone(to_zone)

    #Use today's date to get yesterday
    today = est.date()
    yest = today - relativedelta(days=1) 

    #Query to get the creation time for each table in the dataset
    sql = """
    SELECT * FROM `<YOUR DATASET ID>.__TABLES_SUMMARY__`
    """

    #Running the query and putting the results directly into a df
    df = client.query(sql).to_dataframe()

    #Converting the creation time that's in unix (ms) to a datetime object
    df['date'] = pd.to_datetime(df['creation_time'], unit='ms').dt.strftime('%Y-%m-%d')

    #Getting all the temp tables and then getting any that are more than 1 day old
    df_del = df.loc[df['table_id'].str.contains('^temp_*')]
    df_del = df_del.loc[df_del['date'] <= pd.to_datetime(yest).strftime('%Y-%m-%d')]

    #Putting all our tables that meet the above conditions into a list to be deleted
    temp_to_del = df_del['table_id'].tolist()

    #Calling the api to delete the selected temp tables
    #Looing through the list of temp tables and deleteing each one
    for each in temp_to_del: 
        table_ref = client.dataset(dataset_id).table(each)
        client.delete_table(table_ref) 
    
    return 'Success'