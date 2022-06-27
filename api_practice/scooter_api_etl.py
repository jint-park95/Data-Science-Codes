# import libraries
import requests
import pandas as pd
import datetime 
from google.cloud import bigquery
from google.oauth2 import service_account
import pytz

# empty strings get list of urls
name_list = []
url_list = []

url = "https://mds.linkyour.city/gbfs/us_tx_austin/gbfs.json"
response = requests.get(url).json()
for i in response['data']['en']['feeds']:
    name_list.append(i['name'])
    url_list.append(i['url'])

url_df = pd.DataFrame({'name' : name_list, 'url' : url_list})

try: 
    fbs_url = url_df.loc[url_df['name'] == 'free_bike_status']['url'].values[0]
except IndexError:
    print(f"ERROR: free_bike_status file does not exist for the url: {url}")
else:
    fbs_url = fbs_url.replace("'", "")
    fbs_response = requests.get(fbs_url).json()

# print last updated EPOCH TIME
print(f"Last updated POSIX : {fbs_response['last_updated']}")

# Quick data manipulation
bike_dict = fbs_response['data']['bikes']
bike_df = pd.DataFrame.from_dict(bike_dict)

last_updated = datetime.datetime.fromtimestamp(int(fbs_response['last_updated'])).strftime('%Y-%m-%d %H:%M:%S')
bike_df['last_updated'] = last_updated
bike_df.head()

# Reference: https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe

# Construct a BigQuery client object.
path = r'C:\Users\jintp\Documents\GitHub\Data-Science-Codes\api_practice\auth\bikeshare_analytics_bq.json'
credentials = service_account.Credentials.from_service_account_file(path)
table_id = 'jintaepark-portoflio-project.bikeshare_analytics.raw_bike_location'

client = bigquery.Client(credentials=credentials)
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

job = client.load_table_from_dataframe(bike_df, table_id, job_config=job_config)  # Make an API request.
job.result()  # Wait for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)