#%% Import Libraries
import requests
from google.cloud import bigquery
from datetime import date, timedelta
import pandas as pd
#%%
# Define the API base URL
api_base_url = "https://rata.digitraffic.fi/api/v1/trains/"
# Define the BigQuery project and dataset details
project_id = "normative-analytics"
dataset_id = "datasci"
table_id = "train_data"
# Calculate the start date (3 years ago) and end date (today)
today = date.today()
start_date = date(2021,1,1)
#%%
# Retrieve data from the API
api_data = get_data_from_api(start_date, today)
#%% Check retrieved data
api_data
#%% Convert data into a DataFrame
df = load_json_into_dataframe(api_data)
#%%
df.head(10)
#%% Load data into BigQuery
load_into_bigquery(df)
#%%
# Function to retrieve data from the API
def get_data_from_api(start_date, end_date):
    data = []
    current_date = start_date

    while current_date <= end_date:
        formatted_date = current_date.strftime("%Y-%m-%d")
        api_url = f"{api_base_url}{formatted_date}/27"
        
        response = requests.get(api_url)
        if response.status_code == 200:
            data.extend(response.json())
            print(f"Data retrieved for {formatted_date}")
        else:
            print(f"Failed to retrieve data for {formatted_date}. Status code: {response.status_code}")
        
        current_date += timedelta(days=1)

    return data
#%%
def load_json_into_dataframe(json_data):
    # Replace this with your actual logic to convert JSON to DataFrame
    # Here, we assume that the JSON data is a list of dictionaries
    return pd.DataFrame(json_data)
#%%
def load_into_bigquery(df):
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq_table_path = "normative-analytics.datasci.train_data"

    job = client.load_table_from_dataframe(
        df, bq_table_path, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(bq_table_path)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), bq_table_path
        )
    )
# %%
