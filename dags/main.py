from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json
import boto3
import pandas as pd
from datetime import datetime
import os
import numpy as np
from IPython.display import display
import botocore
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()
aws_client_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_id = os.getenv('AWS_SECRET_ACCESS_KEY')
s3 = boto3.resource('s3', aws_access_key_id=aws_client_id, aws_secret_access_key=aws_secret_id)


def extract():
  url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
  parameters = {
    'start':'1',
    'limit':'15',
    'convert':'USD'
  }
  headers = {
    'Accepts': 'application/json',
    'X-CMC_PRO_API_KEY': 'aa1b2135-a6e5-4f39-86e4-d8445bd143ea',
  }

  session = Session()
  session.headers.update(headers)

  try:
    response = session.get(url, params=parameters)
    data = json.loads(response.text)
    print('Succesfully extracted')
  except (ConnectionError, Timeout, TooManyRedirects) as e:
    print(e)

  #df = pd.json_normalize(data['data'])

  bucket_name = 'coinbucket'
  file_name = 'CoinMarketData.json'
  try:
    s3.Object(bucket_name, file_name).put(Body=json.dumps(data).encode('utf-8'))
    print('Succesfully placed to s3!')
  except:
    print('Failed placing to s3')

def transform():
  # getting data from s3
  bucket_name = 'coinbucket'
  file_name = 'CoinMarketData.json'
  try:
      s3_object = s3.Object(bucket_name, file_name)
      df = json.loads(s3_object.get()['Body'].read().decode('utf-8'))
  except:
      pass
  
  df = pd.json_normalize(df['data'])
  

  # deleting unnecessary columns
  df_final = df.drop(columns=['platform.token_address', 'platform.slug', 'platform.symbol', 'platform.name', 'platform.id', 'quote.USD.tvl', 'platform', 'self_reported_circulating_supply', 'self_reported_market_cap', 'tvl_ratio', 'quote.USD.last_updated'])

  # chanching object dtype to string dtype
  object_cols = df_final.select_dtypes(include=['object']).columns
  df_final[object_cols] = df_final[object_cols].astype('string')

  # changing date columns Dtypes
  date_cols = ['last_updated','date_added']
  for col in date_cols:
    if col in df_final.columns:
      df_final[date_cols] = df_final[date_cols].astype('datetime64[ns]')

  # trimming down the column names with 'quote.usd.'
  gaidys = [col for col in df_final.columns if 'quote' in col]
  for col in gaidys:
    index = df_final.columns.get_loc(col)
    trimmed_name = df_final.columns[index].split('.')[-1]
    df_final = df_final.rename(columns={df_final.columns[index] : trimmed_name})

  print('Succesfully transformed.')
  # place transformed data into s3 temp folder.
  df_final.to_csv('tempdata.csv', index=False)
  try:
    s3_tempfile=s3.Object(bucket_name, 'temp/tempdata.csv')
    s3_tempfile.upload_file('tempdata.csv')
    print('Successfully uploaded transformed data to S3')
  except:
    print('Failed uploading temp')


def load_to_gbq():
  bucket_name = 'coinbucket'
  file_path = 'temp/tempdata.csv'
  try:
    s3_object = s3.Object(bucket_name, file_path)
    df = pd.read_csv(s3_object.get()['Body'], parse_dates=['last_updated','date_added'])
    print('Succesfully loaded from s3')
  except:
    print('Failed loading from s3')
    
  df.columns = df.columns.str.replace('.', '_')
  object_cols = df.select_dtypes(include=['object']).columns
  df[object_cols] = df[object_cols].astype('string')

  current_dir = os.path.dirname(os.path.abspath(__file__))
  service_account_file = os.path.join(current_dir, 'cloud_service.json')
  credentials = service_account.Credentials.from_service_account_file( service_account_file)
  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = service_account_file

  client = bigquery.Client()

  table_id = "tactical-hydra-380717.my_dataset.my_python_table"

  table_schema = []
  for column in df.columns:
    if df[column].dtype == 'int64':
      column_type = 'INTEGER'
    elif df[column].dtype == 'float64':
      column_type = 'FLOAT'
    elif df[column].dtype == 'datetime64[ns]':
      column_type = 'DATETIME'
    else:
      column_type = 'STRING'
    table_schema.append({'name': column, 'type': column_type})
    

  df.to_gbq(destination_table='my_dataset.my_python_table', project_id='tactical-hydra-380717', credentials=credentials, if_exists='append')

  try:
    table = client.get_table(table_id)
    print("Table already exists {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
          )
  except:
    table = bigquery.Table(table_id, schema=table_schema)
    table = client.create_table(table)  # Make an API request.
    print(
      "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )


