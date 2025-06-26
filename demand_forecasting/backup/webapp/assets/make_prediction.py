#Predict using Served Model
import requests
import json
from datetime import date, datetime
from databricks.sdk import WorkspaceClient
import os

def query_endpoint(history_pd):
  
  ## Set up authorization new
  client_id = os.getenv('DATABRICKS_CLIENT_ID')
  client_secret = os.getenv('DATABRICKS_CLIENT_SECRET')
  databricks_instance = os.getenv('DATABRICKS_INSTANCE')
  serving_endpoint_url = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/forecasting_model_serving/invocations'

  token_url = "https://e2-demo-field-eng.cloud.databricks.com/oidc/v1/token"
  token_data = {
      'grant_type': 'client_credentials',
      'scope': 'all-apis'
  }
  token_response = requests.post(
      token_url,
      data=token_data,
      auth=(client_id, client_secret)
  )
  token_response.raise_for_status()
  access_token = token_response.json()['access_token']
  #print(access_token)

  # Custom encoder for handling date and datetime objects in JSON serialization
  class CustomJSONEncoder(json.JSONEncoder):
      def default(self, obj):
          if isinstance(obj, (datetime, date)):
              return obj.isoformat()
          return json.JSONEncoder.default(self, obj)

  # Prepare data payload from DataFrame for model invocation
  data_payload = {"dataframe_records": history_pd.rename(columns={'sales': 'y'}).to_dict(orient='records')} #'split'
  data_json = json.dumps(data_payload, cls=CustomJSONEncoder)

  # Setup headers for the POST request
  headers = {
      'Authorization': f'Bearer {access_token}',
      'Content-Type': 'application/json'
  }
  # API call to deploy model and obtain predictions
  serving_endpoint_url = 'https://e2-demo-field-eng.cloud.databricks.com/serving-endpoints/forecasting_model_serving/invocations'
  response = requests.post(serving_endpoint_url, headers=headers, data=data_json)
  
  return response