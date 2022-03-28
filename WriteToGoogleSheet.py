import pandas as pd
import numpy as np
import pygsheets
import os
import json
from google.oauth2 import service_account

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# read Google APIs through Github secrets
gapi = os.environ.get('GAPI')

# customize the authorization 
SCOPES = ('https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive')
service_account_info = json.loads(gapi)
my_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

client = pygsheets.authorize(custom_credentials =my_credentials)
print("-----------------Authorized--------------------")
sheet = client.open('Sheet1')
print("-----------------Sheet Opened------------------")

rawdf = pd.read_csv("https://data.ny.gov/api/views/vxuj-8kew/rows.csv?accessType=DOWNLOAD&sorting=true", header= 0, index_col=False)


wks = sheet[0]
print("-----------------First Sheet Accessed----------")

wks.set_dataframe(rawdf,(1,1))
print("-----------------Data Updated------------------")
