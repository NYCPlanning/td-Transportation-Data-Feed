import pandas as pd
import pygsheets
import os
import json
import datetime
import pytz

from google.oauth2 import service_account

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

timestamp=datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%m/%d/%Y')

# read Google APIs through Github secrets
gapi = os.environ.get('GAPI')

# customize the authorization 
SCOPES = ('https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive')
service_account_info = json.loads(gapi)
my_credentials = service_account.Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

client = pygsheets.authorize(custom_credentials =my_credentials)
print("-----------------Authorized--------------------")
sheet = client.open('Transportation Data Feed')
print("-----------------Sheet Opened------------------")

#Define which sheet to open in the file
wks = sheet[0]

#Get the data from MTA website
rawdf = pd.read_csv("https://data.ny.gov/api/views/vxuj-8kew/rows.csv?accessType=DOWNLOAD&sorting=true", header= 0, index_col=False)
wks.set_dataframe(rawdf,(1,1))
print("--------------MTA Data Populated---------------")

    
  
