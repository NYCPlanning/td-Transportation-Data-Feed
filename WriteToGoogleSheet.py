import pandas as pd
import numpy as np
import pygsheets
import os
import json
import datetime
import pytz
import plotly.express as px
import plotly.graph_objects as go

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
#Get the data from the Sheet into python as DF
df = wks.get_as_df()

try:
    df=df.replace('TBD','')
    df['Date']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in df['Date']]
    df=df.sort_values(['Date']).reset_index(drop=True)
    df['end']=df['Date'].dt.strftime('%m/%d/%Y')
    df['start']=np.roll(df['end'],6)
    df['DateRange']=df['start']+' - '+df['end']   
    df['Subway']=pd.to_numeric(df['Subways: Total Estimated Ridership'])
    df['SubwayPct']=pd.to_numeric(df['Subways: % of Comparable Pre-Pandemic Day'])
    df['SubwayPrior']=df['Subway']/df['SubwayPct']
    df['SubwayAvg']=df['Subway'].rolling(7,min_periods=1).mean()
    df['SubwayPriorAvg']=df['SubwayPrior'].rolling(7,min_periods=1).mean()
    df['Bus']=pd.to_numeric(df['Buses: Total Estimated Ridership'])
    df['BusPct']=pd.to_numeric(df['Buses: % of Comparable Pre-Pandemic Day'])
    df['BusPrior']=df['Bus']/df['BusPct']
    df['BusAvg']=df['Bus'].rolling(7,min_periods=1).mean()
    df['BusPriorAvg']=df['BusPrior'].rolling(7,min_periods=1).mean()
    df['LIRR']=pd.to_numeric(df['LIRR: Total Estimated Ridership'])
    df['LIRRPct']=pd.to_numeric(df['LIRR: % of 2019 Monthly Weekday/Saturday/Sunday Average'])
    df['LIRRPrior']=df['LIRR']/df['LIRRPct']
    df['LIRRAvg']=df['LIRR'].rolling(7,min_periods=1).mean()
    df['LIRRPriorAvg']=df['LIRRPrior'].rolling(7,min_periods=1).mean()
    df['MNR']=pd.to_numeric(df['Metro-North: Total Estimated Ridership'],errors='coerce')
    df['MNRPct']=pd.to_numeric(df['Metro-North: % of 2019 Monthly Weekday/Saturday/Sunday Average'])
    df['MNRPrior']=df['MNR']/df['MNRPct']
    df['MNRAvg']=df['MNR'].rolling(7,min_periods=1).mean()
    df['MNRPriorAvg']=df['MNRPrior'].rolling(7,min_periods=1).mean()
    df['AAR']=pd.to_numeric(df['Access-A-Ride: Total Scheduled Trips'])
    df['AARPct']=pd.to_numeric(df['Access-A-Ride: % of Comprable Pre-Pandemic Day'])
    df['AARPrior']=df['AAR']/df['AARPct']
    df['AARAvg']=df['AAR'].rolling(7,min_periods=1).mean()
    df['AARPriorAvg']=df['AARPrior'].rolling(7,min_periods=1).mean()
    df['BT']=pd.to_numeric(df['Bridges and Tunnels: Total Traffic'])
    df['BTPct']=pd.to_numeric(df['Bridges and Tunnels: % of Comparable Pre-Pandemic Day'])
    df['BTPrior']=df['BT']/df['BTPct']
    df['BTAvg']=df['BT'].rolling(7,min_periods=1).mean()
    df['BTPriorAvg']=df['BTPrior'].rolling(7,min_periods=1).mean()
    df['Subway']=df['SubwayAvg']/df['SubwayPriorAvg']
    df['Bus']=df['BusAvg']/df['BusPriorAvg']
    df['Long Island Rail Road']=df['LIRRAvg']/df['LIRRPriorAvg']
    df['Metro-North Railroad']=df['MNRAvg']/df['MNRPriorAvg']
    df['Access-A-Ride']=df['AARAvg']/df['AARPriorAvg']
    df['Bridges and Tunnels']=df['BTAvg']/df['BTPriorAvg']    
    df=df.loc[6:,['Date','DateRange','Subway','Bus','Long Island Rail Road','Metro-North Railroad',
                  'Access-A-Ride','Bridges and Tunnels']].sort_values(['Date']).reset_index(drop=True)
    
    dfcolors={'Subway':'rgba(114,158,206,0.8)',
              'Bus':'rgba(103,191,92,0.8)',
              'Long Island Rail Road':'rgba(237,102,93,0.8)',
              'Metro-North Railroad':'rgba(168,120,110,0.8)',
              'Access-A-Ride':'rgba(237,151,202,0.8)',
              'Bridges and Tunnels':'rgba(173,139,201,0.8)'}
    dfnotes={'Subway':'*',
         'Bus':'*',
         'Long Island Rail Road':'**',
         'Metro-North Railroad':'**',
         'Access-A-Ride':'**',
         'Bridges and Tunnels':'*'}
    fig=go.Figure()
    fig=fig.add_trace(go.Scattergl(name='',
                                   mode='none',
                                   x=df['Date'],
                                   y=df['Subway'],
                                   showlegend=False,
                                   hovertext='<b>'+df['DateRange']+'</b>',
                                   hoverinfo='text'))
    for i in ['Subway','Bus','Long Island Rail Road','Metro-North Railroad','Access-A-Ride','Bridges and Tunnels']:
        fig=fig.add_trace(go.Scattergl(name=i+dfnotes[i]+'   ',
                                       mode='lines',
                                       x=df['Date'],
                                       y=df[i],
                                       line={'color':dfcolors[i],
                                             'width':2},
                                       hovertext=[i+': '+'{0:.1%}'.format(x) for x in df[i]],
                                       hoverinfo='text'))
        fig=fig.add_trace(go.Scattergl(name='',
                                       mode='markers',
                                       x=[df.loc[len(df)-1,'Date']],
                                       y=[df.loc[len(df)-1,i]],
                                       marker={'color':dfcolors[i],
                                               'size':8},
                                       showlegend=False,
                                       hoverinfo='skip'))
    fig.update_layout(
        template='plotly_white',
        title={'text':'<b>MTA Estimated Ridership and Traffic as Percentage of 2019</b><br>(7-Day Moving Average)',
               'font_size':20,
               'x':0.5,
               'xanchor':'center',
               'y':0.95,
               'yanchor':'top'},
        legend={'orientation':'h',
                'title_text':'',
                'font_size':16,
                'x':0.5,
                'xanchor':'center',
                'y':1,
                'yanchor':'bottom'},
        margin = {'b': 160,
                  'l': 80,
                  'r': 40,
                  't': 140},
        xaxis={'title':{'text':'<b>Date</b>',
                        'font_size':14},
               'tickfont_size':12,
               'dtick':'M1',
               'range':[min(df['Date'])-datetime.timedelta(days=15),max(df['Date'])+datetime.timedelta(days=15)],
               'fixedrange':True,
               'showgrid':False},
        yaxis={'title':{'text':'<b>Percent Change</b>',
                        'font_size':14},
               'tickfont_size':12,
               'tickformat':'.0%',
               'fixedrange':True,
               'showgrid':False},
        hoverlabel={'bgcolor':'rgba(255,255,255,0.95)',
                    'bordercolor':'rgba(0,0,0,0.1)',
                    'font_size':14},
        font={'family':'Arial',
              'color':'black'},
        dragmode=False,
        hovermode='x unified',
        )
    fig.add_annotation(text='<i>*% of Comparable Pre-Pandemic Day</i>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-80)
    fig.add_annotation(text='<i>**% of 2019 Monthly Weekday/Saturday/Sunday Average</i>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-100)
    fig.add_annotation(text = 'Data Source: <a href="https://new.mta.info/coronavirus/ridership" target="blank">Metropolitan Transportation Authority</a>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-120)
    fig.write_html('./7DayAvg.html',
                   include_plotlyjs='cdn',
                   config={'displayModeBar':True,
                           'displaylogo':False,
                           'modeBarButtonsToRemove':['select',
                                                     'lasso2d']})
    
    print(timestamp+' SUCCESS!')

except:
    print(timestamp+' ERROR!')    
    
  
