import pandas as pd
import numpy as np
import pygsheets
import os
import json
import datetime
import pytz
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
wks1 = sheet[0]
wks2 = sheet[2]
wks3 = sheet[3]

#Get the data from the Sheet into python as DF
df1 = wks1.get_as_df()
df2 = wks2.get_as_df()
df3 = wks3.get_as_df()

# df1=pd.read_csv('C:/Users/mayij/Desktop/1.csv')
# df2=pd.read_csv('C:/Users/mayij/Desktop/2.csv')
# df3=pd.read_csv('C:/Users/mayij/Desktop/3.csv')



try:
    df1=df1.replace('TBD','')
    df1['Date']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in df1['Date']]
    df1=df1.sort_values(['Date']).reset_index(drop=True)
    df1['End Date']=df1['Date'].dt.strftime('%m/%d/%Y')
    df1['Start Date']=np.roll(df1['End Date'],6)
    df1['DateRange']=df1['Start Date']+' - '+df1['End Date']
    df1['DOW']=df1['Date'].dt.day_of_week
    df1['Subway']=pd.to_numeric(df1['Subways: Total Estimated Ridership'])
    df1['SubwayPct']=pd.to_numeric(df1['Subways: % of Comparable Pre-Pandemic Day'])
    df1['SubwayPrior']=df1['Subway']/df1['SubwayPct']
    df1['SubwayWeek']=df1['Subway'].rolling(7,min_periods=1).mean()*7
    df1['SubwayPriorWeek']=df1['SubwayPrior'].rolling(7,min_periods=1).mean()*7
    df1['Bus']=pd.to_numeric(df1['Buses: Total Estimated Ridership'])
    df1['BusPct']=pd.to_numeric(df1['Buses: % of Comparable Pre-Pandemic Day'])
    df1['BusPrior']=df1['Bus']/df1['BusPct']
    df1['BusWeek']=df1['Bus'].rolling(7,min_periods=1).mean()*7
    df1['BusPriorWeek']=df1['BusPrior'].rolling(7,min_periods=1).mean()*7
    df1['LIRR']=pd.to_numeric(df1['LIRR: Total Estimated Ridership'])
    df1['LIRRPct']=pd.to_numeric(df1['LIRR: % of 2019 Monthly Weekday/Saturday/Sunday Average'])
    df1['LIRRPrior']=df1['LIRR']/df1['LIRRPct']
    df1['LIRRWeek']=df1['LIRR'].rolling(7,min_periods=1).mean()*7
    df1['LIRRPriorWeek']=df1['LIRRPrior'].rolling(7,min_periods=1).mean()*7   
    df1['MNR']=pd.to_numeric(df1['Metro-North: Total Estimated Ridership'],errors='coerce')
    df1['MNRPct']=pd.to_numeric(df1['Metro-North: % of 2019 Monthly Weekday/Saturday/Sunday Average'])
    df1['MNRPrior']=df1['MNR']/df1['MNRPct']
    df1['MNRWeek']=df1['MNR'].rolling(7,min_periods=1).mean()*7
    df1['MNRPriorWeek']=df1['MNRPrior'].rolling(7,min_periods=1).mean()*7
    df1['AAR']=pd.to_numeric(df1['Access-A-Ride: Total Scheduled Trips'])
    df1['AARPct']=pd.to_numeric(df1['Access-A-Ride: % of Comprable Pre-Pandemic Day'])
    df1['AARPrior']=df1['AAR']/df1['AARPct']
    df1['AARWeek']=df1['AAR'].rolling(7,min_periods=1).mean()*7
    df1['AARPriorWeek']=df1['AARPrior'].rolling(7,min_periods=1).mean()*7
    df1['BT']=pd.to_numeric(df1['Bridges and Tunnels: Total Traffic'])
    df1['BTPct']=pd.to_numeric(df1['Bridges and Tunnels: % of Comparable Pre-Pandemic Day'])
    df1['BTPrior']=df1['BT']/df1['BTPct']
    df1['BTWeek']=df1['BT'].rolling(7,min_periods=1).mean()*7
    df1['BTPriorWeek']=df1['BTPrior'].rolling(7,min_periods=1).mean()*7
    df1['MTA Subway']=df1['SubwayWeek']/df1['SubwayPriorWeek']
    df1['MTA Bus']=df1['BusWeek']/df1['BusPriorWeek']
    df1['LIRR']=df1['LIRRWeek']/df1['LIRRPriorWeek']
    df1['MNR']=df1['MNRWeek']/df1['MNRPriorWeek']
    df1['Access-A-Ride']=df1['AARWeek']/df1['AARPriorWeek']
    df1['MTA Bridges and Tunnels']=df1['BTWeek']/df1['BTPriorWeek']
    df1=df1.loc[df1['DOW']==5,['Start Date','DateRange','MTA Subway','MTA Bus','LIRR','MNR','Access-A-Ride',
                               'MTA Bridges and Tunnels']].reset_index(drop=True)
    
    df2['Start Date']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in df2['Date']]
    df2['Start Date']=df2['Start Date'].dt.strftime('%m/%d/%Y')
    df2['RailWeek']=[pd.to_numeric(str(x).replace(',','')) for x in df2['Rail']]
    df2['RailPct']=[pd.to_numeric(str(x).replace('%',''))/100 if pd.notna(x) else np.nan for x in df2['Rail 2019 Comp']]
    df2['RailPriorWeek']=df2['RailWeek']/(1+df2['RailPct'])
    df2['BusWeek']=[pd.to_numeric(str(x).replace(',','')) for x in df2['Bus']]
    df2['BusPct']=[pd.to_numeric(str(x).replace('%',''))/100 if pd.notna(x) else np.nan for x in df2['Bus 2019 Comp']]
    df2['BusPriorWeek']=df2['BusWeek']/(1+df2['BusPct'])
    df2['LRTWeek']=[pd.to_numeric(str(x).replace(',','')) for x in df2['LRT']]
    df2['LRTPct']=[pd.to_numeric(str(x).replace('%',''))/100 if pd.notna(x) else np.nan for x in df2['LRT 2019 Comp']]
    df2['LRTPriorWeek']=df2['LRTWeek']/(1+df2['LRTPct'])    
    df2['NJT Rail']=df2['RailWeek']/df2['RailPriorWeek']
    df2['NJT Bus']=df2['BusWeek']/df2['BusPriorWeek']
    df2['NJT LRT']=df2['LRTWeek']/df2['LRTPriorWeek']
    df2=df2.loc[60:,['Start Date','NJT Rail','NJT Bus','NJT LRT']].reset_index(drop=True)
    
    df3['PATH']=[pd.to_numeric(str(x).replace('%',''))/100 if pd.notna(x) else np.nan for x in df3['PATH % of 2019']]
    df3=df3[['Month','PATH']].reset_index(drop=True)
    
    df=pd.merge(df1,df2,on='Start Date',how='left')
    df['Month']=[str(x).split('/')[-1]+'-'+str(x).split('/')[0] for x in df['Start Date']]
    df=pd.merge(df,df3,on='Month',how='left')
    df['Date']=[datetime.datetime.strptime(x,'%m/%d/%Y') for x in df['Start Date']]
    
    dfcolors={'MTA Subway':'rgba(31,119,180,0.8)',
              'MTA Bus':'rgba(174,199,232,0.8)',
              'LIRR':'rgba(255,127,14,0.8)',
              'MNR':'rgba(255,187,120,0.8)',
              'Access-A-Ride':'rgba(44,160,44,0.8)',
              'MTA Bridges and Tunnels':'rgba(152,223,138,0.8)',
              'NJT Rail':'rgba(214,39,40,0.8)',
              'NJT Bus':'rgba(255,152,150,0.8)',
              'NJT LRT':'rgba(148,103,189,0.8)',
              'PATH':'rgba(197,176,213,0.8)'}
    dfnotes={'MTA Subway':'*',
             'MTA Bus':'*',
             'LIRR':'**',
             'MNR':'**',
             'Access-A-Ride':'**',
             'MTA Bridges and Tunnels':'*',
             'NJT Rail':'*',
             'NJT Bus':'*',
             'NJT LRT':'*',
             'PATH':'***'}
    fig=go.Figure()
    fig=fig.add_trace(go.Scattergl(name='',
                                   mode='none',
                                   x=df['Date'],
                                   y=df['MTA Subway'],
                                   showlegend=False,
                                   hovertext='<b>'+df['DateRange']+'</b>',
                                   hoverinfo='text'))
    for i in ['MTA Subway','MTA Bus','LIRR','MNR','Access-A-Ride','MTA Bridges and Tunnels','NJT Rail','NJT Bus','NJT LRT','PATH']:
        fig=fig.add_trace(go.Scattergl(name=i+dfnotes[i]+'   ',
                                       mode='lines',
                                       x=df['Date'],
                                       y=df[i],
                                       line={'color':dfcolors[i],
                                             'width':3},
                                       hovertext=[i+': '+'{0:.1%}'.format(x) for x in df[i]],
                                       hoverinfo='text'))
    fig.update_layout(
        template='plotly_white',
        title={'text':'<b>Weekly Ridership and Traffic as % of 2019</b>',
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
        margin = {'b': 180,
                  'l': 80,
                  'r': 40,
                  't': 120},
        xaxis={'title':{'text':'<b>Date</b>',
                        'font_size':14},
               'tickfont_size':12,
               'dtick':'M1',
               'range':[min(df['Date'])-datetime.timedelta(days=15),max(df['Date'])+datetime.timedelta(days=15)],
               'fixedrange':True,
               'showgrid':False},
        yaxis={'title':{'text':'<b>% of 2019</b>',
                        'font_size':14},
               'tickfont_size':12,
               'tickformat':'.0%',
               'fixedrange':True,
               'dtick':0.1,
               'showgrid':False},
        hoverlabel={'bgcolor':'rgba(255,255,255,0.95)',
                    'bordercolor':'rgba(0,0,0,0.1)',
                    'font_size':14},
        font={'family':'Arial',
              'color':'black'},
        dragmode=False,
        hovermode='x unified',
        )
    fig.add_hline(y=0.5,
                  line_color='rgba(0,0,0,0.2)',
                  line_width=1,
                  layer='below')
    fig.add_hline(y=1,
                  line_color='rgba(0,0,0,0.2)',
                  line_width=1,
                  layer='below')
    fig.add_annotation(text='<i>* % of Comparable Pre-Pandemic Day</i>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-80)
    fig.add_annotation(text='<i>** % of 2019 Monthly Weekday/Saturday/Sunday Average</i>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-100)
    fig.add_annotation(text='<i>*** Monthly Average as % of 2019 Monthly Average</i>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-120)
    fig.add_annotation(text = 'Data Source: <a href="https://new.mta.info/coronavirus/ridership" target="blank">Metropolitan Transportation Authority</a>; New Jersey Transit; <a href="https://www.panynj.gov/path/en/about/stats.html" target="blank">Port Authority NY NJ</a>',
                       font_size=14,
                       showarrow=False,
                       x=1,
                       xanchor='right',
                       xref='paper',
                       y=0,
                       yanchor='top',
                       yref='paper',
                       yshift=-140)
    fig.write_html('./Weekly.html',
                   include_plotlyjs='cdn',
                   config={'displayModeBar':True,
                           'displaylogo':False,
                           'modeBarButtonsToRemove':['select',
                                                     'lasso2d']})
    
    print(timestamp+' SUCCESS!')

except:
    print(timestamp+' ERROR!')    
    
  
