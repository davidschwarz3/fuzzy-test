#!/usr/bin/env python
# coding: utf-8

# In[110]:


import pandas as pd
import numpy as np

import os

import plotly as py
import plotly.graph_objs as go
from plotly.offline import iplot, init_notebook_mode
import plotly.express as px
from plotly.subplots import make_subplots
pd.options.display.max_columns = 100

#import matplotlib.pyplot as plt

pd.options.plotting.backend = 'plotly'

from datetime import timedelta
#import swifter

from datetime import date, time, datetime
z = datetime.strptime('19:54:59.750','%H:%M:%S.%f').time()
import time
#from yahoo_fin.stock_info import *
from datetime import date, time, datetime
z = datetime.strptime('19:54:59.750','%H:%M:%S.%f').time()
#import yfinance as yf
#import pandas_datareader as pdr
from datetime import datetime
from datetime import date
#import pandas.io.sql as sqlio
import elasticsearch
import unicodedata
import csv



# In[111]:


def withimb(imb, diff):
    #raw diff is calculated to be negative is price has moved down
    try:
        if imb > 0:
            if diff >= 0:
                direction = diff*1
            else:
                direction = diff*1
        else:
            if diff < 0:
                direction = diff*-1
            else:
                direction = diff*-1
        return direction
    except ZeroDivisionError:
        return 0        


# In[112]:


def PRO(imb, refprice, opening):
    if refprice == 0:
        PRO = 0
    elif imb == 0:
        PRO = 0
    elif imb >= 0:
        PRO = (opening - (refprice + .01))/refprice
    else:
        PRO = (refprice - (opening + .01))/refprice
    return round(PRO, 6)


# In[113]:


def AbsReturn(imb, refprice, exitprice):
    if refprice == 0:
        absreturn = 0
    elif imb >= 0:
        absreturn = exitprice - (refprice + .01)
    else:
        absreturn = refprice - (exitprice+.01)
    return round(absreturn,2)


# In[114]:


def PRR(absreturn, entry):
    if absreturn == 0:
        PRR = 0
    elif refprice == 0:
        PRR = 0
    else:
        PRR = absreturn / entry
    return round(PRR, 6)


# In[115]:


def sharestraded(imb, previous_imb,maxshares):
    if previous_imb == 0:
        NomImbChange = imb
    elif imb == 0:
        NomImbChange = imb
    else:
        if previous_imb/imb > 0:
            if imb >= 0:
                if imb > maxshares:
                    NomImbChange = imb - maxshares
                else:
                    NomImbChange = 0
            else:
                if imb < maxshares:
                    NomImbChange = maxshares - imb
                else:
                    NomImbChange = 0
        else:
            NomImbChange = imb
    return np.absolute(NomImbChange)


# In[116]:


def locate_max(series, query, column):
    position = 2
    while True:
        if int(query) == int(series[position][column]):
            answer = position - 2
            return answer
        position += 3


# In[117]:


def locate(series, query):
    position = 0
    while True:
        if series[position] == query:
            return position
        position += 1


# In[118]:


def find(df,col,value):
    df = df[df[col]==value]
    return df


# In[119]:


#To create new df set the df = remove()
def remove(df,list_to_remove,col_to_remove):
    original = df.index.name
    df = df.reset_index()
    index_to_remove = df[df[col_to_remove].isin(list_to_remove)].index
    df = df.iloc[df.index.drop(index_to_remove)]
    df = df.set_index(original)
    return df


# In[120]:


def info(df):
    return df.info(show_counts=True)


# In[121]:


def rmse(a, b):
    return mean_squared_error(a,b,squared=False)


# In[122]:


def paste(df):
    return df.to_clipboard()


# In[123]:


#To create new df set the df = remove()
def remove(df,list_to_remove,col_to_remove):
    original = df.index.name
    df = df.reset_index()
    index_to_remove = df[df[col_to_remove].isin(list_to_remove)].index
    df = df.iloc[df.index.drop(index_to_remove)]
    df = df.set_index(original)
    return df


# In[124]:


#creates dataframe of just one symbol
def plot_candles_outdated(symbol, df, date):
    sym = df[df['SYMBOL']==symbol]
    sym = sym[sym['Date']==date]
    #plotting to view candle/imbalance/MQTY/Volume of one symbol
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True,row_width=[.3,.5,.5,1.5],
                        vertical_spacing=.03,
                        subplot_titles=('Candles','IMB','MQTY','Vol'))
    fig.add_trace(go.Candlestick(x=sym.index,
                   open=sym['Reference_Price'],
                   high=sym['ref_price_max'],
                   low=sym['ref_price_min'],
                   close=sym['ref_price_last']),row=1,col=1)

    fig.add_trace(go.Scatter(x=sym.index,y=sym['Real_Time_Imbalance'],mode='lines',name='Imb'), row=2, col=1)
    fig.add_trace(go.Scatter(x=sym.index,y=sym['matched_quantity'],mode='lines',name='Mqty'), row=3, col=1)
    fig.add_trace(go.Scatter(x=sym.index,y=sym['Volume'],mode='lines',name='Vol'), row=4, col=1)
    fig.update(layout_xaxis_rangeslider_visible=False)

    fig.show()


# In[125]:


#creates dataframe of just one symbol
def plot_candles(symbol, df, date):
    sym = df[df['SYMBOL']==symbol]
    sym = sym[sym['Date']==date]
    #plotting to view candle/imbalance/MQTY/Volume of one symbol
    fig = make_subplots(rows=4, cols=1, shared_xaxes=True,row_width=[.3,.5,.5,1.5],
                        vertical_spacing=.03,
                        subplot_titles=('Candles','IMB','MQTY','Vol'))
    fig.add_trace(go.Candlestick(x=sym.index,
                   open=sym['Reference_Price'],
                   high=sym['ref_price_max'],
                   low=sym['ref_price_min'],
                   close=sym['ref_price_last']),row=1,col=1)

    fig.add_trace(go.Scatter(x=sym.index,y=sym['Real_Time_Imbalance'],mode='lines',name='Imb'), row=2, col=1)
    fig.add_trace(go.Scatter(x=sym.index,y=sym['matched_quantity'],mode='lines',name='Mqty'), row=3, col=1)
    fig.add_trace(go.Scatter(x=sym.index,y=sym['Volume'],mode='lines',name='Vol'), row=4, col=1)
    fig.add_trace(go.Scatter(x=sym.index,y=sym['Closing_Price'],mode='lines',name='Closing'), row=1, col=1)
    icpplot = sym[['Indicated_Clearing_Price']].replace(0,np.nan)
    fig.add_trace(go.Scatter(x=icpplot.index,y=icpplot['Indicated_Clearing_Price'],mode='lines',name='ICP'), row=1, col=1)


    fig.update(layout_xaxis_rangeslider_visible=False)

    fig.show()


# In[126]:


def results_table(df, date_col, deployed_col, return_col, factor):  
    grouping = df.groupby(factor)
    table = df.groupby(factor)[deployed_col,return_col].sum()

    u_factor = factor.copy()
    timegrouper = type(pd.Grouper(freq='3s'))
    i = 0
    while i < len(u_factor):
        if type(u_factor[i]) == timegrouper:
                u_factor[i] = 'Time'
        else:
            pass
        i = i+1
    if date_col in u_factor:
        pass
    else:
        u_factor.append(date_col)
    if 'SYMBOL' in u_factor:
        pass
    else:
        u_factor.append('SYMBOL')

    table['Positional_Return'] = (table[return_col]/table[deployed_col])
    table = table.reset_index()
    #winners = df[(df['Winners']==True) & (df[deployed_col]!=0)].reset_index()

    winners = df[(df[return_col]>0) & (df[deployed_col].notna())] #removed reset_index to accomodate timegrouper 8/8/22
    win_summary = winners.groupby(u_factor)[['SYMBOL']].nunique()
    win_summary.rename(columns={'SYMBOL':'Win_Count'},inplace=True)
    if any(isinstance(x, timegrouper) for x in factor):
        win_summary = win_summary.reset_index().set_index('Time')
    else:
        win_summary = win_summary.reset_index()
    win_summary = win_summary.groupby(factor)[['Win_Count']].sum().reset_index()
    win_dollars = winners.groupby(factor)[[return_col]].sum().reset_index()
    win_summary = win_summary.merge(win_dollars)
    win_summary.rename(columns={return_col:'Winner_Dollars'},inplace=True)
    winmean = winners.groupby(factor)[[return_col]].mean().reset_index()
    winmean.rename(columns={return_col:'WinMean'},inplace=True)
    win_summary = win_summary.merge(winmean)
    losers = df[(df[return_col]<=0) & (df[deployed_col].notna())] #removed reset_index to accomodate timegrouper 8/8/22
    losers = losers[losers[deployed_col]!=0]
    lose_dollars = losers.groupby(factor)[[return_col]].sum().reset_index()
    lose_dollars.rename(columns={return_col:'Loser_Dollars'},inplace=True)
    losemean = losers.groupby(factor)[[return_col]].mean().reset_index()
    losemean.rename(columns={return_col:'LoseMean'},inplace=True)
    lose_dollars = lose_dollars.merge(losemean)
    win_summary = pd.merge(win_summary,lose_dollars)  
    table = pd.merge(table,win_summary,how='left')

    tb = df[df[deployed_col]!=0]
    tb = tb.groupby(u_factor)[['SYMBOL']].nunique()
    tb.rename(columns={'SYMBOL':'SYMBOLS_Traded'},inplace=True)
    if any(isinstance(x, timegrouper) for x in factor):
        tb = tb.reset_index().set_index('Time')
    else:
        tb = tb.reset_index()
    tb = tb.groupby(factor)[['SYMBOLS_Traded']].sum().reset_index()
    table = pd.merge(table,tb,how='left')
    table = pd.merge(table,win_summary,how='outer')
    table['WinMean'] = table['Winner_Dollars'] / table['Win_Count']
    table['LoseMean'] =  table['Loser_Dollars'] / (table['SYMBOLS_Traded']-table['Win_Count'])
    table['Win_Rate'] = table['Win_Count']/table['SYMBOLS_Traded']
    table['Win_Ratio'] = table['WinMean']/np.absolute(table['LoseMean'])
    
    return table


# In[127]:


def set_hurdles_original(df, bp, freq, feature_list, target_bracket):
    i = 3
    features = [pd.Grouper(freq=freq)] + feature_list + [target_bracket]
    features_notime = ['Time'] + feature_list + [target_bracket]

    #Created the grouped data and set it up
    data_group = df.groupby(features).sum().reset_index()
    data_group['Count'] = df.groupby(features)['SYMBOL'].count().values
    data_group['Win_Count'] = df.groupby(features)['Imb_Size_Return'].apply(lambda x: ((x>0).sum())).values
    data_group['Win_Rate'] = df.groupby(features)['Imb_Size_Return'].apply(lambda x: ((x>0).sum())/x.count()).values
    keepcols = ['Imb_Size_Cap_Deployed','Imb_Size_Return'] + features_notime + ['Count','Win_Rate','Win_Count']
    data_group = data_group[keepcols]
    

    #Split the PRI brackets
    data_group[target_bracket] = data_group[target_bracket].astype(str)
    data_group[['PRI Low', 'PRI High']] = data_group[target_bracket].str.split(',', expand=True)
    data_group['PRI Low'] = data_group['PRI Low'].str.replace('(','')
    data_group['PRI High'] = data_group['PRI High'].str.replace(']','')
    data_group['PRI Low'] = data_group['PRI Low'].str.replace('-inf','-1').astype('float')

    sort = [False]
    j = 0
    t = len(features_notime[:-1])
    while j < t:
        sort.insert(0,True)
        j = j +1
        
    data_group[features_notime[-2]] = data_group[features_notime[-2]].astype(str)
    low_name, high_name = features_notime[-2] + 'Low', features_notime[-2] + 'High'
    data_group[[low_name, high_name]] = data_group[features_notime[-2]].str.split(',', expand=True)
    data_group[low_name] = data_group[low_name].str.replace('(','').astype('float')
    data_group[high_name] = data_group[high_name].str.replace(']','').astype('float')

    #Sort the data
    data_group = data_group.sort_values(features_notime[:-2] + [low_name] + ['PRI Low'], ascending=sort)

    #Remove the PRI brackets where PRI >= .1 and not < bp5
    #data_group = data_group[data_group['PRI Low']!='0.99']
    #data_group = data_group[data_group['PRI Low']!='0.1']
    #data_group = data_group[data_group['PRI Low']!='0.0']


    #Cut the -inf to 0 IADV
    #data_group = data_group[data_group[low_name]!='-inf']

    #Re-calc the Imb Size Cap Deployed Cumm
    data_group['Win_Rate_Cumm'] = data_group.groupby(features_notime[:-1])['Win_Count'].cumsum() / data_group.groupby(features_notime[:-1])['Count'].cumsum()
    data_group['Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1])['Imb_Size_Cap_Deployed'].cumsum()
    data_group['Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1])['Imb_Size_Return'].cumsum()
    data_group['Imb Size % Return Cumm'] = data_group['Imb Size Return Cumm'] / data_group['Imb Size Cap Deployed Cumm']
    data_group['Line above '+str(bp)+''] = np.where((data_group['Imb_Size_Return']/data_group['Imb_Size_Cap_Deployed'])>=bp,1,0)
    data_group['Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Line above '+str(bp)+''].cumsum()
    data_group['Line above '+str(bp)+' Cumm'] = np.where(data_group['Line above '+str(bp)+'']==1,data_group['Line above '+str(bp)+' Cumm']-1,data_group['Line above '+str(bp)+' Cumm'])

    #Calculate the return by potential hurdle spot
    data_group['Mariginal Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1]+ ['Line above '+str(bp)+' Cumm'])['Imb_Size_Cap_Deployed'].cumsum()
    data_group['Mariginal Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1] + ['Line above '+str(bp)+' Cumm'])['Imb_Size_Return'].cumsum()
    data_group['Mariginal Imb Size % Return Cumm'] = data_group['Mariginal Imb Size Return Cumm'] / data_group['Mariginal Imb Size Cap Deployed Cumm']
    data_group['Marginal Line above '+str(bp)+''] = np.where(data_group['Mariginal Imb Size % Return Cumm']>=bp,1,0)
    data_group['Marginal Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Marginal Line above '+str(bp)+''].cumsum()
    data_group['Marginal Line above '+str(bp)+' Cumm'] = np.where(data_group['Marginal Line above '+str(bp)+'']==1,data_group['Marginal Line above '+str(bp)+' Cumm']-1,data_group['Marginal Line above '+str(bp)+' Cumm'])
    data_group['Marginal 2 Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal Line above '+str(bp)+' Cumm'])['Imb_Size_Cap_Deployed'].cumsum()
    data_group['Marginal 2 Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal Line above '+str(bp)+' Cumm'])['Imb_Size_Return'].cumsum()
    data_group['Marginal 2 Imb Size % Return Cumm'] = data_group['Marginal 2 Imb Size Return Cumm'] / data_group['Marginal 2 Imb Size Cap Deployed Cumm']
    data_group['Marginal 2 Line above '+str(bp)+''] = np.where(data_group['Marginal 2 Imb Size % Return Cumm']>=bp,1,0)
    data_group['Marginal 2 Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Marginal 2 Line above '+str(bp)+''].cumsum()
    data_group['Marginal 2 Line above '+str(bp)+' Cumm'] = np.where(data_group['Marginal 2 Line above '+str(bp)+'']==1,data_group['Marginal 2 Line above '+str(bp)+' Cumm']-1,data_group['Marginal 2 Line above '+str(bp)+' Cumm'])

    while i < 15:
        data_group['Marginal ' + str(i) + ' Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal '+str(i-1)+' Line above '+str(bp)+' Cumm'])['Imb_Size_Cap_Deployed'].cumsum()
        data_group['Marginal ' + str(i) + ' Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal '+str(i-1)+' Line above '+str(bp)+' Cumm'])['Imb_Size_Return'].cumsum()
        data_group['Marginal ' + str(i) + ' Imb Size % Return Cumm'] = data_group['Marginal ' + str(i) + ' Imb Size Return Cumm'] / data_group['Marginal ' + str(i) + ' Imb Size Cap Deployed Cumm']
        data_group['Marginal ' + str(i) + ' Line above '+str(bp)+''] = np.where(data_group['Marginal ' + str(i) + ' Imb Size % Return Cumm']>=bp,1,0)
        data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Marginal ' + str(i) + ' Line above '+str(bp)+''].cumsum()
        data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm'] = np.where(data_group['Marginal ' + str(i) + ' Line above '+str(bp)+'']==1,data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm']-1,data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm'])
        i = i + 1

    #Reset index
    data_group = data_group.reset_index(drop=True)

    #Set up Hurdles Spots data
    data_hurdle_spots = data_group.groupby(features_notime[:-1])['Marginal 14 Line above '+str(bp)+' Cumm'].max()
    data_hurdle_spots = data_hurdle_spots.reset_index()
    data_hurdle_spots.rename(columns={'Marginal 14 Line above '+str(bp)+' Cumm':'Hurdle Spots'}, inplace=True)

    #Merge Hurdle Spots data
    data_group = data_group.merge(data_hurdle_spots, how='left', on=features_notime[:-1])
    data_group.rename(columns={'Hurdle Spots':'Hurdle Sum 14'}, inplace=True)

    #Narrow to the x hurdle
    data_group['Hurdle Sum Match'] = np.where(data_group['Marginal 14 Line above '+str(bp)+' Cumm']==data_group['Hurdle Sum 14'],1,0)

    data_group['Hurdle Sum Match Cumm'] = data_group.groupby(features_notime[:-1], as_index=False)['Hurdle Sum Match'].cumsum()

    data_group['one above x hurdle'] = np.where(data_group['Hurdle Sum Match Cumm']==1,1,0)
    #data_group['x hurdle'] = np.where(data_group['Hurdle Sum 14']==0,data_group['one above x hurdle'],np.where((data_group['one above x hurdle'].shift(-1)==1)&(data_group[features_notime[-2]].shift(-1)==data_group[features_notime[-2]]),1,0))
    #cond1 = data_group['Hurdle Sum 14']==0
    #cond2 = data_group['one above x hurdle'].shift(-1)==1
    #cond3 = is_same_lane
    #cond4 = cond2 & cond3
    #data_group['x hurdle'] = np.where(cond1,data_group['one above x hurdle'],np.where(cond4,1,0))
    data_group['is_same_lane_and_time'] = (data_group[features_notime[:-1]]==data_group[features_notime[:-1]].shift(-1)).sum(axis=1)==len(features_notime[:-1])
    #a1 = data_group['Hurdle Sum 14']==0
    #a2 = np.logical_and(data_group['is_same_lane_and_time']==False,  data_group['one above x hurdle']==1)
    #a3 = np.logical_and(data_group['is_same_lane_and_time']==True, data_group['one above x hurdle'].shift(-1)==1)    
    #vals = [data_group['one above x hurdle'],1,1]
    #data_group['x hurdle'] = np.select([a1,a2,a3],vals,'X')
    
    cond1 = data_group['x hurdle']==1
    cond2 = data_group['Hurdle Sum 14']==0
    cond3 = cond1 & cond2
    data_group['x hurdle'] = np.where(cond3,'Do Not Trade',data_group['x hurdle'])

    #Create y Hurdle column
    data_group['y hurdle'] = np.where(data_group['x hurdle']=='Do Not Trade','Do Not Trade','TBD')

    #Identify where the Cap Deployed is not 0 and group them
    data_group['Non-Zero Cap Deployed'] = np.where(data_group['Imb_Size_Cap_Deployed']==0,0,1)
    data_group['Non-Zero Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1])['Non-Zero Cap Deployed'].cumsum()

    #Set up data to identify the range of PRIs where the y hurdles are
    data_x_hurdles = data_group[(data_group['x hurdle']=='1')]
    #data_x_hurdles = data_x_hurdles.drop(columns={'PRI_Brackets','Imb_Size_Cap_Deployed','Imb_Size_Return','Positional_Cap_Deployed','Positional_Return','PRI Low','PRI High','IADV Low','IADV High','Imb Size Cap Deployed Cumm','Imb Size Return Cumm','Imb Size % Return Cumm','Line above 20 bps','Line above 20 bps Cumm','Mariginal Imb Size Cap Deployed Cumm','Mariginal Imb Size Return Cumm','Mariginal Imb Size % Return Cumm','Marginal Line above 20 bps','Marginal Line above 20 bps Cumm','Marginal 2 Imb Size Cap Deployed Cumm','Marginal 2 Imb Size Return Cumm','Marginal 2 Imb Size % Return Cumm','Marginal 2 Line above 20 bps','Marginal 2 Line above 20 bps Cumm','Marginal 3 Imb Size Cap Deployed Cumm','Marginal 3 Imb Size Return Cumm','Marginal 3 Imb Size % Return Cumm','Marginal 3 Line above 20 bps','Marginal 3 Line above 20 bps Cumm','Marginal 4 Imb Size Cap Deployed Cumm','Marginal 4 Imb Size Return Cumm','Marginal 4 Imb Size % Return Cumm','Marginal 4 Line above 20 bps','Marginal 4 Line above 20 bps Cumm','Marginal 5 Imb Size Cap Deployed Cumm','Marginal 5 Imb Size Return Cumm','Marginal 5 Imb Size % Return Cumm','Marginal 5 Line above 20 bps','Marginal 5 Line above 20 bps Cumm','Marginal 6 Imb Size Cap Deployed Cumm','Marginal 6 Imb Size Return Cumm','Marginal 6 Imb Size % Return Cumm','Marginal 6 Line above 20 bps','Marginal 6 Line above 20 bps Cumm','Marginal 7 Imb Size Cap Deployed Cumm','Marginal 7 Imb Size Return Cumm','Marginal 7 Imb Size % Return Cumm','Marginal 7 Line above 20 bps','Marginal 7 Line above 20 bps Cumm','Marginal 8 Imb Size Cap Deployed Cumm','Marginal 8 Imb Size Return Cumm','Marginal 8 Imb Size % Return Cumm','Marginal 8 Line above 20 bps','Marginal 8 Line above 20 bps Cumm','Marginal 9 Imb Size Cap Deployed Cumm','Marginal 9 Imb Size Return Cumm','Marginal 9 Imb Size % Return Cumm','Marginal 9 Line above 20 bps','Marginal 9 Line above 20 bps Cumm','Marginal 10 Imb Size Cap Deployed Cumm','Marginal 10 Imb Size Return Cumm','Marginal 10 Imb Size % Return Cumm','Marginal 10 Line above 20 bps','Marginal 10 Line above 20 bps Cumm','Marginal 11 Imb Size Cap Deployed Cumm','Marginal 11 Imb Size Return Cumm','Marginal 11 Imb Size % Return Cumm','Marginal 11 Line above 20 bps','Marginal 11 Line above 20 bps Cumm','Marginal 12 Imb Size Cap Deployed Cumm','Marginal 12 Imb Size Return Cumm','Marginal 12 Imb Size % Return Cumm','Marginal 12 Line above 20 bps','Marginal 12 Line above 20 bps Cumm','Marginal 13 Imb Size Cap Deployed Cumm','Marginal 13 Imb Size Return Cumm','Marginal 13 Imb Size % Return Cumm','Marginal 13 Line above 20 bps','Marginal 13 Line above 20 bps Cumm','Marginal 14 Imb Size Cap Deployed Cumm','Marginal 14 Imb Size Return Cumm','Marginal 14 Imb Size % Return Cumm','Marginal 14 Line above 20 bps','Marginal 14 Line above 20 bps Cumm','Hurdle Sum 14','Hurdle Sum Match','Hurdle Sum Match Cumm','one above x hurdle','x hurdle','y hurdle','Non-Zero Cap Deployed'}, axis=1)
    data_x_hurdles = data_x_hurdles[features_notime[:-1] + ['Non-Zero Cap Deployed Cumm']]
    data_x_hurdles.rename(columns={'Non-Zero Cap Deployed Cumm':'range y hurdle'}, inplace=True)

    #Subtract 1 from the y hurdle range
    data_x_hurdles['range y hurdle'] = data_x_hurdles['range y hurdle']+1

    #merge y hurdle range data with the main data
    data_group = data_group.merge(data_x_hurdles, how='left', on=features_notime[:-1])

    #Create a column that shows where the Non-Zero Cap Deployed Cumm matches the range y hurdle
    data_group['range y hurdle match'] = np.where(data_group['range y hurdle']==data_group['Non-Zero Cap Deployed Cumm'],1,0)
    data_group['range y hurdle match cumm'] = data_group.groupby(features_notime[:-1], as_index=False)['range y hurdle match'].cumsum()

    data_y_hurdle_spots = data_group[data_group['range y hurdle match']==1]
    data_y_hurdle_spots = data_y_hurdle_spots[data_y_hurdle_spots['range y hurdle match cumm']==1]

    
    #Delete excess columns
    #rename 'range y hurdle match cumm' as 'y hurdle match'
    #data_y_hurdle_spots = data_y_hurdle_spots.drop(columns={'Imb_Size_Cap_Deployed','Imb_Size_Return','Positional_Cap_Deployed','Positional_Return','PRI Low','PRI High','IADV Low','IADV High','Imb Size Cap Deployed Cumm','Imb Size Return Cumm','Imb Size % Return Cumm','Line above 20 bps','Line above 20 bps Cumm','Mariginal Imb Size Cap Deployed Cumm','Mariginal Imb Size Return Cumm','Mariginal Imb Size % Return Cumm','Marginal Line above 20 bps','Marginal Line above 20 bps Cumm','Marginal 2 Imb Size Cap Deployed Cumm','Marginal 2 Imb Size Return Cumm','Marginal 2 Imb Size % Return Cumm','Marginal 2 Line above 20 bps','Marginal 2 Line above 20 bps Cumm','Marginal 3 Imb Size Cap Deployed Cumm','Marginal 3 Imb Size Return Cumm','Marginal 3 Imb Size % Return Cumm','Marginal 3 Line above 20 bps','Marginal 3 Line above 20 bps Cumm','Marginal 4 Imb Size Cap Deployed Cumm','Marginal 4 Imb Size Return Cumm','Marginal 4 Imb Size % Return Cumm','Marginal 4 Line above 20 bps','Marginal 4 Line above 20 bps Cumm','Marginal 5 Imb Size Cap Deployed Cumm','Marginal 5 Imb Size Return Cumm','Marginal 5 Imb Size % Return Cumm','Marginal 5 Line above 20 bps','Marginal 5 Line above 20 bps Cumm','Marginal 6 Imb Size Cap Deployed Cumm','Marginal 6 Imb Size Return Cumm','Marginal 6 Imb Size % Return Cumm','Marginal 6 Line above 20 bps','Marginal 6 Line above 20 bps Cumm','Marginal 7 Imb Size Cap Deployed Cumm','Marginal 7 Imb Size Return Cumm','Marginal 7 Imb Size % Return Cumm','Marginal 7 Line above 20 bps','Marginal 7 Line above 20 bps Cumm','Marginal 8 Imb Size Cap Deployed Cumm','Marginal 8 Imb Size Return Cumm','Marginal 8 Imb Size % Return Cumm','Marginal 8 Line above 20 bps','Marginal 8 Line above 20 bps Cumm','Marginal 9 Imb Size Cap Deployed Cumm','Marginal 9 Imb Size Return Cumm','Marginal 9 Imb Size % Return Cumm','Marginal 9 Line above 20 bps','Marginal 9 Line above 20 bps Cumm','Marginal 10 Imb Size Cap Deployed Cumm','Marginal 10 Imb Size Return Cumm','Marginal 10 Imb Size % Return Cumm','Marginal 10 Line above 20 bps','Marginal 10 Line above 20 bps Cumm','Marginal 11 Imb Size Cap Deployed Cumm','Marginal 11 Imb Size Return Cumm','Marginal 11 Imb Size % Return Cumm','Marginal 11 Line above 20 bps','Marginal 11 Line above 20 bps Cumm','Marginal 12 Imb Size Cap Deployed Cumm','Marginal 12 Imb Size Return Cumm','Marginal 12 Imb Size % Return Cumm','Marginal 12 Line above 20 bps','Marginal 12 Line above 20 bps Cumm','Marginal 13 Imb Size Cap Deployed Cumm','Marginal 13 Imb Size Return Cumm','Marginal 13 Imb Size % Return Cumm','Marginal 13 Line above 20 bps','Marginal 13 Line above 20 bps Cumm','Marginal 14 Imb Size Cap Deployed Cumm','Marginal 14 Imb Size Return Cumm','Marginal 14 Imb Size % Return Cumm','Marginal 14 Line above 20 bps','Marginal 14 Line above 20 bps Cumm','Hurdle Sum 14','Hurdle Sum Match','Hurdle Sum Match Cumm','one above x hurdle','x hurdle','y hurdle','Non-Zero Cap Deployed','Non-Zero Cap Deployed Cumm','range y hurdle','range y hurdle match'}, axis=1)
    data_y_hurdle_spots = data_y_hurdle_spots[features_notime +['range y hurdle match cumm']]
    data_y_hurdle_spots.rename(columns={'range y hurdle match cumm':'y hurdle match'}, inplace=True)

    #merge with main data
    data_group = data_group.merge(data_y_hurdle_spots, how='left', on=features_notime)

    #Create a data of the max Imb Size Return Cumm per IADV bracket
    max_deployed = data_group.groupby(features_notime[:-1], as_index=False)['Imb Size Return Cumm'].max()
    #Rename column
    max_deployed.rename(columns={'Imb Size Return Cumm':'max cap deployed'}, inplace=True)
    #Merge with main data
    data_group = data_group.merge(max_deployed, how='left', on=features_notime[:-1])

    #Create a data of the max Non-Zero Cap Deployed Cumm per IADV bracket
    max_non_zero = data_group.groupby(features_notime[:-1], as_index=False)['Non-Zero Cap Deployed Cumm'].max()
    #Rename column
    max_non_zero.rename(columns={'Non-Zero Cap Deployed Cumm':'Max Non-Zero Cap Deployed Cumm'}, inplace=True)
    #Merge with main data
    data_group = data_group.merge(max_non_zero, how='left', on=features_notime[:-1])

    #Make a column for where the Non-Zero Cap Deployed Cumm is equal to the Max Non-Zero Cap Deployed Cumm
    data_group['Non-Zero Cap Deployed Cumm Match'] = np.where(data_group['Non-Zero Cap Deployed Cumm']==data_group['Max Non-Zero Cap Deployed Cumm'],1,0)

    #Make data where the x hurdle is the Max Non-Zero Cap Deployed Cumm
    x_hurdle_max = data_group[(data_group['x hurdle']=='1')&(data_group['Non-Zero Cap Deployed Cumm Match']==1)]
    #Drop unnecessary columns
    #x_hurdle_max = x_hurdle_max.drop(columns={'PRI_Brackets','Imb_Size_Cap_Deployed','Imb_Size_Return','Positional_Cap_Deployed','Positional_Return','PRI Low','PRI High','IADV Low','IADV High','Imb Size Cap Deployed Cumm','Imb Size Return Cumm','Imb Size % Return Cumm','Line above 20 bps','Line above 20 bps Cumm','Mariginal Imb Size Cap Deployed Cumm','Mariginal Imb Size Return Cumm','Mariginal Imb Size % Return Cumm','Marginal Line above 20 bps','Marginal Line above 20 bps Cumm','Marginal 2 Imb Size Cap Deployed Cumm','Marginal 2 Imb Size Return Cumm','Marginal 2 Imb Size % Return Cumm','Marginal 2 Line above 20 bps','Marginal 2 Line above 20 bps Cumm','Marginal 3 Imb Size Cap Deployed Cumm','Marginal 3 Imb Size Return Cumm','Marginal 3 Imb Size % Return Cumm','Marginal 3 Line above 20 bps','Marginal 3 Line above 20 bps Cumm','Marginal 4 Imb Size Cap Deployed Cumm','Marginal 4 Imb Size Return Cumm','Marginal 4 Imb Size % Return Cumm','Marginal 4 Line above 20 bps','Marginal 4 Line above 20 bps Cumm','Marginal 5 Imb Size Cap Deployed Cumm','Marginal 5 Imb Size Return Cumm','Marginal 5 Imb Size % Return Cumm','Marginal 5 Line above 20 bps','Marginal 5 Line above 20 bps Cumm','Marginal 6 Imb Size Cap Deployed Cumm','Marginal 6 Imb Size Return Cumm','Marginal 6 Imb Size % Return Cumm','Marginal 6 Line above 20 bps','Marginal 6 Line above 20 bps Cumm','Marginal 7 Imb Size % Return Cumm','Marginal 7 Line above 20 bps','Marginal 7 Imb Size Cap Deployed Cumm','Marginal 7 Imb Size Return Cumm','Marginal 7 Line above 20 bps Cumm','Marginal 8 Imb Size Cap Deployed Cumm','Marginal 8 Imb Size Return Cumm','Marginal 8 Imb Size % Return Cumm','Marginal 8 Line above 20 bps','Marginal 8 Line above 20 bps Cumm','Marginal 9 Imb Size Cap Deployed Cumm','Marginal 9 Imb Size Return Cumm','Marginal 9 Imb Size % Return Cumm','Marginal 9 Line above 20 bps','Marginal 9 Line above 20 bps Cumm','Marginal 10 Imb Size Cap Deployed Cumm','Marginal 10 Imb Size Return Cumm','Marginal 10 Imb Size % Return Cumm','Marginal 10 Line above 20 bps','Marginal 10 Line above 20 bps Cumm','Marginal 11 Imb Size Cap Deployed Cumm','Marginal 11 Imb Size Return Cumm','Marginal 11 Imb Size % Return Cumm','Marginal 11 Line above 20 bps','Marginal 11 Line above 20 bps Cumm','Marginal 12 Imb Size Cap Deployed Cumm','Marginal 12 Imb Size Return Cumm','Marginal 12 Imb Size % Return Cumm','Marginal 12 Line above 20 bps','Marginal 12 Line above 20 bps Cumm','Marginal 13 Imb Size Cap Deployed Cumm','Marginal 13 Imb Size Return Cumm','Marginal 13 Imb Size % Return Cumm','Marginal 13 Line above 20 bps','Marginal 13 Line above 20 bps Cumm','Marginal 14 Imb Size Cap Deployed Cumm','Marginal 14 Imb Size Return Cumm','Marginal 14 Imb Size % Return Cumm','Marginal 14 Line above 20 bps','Marginal 14 Line above 20 bps Cumm','Hurdle Sum 14','Hurdle Sum Match','Hurdle Sum Match Cumm','one above x hurdle','x hurdle','y hurdle','Non-Zero Cap Deployed','Non-Zero Cap Deployed Cumm','range y hurdle','range y hurdle match','range y hurdle match cumm','max cap deployed','y hurdle match','Max Non-Zero Cap Deployed Cumm'}, axis=1)
    x_hurdle_max = x_hurdle_max[features_notime[:-1] + ['Non-Zero Cap Deployed Cumm Match']]
    #Rename the column
    x_hurdle_max.rename(columns={'Non-Zero Cap Deployed Cumm Match':'x hurdle is max non-zero cap deployed'}, inplace=True)
    #Merge with main data
    data_group = data_group.merge(x_hurdle_max, how='left', on=features_notime[:-1])

    #Set the y hurdle
    data_group['y hurdle'] = 0
    data_group['y hurdle'] = np.where(data_group['x hurdle']=='Do Not Trade','Do Not Trade',np.where((data_group['x hurdle'].shift(1)=='1')&(data_group['Non-Zero Cap Deployed']==1),1,np.where(data_group['y hurdle match']==1,1,np.where((data_group['x hurdle is max non-zero cap deployed']==1)&(data_group['PRI Low']=='-inf'),1,0))))

    #Need to correct where the x and y hurdles are 'Do Not Trade', but the max cap deployed for that IADV bucket is 0 (should be 'no data for hurdle')
    data_group['x hurdle'] = np.where((data_group['x hurdle']=='Do Not Trade')&(data_group['max cap deployed']==0),'no data for hurdle',data_group['x hurdle'])
    data_group['y hurdle'] = np.where((data_group['x hurdle']=='no data for hurdle'),'no data for hurdle',data_group['y hurdle'])

    #Create df with just the hurdle spots
    df_hurdles_set = data_group[(data_group['x hurdle']!='0')|(data_group['y hurdle']=='0')]
    
    df_hurdle_lines = df_hurdles_set[features_notime[:-1] + ['PRI Low','x hurdle','y hurdle']]
    df_hurdle_lines = df_hurdle_lines[df_hurdle_lines['x hurdle']!='0']
    df_hurdle_lines['PRI Low'] = np.where(df_hurdle_lines['x hurdle']=='Do Not Trade',1,df_hurdle_lines['PRI Low'])
    df_hurdle_lines = df_hurdle_lines.sort_values('Time')
    
    return df_hurdle_lines, df_hurdles_set, data_group, bp


# In[128]:


def set_hurdles(df, bp: float, freq: str, feature_list: list, target_bracket: str, target_sort=False, do_not_trade_val=1):
    """df is your dataframe. Dataframe needs to have a datetime index
    bp is the basis point cut expressed as a float i.e. .002
    freq is an input for the pd.Grouper. i.e. '600s'
    feature_list must have atleast 3 factors. If you using less than 3 features include dummy features at the beginning
    target_bracket is a string for the column name of the target bracket i.e. 'Per_Return_Exp'
    """
    i = 3
    features = [pd.Grouper(freq=freq)] + feature_list + [target_bracket]
    features_notime = ['Time'] + feature_list + [target_bracket]

    #Created the grouped data and set it up
    data_group = df.groupby(features).sum().reset_index()
    data_group['Count'] = df.groupby(features)['SYMBOL'].count().values
    data_group['Win_Count'] = df.groupby(features)['Imb_Size_Return'].apply(lambda x: ((x>0).sum())).values
    data_group['Win_Rate'] = df.groupby(features)['Imb_Size_Return'].apply(lambda x: ((x>0).sum())/x.count()).values
    keepcols = ['Imb_Size_Cap_Deployed','Imb_Size_Return'] + features_notime + ['Count','Win_Rate','Win_Count']
    data_group = data_group[keepcols]


    #Split the PRI brackets
    data_group[target_bracket] = data_group[target_bracket].astype(str)
    data_group[['PRI Low', 'PRI High']] = data_group[target_bracket].str.split(',', expand=True)
    data_group['PRI Low'] = data_group['PRI Low'].str.replace('(','')
    data_group['PRI High'] = data_group['PRI High'].str.replace(']','')
    data_group['PRI Low'] = data_group['PRI Low'].str.replace('-inf','-1').astype('float')
 
    sort = [target_sort]
    j = 0
    t = len(features_notime[:-1])
    while j < t:
        sort.insert(0,True)
        j = j +1

    data_group[features_notime[-2]] = data_group[features_notime[-2]].astype(str)
    low_name, high_name = features_notime[-2] + 'Low', features_notime[-2] + 'High'
    data_group[[low_name, high_name]] = data_group[features_notime[-2]].str.split(',', expand=True)
    data_group[low_name] = data_group[low_name].str.replace('(','').astype('float')
    data_group[high_name] = data_group[high_name].str.replace(']','').astype('float')

    #Sort the data
    data_group = data_group.sort_values(features_notime[:-2] + [low_name] + ['PRI Low'], ascending=sort)

    #Remove the PRI brackets where PRI >= .1 and not < bp5
    #data_group = data_group[data_group['PRI Low']!='0.99']
    #data_group = data_group[data_group['PRI Low']!='0.1']
    #data_group = data_group[data_group['PRI Low']!='0.0']


    #Cut the -inf to 0 IADV
    #data_group = data_group[data_group[low_name]!='-inf']

    #Re-calc the Imb Size Cap Deployed Cumm
    data_group['Win_Rate_Cumm'] = data_group.groupby(features_notime[:-1])['Win_Count'].cumsum() / data_group.groupby(features_notime[:-1])['Count'].cumsum()
    data_group['Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1])['Imb_Size_Cap_Deployed'].cumsum()
    data_group['Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1])['Imb_Size_Return'].cumsum()
    data_group['Imb Size % Return Cumm'] = data_group['Imb Size Return Cumm'] / data_group['Imb Size Cap Deployed Cumm']
    data_group['Line above '+str(bp)+''] = np.where((data_group['Imb_Size_Return']/data_group['Imb_Size_Cap_Deployed'])>=bp,1,0)
    data_group['Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Line above '+str(bp)+''].cumsum()
    data_group['Line above '+str(bp)+' Cumm'] = np.where(data_group['Line above '+str(bp)+'']==1,data_group['Line above '+str(bp)+' Cumm']-1,data_group['Line above '+str(bp)+' Cumm'])

    #Calculate the return by potential hurdle spot
    data_group['Mariginal Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1]+ ['Line above '+str(bp)+' Cumm'])['Imb_Size_Cap_Deployed'].cumsum()
    data_group['Mariginal Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1] + ['Line above '+str(bp)+' Cumm'])['Imb_Size_Return'].cumsum()
    data_group['Mariginal Imb Size % Return Cumm'] = data_group['Mariginal Imb Size Return Cumm'] / data_group['Mariginal Imb Size Cap Deployed Cumm']
    data_group['Marginal Line above '+str(bp)+''] = np.where(data_group['Mariginal Imb Size % Return Cumm']>=bp,1,0)
    data_group['Marginal Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Marginal Line above '+str(bp)+''].cumsum()
    data_group['Marginal Line above '+str(bp)+' Cumm'] = np.where(data_group['Marginal Line above '+str(bp)+'']==1,data_group['Marginal Line above '+str(bp)+' Cumm']-1,data_group['Marginal Line above '+str(bp)+' Cumm'])
    data_group['Marginal 2 Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal Line above '+str(bp)+' Cumm'])['Imb_Size_Cap_Deployed'].cumsum()
    data_group['Marginal 2 Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal Line above '+str(bp)+' Cumm'])['Imb_Size_Return'].cumsum()
    data_group['Marginal 2 Imb Size % Return Cumm'] = data_group['Marginal 2 Imb Size Return Cumm'] / data_group['Marginal 2 Imb Size Cap Deployed Cumm']
    data_group['Marginal 2 Line above '+str(bp)+''] = np.where(data_group['Marginal 2 Imb Size % Return Cumm']>=bp,1,0)
    data_group['Marginal 2 Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Marginal 2 Line above '+str(bp)+''].cumsum()
    data_group['Marginal 2 Line above '+str(bp)+' Cumm'] = np.where(data_group['Marginal 2 Line above '+str(bp)+'']==1,data_group['Marginal 2 Line above '+str(bp)+' Cumm']-1,data_group['Marginal 2 Line above '+str(bp)+' Cumm'])

    while i < 15:
        data_group['Marginal ' + str(i) + ' Imb Size Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal '+str(i-1)+' Line above '+str(bp)+' Cumm'])['Imb_Size_Cap_Deployed'].cumsum()
        data_group['Marginal ' + str(i) + ' Imb Size Return Cumm'] = data_group.groupby(features_notime[:-1] + ['Marginal '+str(i-1)+' Line above '+str(bp)+' Cumm'])['Imb_Size_Return'].cumsum()
        data_group['Marginal ' + str(i) + ' Imb Size % Return Cumm'] = data_group['Marginal ' + str(i) + ' Imb Size Return Cumm'] / data_group['Marginal ' + str(i) + ' Imb Size Cap Deployed Cumm']
        data_group['Marginal ' + str(i) + ' Line above '+str(bp)+''] = np.where(data_group['Marginal ' + str(i) + ' Imb Size % Return Cumm']>=bp,1,0)
        data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm'] = data_group.groupby(features_notime[:-1])['Marginal ' + str(i) + ' Line above '+str(bp)+''].cumsum()
        data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm'] = np.where(data_group['Marginal ' + str(i) + ' Line above '+str(bp)+'']==1,data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm']-1,data_group['Marginal ' + str(i) + ' Line above '+str(bp)+' Cumm'])
        i = i + 1

    #Reset index
    data_group = data_group.reset_index(drop=True)

    #check if cell below is in the same lane    
    data_group['Time_String'] = data_group['Time'].dt.time.astype(str)
    data_group['Features_with_Time'] = data_group[['Time_String']+features_notime[1:-1]].sum(axis=1)
    data_group['Features_with_Time_Shift'] = data_group['Features_with_Time'].shift(-1)
    is_same_lane = data_group['Features_with_Time']==data_group['Features_with_Time_Shift']
    data_group['is_same_lane_and_time'] = is_same_lane
    #Add one to Marginal 14 Cumm if the individual line is > min_pri and it is the last line
    cond = (data_group['Marginal 14 Line above '+str(bp)+'']==1) & (is_same_lane==False)
    data_group['Marginal 14 Line above '+str(bp)+' Cumm'] = np.where(cond,data_group['Marginal 14 Line above '+str(bp)+' Cumm']+1,data_group['Marginal 14 Line above '+str(bp)+' Cumm'])

    #Set up Hurdles Spots data
    data_hurdle_spots = data_group.groupby(features_notime[:-1])['Marginal 14 Line above '+str(bp)+' Cumm'].max()
    data_hurdle_spots = data_hurdle_spots.reset_index()
    data_hurdle_spots.rename(columns={'Marginal 14 Line above '+str(bp)+' Cumm':'Hurdle Spots'}, inplace=True)

    #Merge Hurdle Spots data
    data_group = data_group.merge(data_hurdle_spots, how='left', on=features_notime[:-1])
    data_group.rename(columns={'Hurdle Spots':'Hurdle Sum 14'}, inplace=True)

    #Narrow to the x hurdle
    data_group['Hurdle Sum Match'] = np.where(data_group['Marginal 14 Line above '+str(bp)+' Cumm']==data_group['Hurdle Sum 14'],1,0)
    #data_group['Hurdle Sum Match'] = np.where(data_group['Marginal 14 Line above '+str(bp)+'']==0,0,data_group['Hurdle Sum Match'])
    data_group['Hurdle Sum Match Cumm'] = data_group.groupby(features_notime[:-1], as_index=False)['Hurdle Sum Match'].cumsum()

    data_group['one above x hurdle'] = np.where(data_group['Hurdle Sum Match Cumm']==1,1,0)
    #data_group['x hurdle'] = np.where(data_group['Hurdle Sum 14']==0,data_group['one above x hurdle'],np.where((data_group['one above x hurdle'].shift(-1)==1)&(data_group[features_notime[-2]].shift(-1)==data_group[features_notime[-2]]),1,0))

    cond1 = data_group['Hurdle Sum 14']==0
    data_group['x hurdle'] = np.where(cond1,data_group['one above x hurdle'],0)

    data_group['one above x hurdle is last'] = (data_group['is_same_lane_and_time']==False) & (data_group['one above x hurdle']==1) & (data_group['Marginal 14 Line above '+str(bp)+'']==1)
    cond2 = data_group['one above x hurdle is last']==True
    data_group['x hurdle'] = np.where(cond2,1,data_group['x hurdle'])

    data_group['x hurdle max'] = data_group.groupby(features_notime[:-1])['x hurdle'].transform('max')
    data_group['check2'] = data_group['one above x hurdle'].shift(-1)==1
    data_group['check3'] = data_group['x hurdle max']==0
    data_group['one above x hurdle is valid'] = (data_group['is_same_lane_and_time']==True) & (data_group['check2']==True) & (data_group['check3']==True)
    cond3 = data_group['one above x hurdle is valid']==True
    data_group['x hurdle'] = np.where(cond3,1,data_group['x hurdle'])

    cond = (data_group['x hurdle']==1) & (data_group['Hurdle Sum 14']==0) & (data_group['Marginal 14 Line above '+str(bp)+'']==0)
    data_group['x hurdle'] = np.where(cond,'Do Not Trade',data_group['x hurdle'])
    
    #Create y Hurdle column
    data_group['y hurdle'] = np.where(data_group['x hurdle']=='Do Not Trade','Do Not Trade','TBD')

    #Identify where the Cap Deployed is not 0 and group them
    data_group['Non-Zero Cap Deployed'] = np.where(data_group['Imb_Size_Cap_Deployed']==0,0,1)
    data_group['Non-Zero Cap Deployed Cumm'] = data_group.groupby(features_notime[:-1])['Non-Zero Cap Deployed'].cumsum()

    #Set up data to identify the range of PRIs where the y hurdles are
    data_x_hurdles = data_group[(data_group['x hurdle']=='1')]
    #data_x_hurdles = data_x_hurdles.drop(columns={'PRI_Brackets','Imb_Size_Cap_Deployed','Imb_Size_Return','Positional_Cap_Deployed','Positional_Return','PRI Low','PRI High','IADV Low','IADV High','Imb Size Cap Deployed Cumm','Imb Size Return Cumm','Imb Size % Return Cumm','Line above 20 bps','Line above 20 bps Cumm','Mariginal Imb Size Cap Deployed Cumm','Mariginal Imb Size Return Cumm','Mariginal Imb Size % Return Cumm','Marginal Line above 20 bps','Marginal Line above 20 bps Cumm','Marginal 2 Imb Size Cap Deployed Cumm','Marginal 2 Imb Size Return Cumm','Marginal 2 Imb Size % Return Cumm','Marginal 2 Line above 20 bps','Marginal 2 Line above 20 bps Cumm','Marginal 3 Imb Size Cap Deployed Cumm','Marginal 3 Imb Size Return Cumm','Marginal 3 Imb Size % Return Cumm','Marginal 3 Line above 20 bps','Marginal 3 Line above 20 bps Cumm','Marginal 4 Imb Size Cap Deployed Cumm','Marginal 4 Imb Size Return Cumm','Marginal 4 Imb Size % Return Cumm','Marginal 4 Line above 20 bps','Marginal 4 Line above 20 bps Cumm','Marginal 5 Imb Size Cap Deployed Cumm','Marginal 5 Imb Size Return Cumm','Marginal 5 Imb Size % Return Cumm','Marginal 5 Line above 20 bps','Marginal 5 Line above 20 bps Cumm','Marginal 6 Imb Size Cap Deployed Cumm','Marginal 6 Imb Size Return Cumm','Marginal 6 Imb Size % Return Cumm','Marginal 6 Line above 20 bps','Marginal 6 Line above 20 bps Cumm','Marginal 7 Imb Size Cap Deployed Cumm','Marginal 7 Imb Size Return Cumm','Marginal 7 Imb Size % Return Cumm','Marginal 7 Line above 20 bps','Marginal 7 Line above 20 bps Cumm','Marginal 8 Imb Size Cap Deployed Cumm','Marginal 8 Imb Size Return Cumm','Marginal 8 Imb Size % Return Cumm','Marginal 8 Line above 20 bps','Marginal 8 Line above 20 bps Cumm','Marginal 9 Imb Size Cap Deployed Cumm','Marginal 9 Imb Size Return Cumm','Marginal 9 Imb Size % Return Cumm','Marginal 9 Line above 20 bps','Marginal 9 Line above 20 bps Cumm','Marginal 10 Imb Size Cap Deployed Cumm','Marginal 10 Imb Size Return Cumm','Marginal 10 Imb Size % Return Cumm','Marginal 10 Line above 20 bps','Marginal 10 Line above 20 bps Cumm','Marginal 11 Imb Size Cap Deployed Cumm','Marginal 11 Imb Size Return Cumm','Marginal 11 Imb Size % Return Cumm','Marginal 11 Line above 20 bps','Marginal 11 Line above 20 bps Cumm','Marginal 12 Imb Size Cap Deployed Cumm','Marginal 12 Imb Size Return Cumm','Marginal 12 Imb Size % Return Cumm','Marginal 12 Line above 20 bps','Marginal 12 Line above 20 bps Cumm','Marginal 13 Imb Size Cap Deployed Cumm','Marginal 13 Imb Size Return Cumm','Marginal 13 Imb Size % Return Cumm','Marginal 13 Line above 20 bps','Marginal 13 Line above 20 bps Cumm','Marginal 14 Imb Size Cap Deployed Cumm','Marginal 14 Imb Size Return Cumm','Marginal 14 Imb Size % Return Cumm','Marginal 14 Line above 20 bps','Marginal 14 Line above 20 bps Cumm','Hurdle Sum 14','Hurdle Sum Match','Hurdle Sum Match Cumm','one above x hurdle','x hurdle','y hurdle','Non-Zero Cap Deployed'}, axis=1)
    data_x_hurdles = data_x_hurdles[features_notime[:-1] + ['Non-Zero Cap Deployed Cumm']]
    data_x_hurdles.rename(columns={'Non-Zero Cap Deployed Cumm':'range y hurdle'}, inplace=True)

    #Subtract 1 from the y hurdle range
    data_x_hurdles['range y hurdle'] = data_x_hurdles['range y hurdle']+1

    #merge y hurdle range data with the main data
    data_group = data_group.merge(data_x_hurdles, how='left', on=features_notime[:-1])

    #Create a column that shows where the Non-Zero Cap Deployed Cumm matches the range y hurdle
    data_group['range y hurdle match'] = np.where(data_group['range y hurdle']==data_group['Non-Zero Cap Deployed Cumm'],1,0)
    data_group['range y hurdle match cumm'] = data_group.groupby(features_notime[:-1], as_index=False)['range y hurdle match'].cumsum()

    data_y_hurdle_spots = data_group[data_group['range y hurdle match']==1]
    data_y_hurdle_spots = data_y_hurdle_spots[data_y_hurdle_spots['range y hurdle match cumm']==1]

    
    #Delete excess columns
    #rename 'range y hurdle match cumm' as 'y hurdle match'
    #data_y_hurdle_spots = data_y_hurdle_spots.drop(columns={'Imb_Size_Cap_Deployed','Imb_Size_Return','Positional_Cap_Deployed','Positional_Return','PRI Low','PRI High','IADV Low','IADV High','Imb Size Cap Deployed Cumm','Imb Size Return Cumm','Imb Size % Return Cumm','Line above 20 bps','Line above 20 bps Cumm','Mariginal Imb Size Cap Deployed Cumm','Mariginal Imb Size Return Cumm','Mariginal Imb Size % Return Cumm','Marginal Line above 20 bps','Marginal Line above 20 bps Cumm','Marginal 2 Imb Size Cap Deployed Cumm','Marginal 2 Imb Size Return Cumm','Marginal 2 Imb Size % Return Cumm','Marginal 2 Line above 20 bps','Marginal 2 Line above 20 bps Cumm','Marginal 3 Imb Size Cap Deployed Cumm','Marginal 3 Imb Size Return Cumm','Marginal 3 Imb Size % Return Cumm','Marginal 3 Line above 20 bps','Marginal 3 Line above 20 bps Cumm','Marginal 4 Imb Size Cap Deployed Cumm','Marginal 4 Imb Size Return Cumm','Marginal 4 Imb Size % Return Cumm','Marginal 4 Line above 20 bps','Marginal 4 Line above 20 bps Cumm','Marginal 5 Imb Size Cap Deployed Cumm','Marginal 5 Imb Size Return Cumm','Marginal 5 Imb Size % Return Cumm','Marginal 5 Line above 20 bps','Marginal 5 Line above 20 bps Cumm','Marginal 6 Imb Size Cap Deployed Cumm','Marginal 6 Imb Size Return Cumm','Marginal 6 Imb Size % Return Cumm','Marginal 6 Line above 20 bps','Marginal 6 Line above 20 bps Cumm','Marginal 7 Imb Size Cap Deployed Cumm','Marginal 7 Imb Size Return Cumm','Marginal 7 Imb Size % Return Cumm','Marginal 7 Line above 20 bps','Marginal 7 Line above 20 bps Cumm','Marginal 8 Imb Size Cap Deployed Cumm','Marginal 8 Imb Size Return Cumm','Marginal 8 Imb Size % Return Cumm','Marginal 8 Line above 20 bps','Marginal 8 Line above 20 bps Cumm','Marginal 9 Imb Size Cap Deployed Cumm','Marginal 9 Imb Size Return Cumm','Marginal 9 Imb Size % Return Cumm','Marginal 9 Line above 20 bps','Marginal 9 Line above 20 bps Cumm','Marginal 10 Imb Size Cap Deployed Cumm','Marginal 10 Imb Size Return Cumm','Marginal 10 Imb Size % Return Cumm','Marginal 10 Line above 20 bps','Marginal 10 Line above 20 bps Cumm','Marginal 11 Imb Size Cap Deployed Cumm','Marginal 11 Imb Size Return Cumm','Marginal 11 Imb Size % Return Cumm','Marginal 11 Line above 20 bps','Marginal 11 Line above 20 bps Cumm','Marginal 12 Imb Size Cap Deployed Cumm','Marginal 12 Imb Size Return Cumm','Marginal 12 Imb Size % Return Cumm','Marginal 12 Line above 20 bps','Marginal 12 Line above 20 bps Cumm','Marginal 13 Imb Size Cap Deployed Cumm','Marginal 13 Imb Size Return Cumm','Marginal 13 Imb Size % Return Cumm','Marginal 13 Line above 20 bps','Marginal 13 Line above 20 bps Cumm','Marginal 14 Imb Size Cap Deployed Cumm','Marginal 14 Imb Size Return Cumm','Marginal 14 Imb Size % Return Cumm','Marginal 14 Line above 20 bps','Marginal 14 Line above 20 bps Cumm','Hurdle Sum 14','Hurdle Sum Match','Hurdle Sum Match Cumm','one above x hurdle','x hurdle','y hurdle','Non-Zero Cap Deployed','Non-Zero Cap Deployed Cumm','range y hurdle','range y hurdle match'}, axis=1)
    data_y_hurdle_spots = data_y_hurdle_spots[features_notime +['range y hurdle match cumm']]
    data_y_hurdle_spots.rename(columns={'range y hurdle match cumm':'y hurdle match'}, inplace=True)

    #merge with main data
    data_group = data_group.merge(data_y_hurdle_spots, how='left', on=features_notime)

    #Create a data of the max Imb Size Return Cumm per IADV bracket
    max_deployed = data_group.groupby(features_notime[:-1], as_index=False)['Imb Size Return Cumm'].max()
    #Rename column
    max_deployed.rename(columns={'Imb Size Return Cumm':'max cap deployed'}, inplace=True)
    #Merge with main data
    data_group = data_group.merge(max_deployed, how='left', on=features_notime[:-1])

    #Create a data of the max Non-Zero Cap Deployed Cumm per IADV bracket
    max_non_zero = data_group.groupby(features_notime[:-1], as_index=False)['Non-Zero Cap Deployed Cumm'].max()
    #Rename column
    max_non_zero.rename(columns={'Non-Zero Cap Deployed Cumm':'Max Non-Zero Cap Deployed Cumm'}, inplace=True)
    #Merge with main data
    data_group = data_group.merge(max_non_zero, how='left', on=features_notime[:-1])

    #Make a column for where the Non-Zero Cap Deployed Cumm is equal to the Max Non-Zero Cap Deployed Cumm
    data_group['Non-Zero Cap Deployed Cumm Match'] = np.where(data_group['Non-Zero Cap Deployed Cumm']==data_group['Max Non-Zero Cap Deployed Cumm'],1,0)

    #Make data where the x hurdle is the Max Non-Zero Cap Deployed Cumm
    x_hurdle_max = data_group[(data_group['x hurdle']=='1')&(data_group['Non-Zero Cap Deployed Cumm Match']==1)]
    #Drop unnecessary columns
    #x_hurdle_max = x_hurdle_max.drop(columns={'PRI_Brackets','Imb_Size_Cap_Deployed','Imb_Size_Return','Positional_Cap_Deployed','Positional_Return','PRI Low','PRI High','IADV Low','IADV High','Imb Size Cap Deployed Cumm','Imb Size Return Cumm','Imb Size % Return Cumm','Line above 20 bps','Line above 20 bps Cumm','Mariginal Imb Size Cap Deployed Cumm','Mariginal Imb Size Return Cumm','Mariginal Imb Size % Return Cumm','Marginal Line above 20 bps','Marginal Line above 20 bps Cumm','Marginal 2 Imb Size Cap Deployed Cumm','Marginal 2 Imb Size Return Cumm','Marginal 2 Imb Size % Return Cumm','Marginal 2 Line above 20 bps','Marginal 2 Line above 20 bps Cumm','Marginal 3 Imb Size Cap Deployed Cumm','Marginal 3 Imb Size Return Cumm','Marginal 3 Imb Size % Return Cumm','Marginal 3 Line above 20 bps','Marginal 3 Line above 20 bps Cumm','Marginal 4 Imb Size Cap Deployed Cumm','Marginal 4 Imb Size Return Cumm','Marginal 4 Imb Size % Return Cumm','Marginal 4 Line above 20 bps','Marginal 4 Line above 20 bps Cumm','Marginal 5 Imb Size Cap Deployed Cumm','Marginal 5 Imb Size Return Cumm','Marginal 5 Imb Size % Return Cumm','Marginal 5 Line above 20 bps','Marginal 5 Line above 20 bps Cumm','Marginal 6 Imb Size Cap Deployed Cumm','Marginal 6 Imb Size Return Cumm','Marginal 6 Imb Size % Return Cumm','Marginal 6 Line above 20 bps','Marginal 6 Line above 20 bps Cumm','Marginal 7 Imb Size % Return Cumm','Marginal 7 Line above 20 bps','Marginal 7 Imb Size Cap Deployed Cumm','Marginal 7 Imb Size Return Cumm','Marginal 7 Line above 20 bps Cumm','Marginal 8 Imb Size Cap Deployed Cumm','Marginal 8 Imb Size Return Cumm','Marginal 8 Imb Size % Return Cumm','Marginal 8 Line above 20 bps','Marginal 8 Line above 20 bps Cumm','Marginal 9 Imb Size Cap Deployed Cumm','Marginal 9 Imb Size Return Cumm','Marginal 9 Imb Size % Return Cumm','Marginal 9 Line above 20 bps','Marginal 9 Line above 20 bps Cumm','Marginal 10 Imb Size Cap Deployed Cumm','Marginal 10 Imb Size Return Cumm','Marginal 10 Imb Size % Return Cumm','Marginal 10 Line above 20 bps','Marginal 10 Line above 20 bps Cumm','Marginal 11 Imb Size Cap Deployed Cumm','Marginal 11 Imb Size Return Cumm','Marginal 11 Imb Size % Return Cumm','Marginal 11 Line above 20 bps','Marginal 11 Line above 20 bps Cumm','Marginal 12 Imb Size Cap Deployed Cumm','Marginal 12 Imb Size Return Cumm','Marginal 12 Imb Size % Return Cumm','Marginal 12 Line above 20 bps','Marginal 12 Line above 20 bps Cumm','Marginal 13 Imb Size Cap Deployed Cumm','Marginal 13 Imb Size Return Cumm','Marginal 13 Imb Size % Return Cumm','Marginal 13 Line above 20 bps','Marginal 13 Line above 20 bps Cumm','Marginal 14 Imb Size Cap Deployed Cumm','Marginal 14 Imb Size Return Cumm','Marginal 14 Imb Size % Return Cumm','Marginal 14 Line above 20 bps','Marginal 14 Line above 20 bps Cumm','Hurdle Sum 14','Hurdle Sum Match','Hurdle Sum Match Cumm','one above x hurdle','x hurdle','y hurdle','Non-Zero Cap Deployed','Non-Zero Cap Deployed Cumm','range y hurdle','range y hurdle match','range y hurdle match cumm','max cap deployed','y hurdle match','Max Non-Zero Cap Deployed Cumm'}, axis=1)
    x_hurdle_max = x_hurdle_max[features_notime[:-1] + ['Non-Zero Cap Deployed Cumm Match']]
    #Rename the column
    x_hurdle_max.rename(columns={'Non-Zero Cap Deployed Cumm Match':'x hurdle is max non-zero cap deployed'}, inplace=True)
    #Merge with main data
    data_group = data_group.merge(x_hurdle_max, how='left', on=features_notime[:-1])

    #Set the y hurdle
    data_group['y hurdle'] = 0
    data_group['y hurdle'] = np.where(data_group['x hurdle']=='Do Not Trade','Do Not Trade',np.where((data_group['x hurdle'].shift(1)=='1')&(data_group['Non-Zero Cap Deployed']==1),1,np.where(data_group['y hurdle match']==1,1,np.where((data_group['x hurdle is max non-zero cap deployed']==1)&(data_group['PRI Low']=='-inf'),1,0))))

    #Need to correct where the x and y hurdles are 'Do Not Trade', but the max cap deployed for that IADV bucket is 0 (should be 'no data for hurdle')
    data_group['x hurdle'] = np.where((data_group['x hurdle']=='Do Not Trade')&(data_group['max cap deployed']==0),'no data for hurdle',data_group['x hurdle'])
    data_group['y hurdle'] = np.where((data_group['x hurdle']=='no data for hurdle'),'no data for hurdle',data_group['y hurdle'])

    #Create df with just the hurdle spots
    df_hurdles_set = data_group[(data_group['x hurdle']!='0')|(data_group['y hurdle']=='0')]
    
    df_hurdle_lines = df_hurdles_set[features_notime[:-1] + ['PRI Low','x hurdle','y hurdle']]
    df_hurdle_lines = df_hurdle_lines[df_hurdle_lines['x hurdle']!='0']
    df_hurdle_lines['PRI Low'] = np.where(df_hurdle_lines['x hurdle']=='Do Not Trade',do_not_trade_val,df_hurdle_lines['PRI Low'])
    df_hurdle_lines = df_hurdle_lines.sort_values('Time')
    
    return df_hurdle_lines, df_hurdles_set, data_group, bp


# In[129]:


def get_symbolslist(date,market='nsdq'):
    #date is str
    #market is 'nsdq' or 'nyse'
    markets = {'nsdq':'R','nyse':'N'}
    import pandas as pd
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    symbols = pd.read_csv(advpath + advfile)
    symbols.columns=['CUSIP','SYMBOL','Type','Exchange','Desc','ETB','ADV']
    df = symbols[symbols['Exchange']==markets[market]]
    df['ETB'] = np.where(df['ETB']=='ETB','ETB','H')
    #df = df[df['ETB']=='ETB']
    df['ADV'] = df['ADV'].astype('int')
    adv = df[['SYMBOL','ADV','ETB','Type']]
    adv.rename(columns={'SYMBOL':'SYMBOL'},inplace=True)
    return adv, df


# In[130]:


def find_daytype(date):
    datesfile = 'Z:/NCIS - B/Days.csv'
    days = pd.read_csv(datesfile, parse_dates=['Date'])
    days = days[['Date','Type']]
    daystype = days[days['Date']==date]['Type'].squeeze()
    return daystype


# In[131]:


def reload_package(package,rename):
    import importlib
    import package as rename #import the module here, so that it can be reloaded.
    importlib.reload(rename)


# In[132]:


def apply_capital_allocation(df, total_capital, imb_percents, cap_percents, liquidity_caps, date,score_col='Total_Score',arti_col='Real_Time_Imbalance',inc_max_shares=True):
    a1 = (df[score_col]>100)
    a2 = (df[score_col].between(75,100,inclusive='left'))
    a3 = (df[score_col].between(50,75,inclusive='left'))
    a4 = (df[score_col].between(20,50,inclusive='left'))
    a5 = (df[score_col].between(0,20,inclusive='left'))
    vals = imb_percents
    default = 0
    df['Max_Imbalance'] = np.select([a1,a2,a3,a4,a5],vals,default=default)*df[arti_col]
    
    a1 = (df[score_col]>100)
    a2 = (df[score_col].between(75,100,inclusive='left'))
    a3 = (df[score_col].between(50,75,inclusive='left'))
    a4 = (df[score_col].between(20,50,inclusive='left'))
    a5 = (df[score_col].between(0,20,inclusive='left'))
    vals = cap_percents
    default = 0
    df['Max_Capital'] = np.select([a1,a2,a3,a4,a5],vals,default=default)*total_capital
    if inc_max_shares == True:
        df['Max_Capital_Shares'] = (df['Max_Capital']/df['Entry_Price'])*np.sign(df[arti_col])
    
    a1 = (df.index<=(date + ' 19:55'))
    a2 = (df.index>=(date + ' 19:55')) & (df['ADV']<1000000)
    a3 = (df.index>=(date + ' 19:55')) & (df['ADV']>=1000000)
    vals = liquidity_caps
    default = liquidity_caps[1]
    df['Max_ADV'] = np.select([a1,a2,a3], vals,default=default)
    df['Max_ADV'] = df['Max_ADV'].astype('float32')*df['ADV']*np.sign(df[arti_col])


# In[133]:


#Applying Trading Logic for previous position
def apply_previous_position(df,date,positional=True,unlimited=False,voladj=False):
    
    if positional==True:
        #Constrained by Capital and Imbalance
        df['Max_Shares'] = np.where(df['Real_Time_Imbalance']>0,df[['Max_Imbalance','Max_Capital_Shares','Max_ADV']].min(axis=1),df[['Max_Imbalance','Max_Capital_Shares','Max_ADV']].max(axis=1))
        df['Playable_Shares'] = (df['Max_Shares']).astype('int64')
        df['Shares_Previous'] = df.groupby(['SYMBOL', 'Date'])['Playable_Shares'].shift(1)
        df['Shares_Previous'] = df['Shares_Previous'].fillna(0)
        df['Shares_Previous'] = df['Shares_Previous'].astype('int64')
        pos = df.groupby(['SYMBOL','Date'])['Shares_Previous'].cummax()
        neg = df.groupby(['SYMBOL','Date'])['Shares_Previous'].cummin()
        df['Max_Shares'] = np.where(df['Real_Time_Imbalance']>=0,pos,neg)
        df['Shares_Traded'] = df.apply(lambda row: sharestraded(row['Playable_Shares'], row['Shares_Previous'],row['Max_Shares']), axis=1)
        df['Shares_Traded'] = np.where(((df['Real_Time_Imbalance']<0 )& (df['ETB']=='H')),0,df['Shares_Traded'])
        df['Positional_Cap_Deployed'] = df['Shares_Traded'] * df['Entry_Price']
        df['Positional_Weighted_Return'] = df['Positional_Cap_Deployed'] * df['Per_Return_Realized']
        df['Num_of_Trades'] = df['Positional_Weighted_Return']!=0
        df['Winners'] = df['Positional_Weighted_Return']>0
        df['Winners_Playable'] = df['Per_Return_Realized']>0
    else:
        pass
    
    if unlimited == True:
        #Only constrained by Imbalance
        df['Max_Shares_Unlimited_Capital'] = np.where(df['Real_Time_Imbalance']>0,df[['Max_Imbalance','Max_ADV']].min(axis=1),df[['Max_Imbalance','Max_ADV']].max(axis=1))
        df['Playable_Shares_Unlimited_Capital'] = (df['Max_Shares_Unlimited_Capital']).astype('int64')
        df['Shares_Previous_Unlimited_Capital'] = df.groupby(['SYMBOL', 'Date'])['Playable_Shares_Unlimited_Capital'].shift(1)
        df['Shares_Previous_Unlimited_Capital'] = df['Shares_Previous_Unlimited_Capital'].fillna(0)
        df['Shares_Previous_Unlimited_Capital'] = df['Shares_Previous_Unlimited_Capital'].astype('int64')
        pos = df.groupby(['SYMBOL','Date'])['Shares_Previous_Unlimited_Capital'].cummax()
        neg = df.groupby(['SYMBOL','Date'])['Shares_Previous_Unlimited_Capital'].cummin()
        df['Max_Shares_Unlimited_Capital'] = np.where(df['Real_Time_Imbalance']>=0,pos,neg)
        df['Shares_Traded_Unlimited_Capital'] = df.apply(lambda row: sharestraded(row['Playable_Shares_Unlimited_Capital'], row['Shares_Previous_Unlimited_Capital'],row['Max_Shares_Unlimited_Capital']), axis=1)
        df['Shares_Traded_Unlimited_Capital'] = np.where(((df['Real_Time_Imbalance']<0 )& (df['ETB']=='H')),0,df['Shares_Traded_Unlimited_Capital'])
        df['Positional_Cap_Deployed_Unlimited_Capital'] = df['Shares_Traded_Unlimited_Capital'] * df['Entry_Price']
        df['Positional_Weighted_Return_Unlimited_Capital'] = df['Positional_Cap_Deployed_Unlimited_Capital'] * df['Per_Return_Realized']
    else:
        pass
    
    if voladj==True:
        #Constrained by Capital and Imbalance and Interval Volume
        df['Interval_Volume_Short'] = df['Interval_Volume'] * -1
        df['Max_Shares_Vol_Adj'] = np.where(df['Real_Time_Imbalance']>0,df[['Max_Imbalance','Max_Capital_Shares','Max_ADV']].min(axis=1),df[['Max_Imbalance','Max_Capital_Shares','Max_ADV']].max(axis=1))
        df['Playable_Shares_Vol_Adj'] = (df['Max_Shares_Vol_Adj']).astype('int64')
        df['Shares_Previous_Vol_Adj'] = df.groupby(['SYMBOL', 'Date'])['Playable_Shares_Vol_Adj'].shift(1)
        df['Shares_Previous_Vol_Adj'] = df['Shares_Previous_Vol_Adj'].fillna(0)
        df['Shares_Previous_Vol_Adj'] = df['Shares_Previous_Vol_Adj'].astype('int64')
        df['Interval_Volume_Previous'] = df.groupby(['SYMBOL', 'Date'])['Interval_Volume'].shift(1)
        pos = df.groupby(['SYMBOL','Date'])['Shares_Previous_Vol_Adj'].cummax()
        neg = df.groupby(['SYMBOL','Date'])['Shares_Previous_Vol_Adj'].cummin()
        df['Max_Shares_Vol_Adj'] = np.where(df['Real_Time_Imbalance']>=0,pos,neg)
        df['Shares_Traded_Vol_Adj'] = df.apply(lambda row: sharestraded(row['Playable_Shares_Vol_Adj'], row['Shares_Previous_Vol_Adj'],row['Max_Shares_Vol_Adj']), axis=1)
        df['Shares_Traded_Vol_Adj'] = np.where(((df['Real_Time_Imbalance']<0 )& (df['ETB']=='H')),0,df['Shares_Traded_Vol_Adj'])
        df['Shares_Traded_Vol_Adj'] = df[['Shares_Traded_Vol_Adj','Interval_Volume']].min(axis=1)
        df['Positional_Cap_Deployed_Vol_Adj'] = df['Shares_Traded_Vol_Adj'] * df['Entry_Price']
        df['Positional_Weighted_Return_Vol_Adj'] = df['Positional_Cap_Deployed_Vol_Adj'] * df['Per_Return_Realized']
    else:
        pass
    
    #Constrained by 100% of Imbalance
    df['Positional_Cap_Deployed_100'] = np.absolute(df['Real_Time_Imbalance']) *  df['Entry_Price']
    df['Positional_Weighted_Return_100'] = df['Positional_Cap_Deployed_100'] * df['Per_Return_Realized']

    a1 = (df.index>=(date + ' 19:00')) & (df.index<(date + ' 19:55'))
    a2 = (df.index>=(date + ' 19:55')) & (df.index<(date + ' 19:58'))
    a3 = (df.index>=(date + ' 19:58'))
    vals = ['Entry50', 'Entry55', 'Entry58']
    df['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')


# In[134]:


def get_daytype(date):
    datesfile = 'Z:/NCIS - B/Days.csv'
    days = pd.read_csv(datesfile, parse_dates=['Date'])
    daytype = days[days['Date']==date]
    daytype = daytype['Type'].squeeze()
    return daytype


# In[135]:


def check_for_summary_file(date,file='_summary.xlsx'):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if os.path.exists(writepath+date+file):
        check = True
    else:
        check = date
    return check


# In[136]:


def get_imbs(date):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    #Change date variable only
    starttime = "'" + date + ' 12:59:45' + "'"
    endtime = "'" + date + ' 14:50:11' + "'"
    endtime2 = "'" + date + ' 23:59"' + "'"

    #Get tick data
    airpath = 'X:/ticks/'+date+'_NASDAQ_ticks/'
    imbs = pd.read_csv(airpath+'imbalances.csv.gz',header=None)
    if len(imbs.columns)==11:
        cols = ['time','offset','symbol','sequence_number','auction_type','matched_quantity','imbalance_size','imbalance_size_2','clearing_price','clearing_price_2','reference_price']
    else:
        cols = ['time','symbol','sequence_number','auction_type','matched_quantity','imbalance_size','imbalance_size_2','clearing_price','clearing_price_2','reference_price']
    imbs.columns = cols
    imbs = imbs[imbs['time'].notna()]
    imbs['time'] = pd.to_datetime(imbs['time'].astype('float'),unit='us')
    imbs['time'] = pd.to_datetime(date+' '+((imbs['time']).dt.time).astype('str'))
    imbs['time'] = imbs['time'].dt.tz_localize(None)
    imb = imbs[imbs['time']>date+' 19:49']
    if len(imb.index) == 0:
        print(date)
    else:
        imb['DATE'] = imb['time'].dt.date
        imb = imb.sort_values(['symbol','time'])
        imb = imb.set_index('time',drop=True)
        imb.rename(columns={'symbol':'SYMBOL'},inplace=True)

        #adv, nsdq_symbols = get_symbolslist(date)
        #imb = pd.merge(imb.reset_index(),adv,how='left')
        #imb['IADV'] = np.absolute(imb['imbalance_size'])/imb['ADV']
        imb['Notional_Imbalance'] = imb['imbalance_size'] * imb['reference_price']
        imb['Abs_Notional_Imbalance'] = np.absolute(imb['imbalance_size'] * imb['reference_price'])
        # daytype = days[days['Date']==date]
        # daytype = daytype['Type'].squeeze()
        imb['Day_Type']= get_daytype(date)
        imb.to_parquet(writepath +date +' imb.parquet')


# In[137]:


li = []
def read_imbs(date):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    imb = pd.read_parquet(writepath +date +' imb.parquet')
    i0raw = imb.groupby(['SYMBOL']).head(1)
    i0 = i0raw[i0raw['imbalance_size']!=0]
    res = i0.groupby(['DATE'])[['SYMBOL']].nunique()
    adv, nsdq = get_symbolslist(date)
    i0 = pd.merge(i0,adv,how='left')
    res['Abs_Notional_Imbalance_Sum'] = i0.groupby(['DATE'])['Abs_Notional_Imbalance'].sum()
    cuts = [0,500000,1000000,5000000,10000000,100000000,np.inf]
    i0['Abs_Notional_Brackets'] = pd.cut(i0['Abs_Notional_Imbalance'],bins=cuts)
    cuts = [0,500001,6000000,25000000,np.inf]
    i0['ADV_Brackets'] = pd.cut(i0['ADV'],bins=cuts)
    tb = i0.groupby(['DATE','Abs_Notional_Brackets'])[['SYMBOL']].count()
    tb = tb.reset_index().set_index('DATE',drop=True)
    tb = tb.pivot(columns='Abs_Notional_Brackets',values='SYMBOL')
    tb = tb.reset_index().set_index('DATE',drop=True)
    res_imb = pd.merge(res.reset_index(),tb.reset_index())
    #tb = i0.groupby(['DATE','ADV_Brackets'])[['SYMBOL']].count()
    #tb = tb.reset_index().set_index('DATE',drop=True)
    #tb = tb.pivot(columns='ADV_Brackets',values='SYMBOL')
    #tb = tb.reset_index().set_index('DATE',drop=True)
    #res_imb = pd.merge(res.reset_index(),tb.reset_index())
    res_imb['DATE'] = res_imb['DATE'].astype('str')
    li.append(imb)
    return res_imb


# In[138]:

def get_kb_logs(date,environment,hostname):
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('module')
        writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
        if not os.path.exists(writepath):
            os.makedirs(writepath)
        es = elasticsearch.Elasticsearch("https://elk:9200", verify_certs=False, http_auth=("jenkins", "K28q**gXCkyVUMPq"))

        query = {
            "sort": {"@timestamp": {"order": "desc"}},
            "_source": ["@timestamp", "message","environment"],
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "@timestamp": {
                                    "format": "strict_date_optional_time",
                                    "gte": date+"T16:00:50.282Z",
                                    "lte": date+"T21:01:27.235Z",
                                }
                            }
                        },
                        {"match_phrase": {"host.name": hostname}},
                        {
                            "bool" :{
                                "should": [
                                    {
                                        "match_phrase": {
                                            "strategy.instance": "NsdqImbalanceToClose_0"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "strategy.instance": "NsdqImbalanceToClose_1"
                                        }
                                    },
                                    {
                                        "match_phrase": {
                                            "strategy.instance": "NsdqImbalanceToClose_2"
                                        }
                                    }
                                ],
                                "minimum_should_match": 1
                            }
                        },
                        {"match_phrase": {"environment": environment}}
                    ]
                }
            },
        }

        # this returns up to 500 rows, adjust to your needs
        data = es.search(index="filebeat-*", body=query, size=1000, scroll='2m')

        sid = data['_scroll_id']
        hits = data['hits']['hits']

        if len(hits) == 0:
            print('pass '+date)
        else:

            # then open a csv file, and loop through the hits, writing to the csv
            with open(writepath + date+ ' processorlogNSDQClose_'+environment+'.csv', 'w') as csvfile:
                filewriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

                while len(hits) > 0:
                    for r in hits:
                        fields = r["_source"]
                        fields = map(lambda x: x.replace('\n', ' '), fields.values())
                        filewriter.writerow(fields)

                    data = es.scroll(scroll_id=sid, scroll='2m')
                    sid = data['_scroll_id']

                    hits = data['hits']['hits']
                    #print("Scrolled another " + str(len(hits)) + " items")

def read_reports(date, report_name):
    filenames = {'orders':' orders.csv',
                 'trinst':' tr_byinstance.csv',
                 'tr':' tradereports.csv',
                 'imb':' imb.parquet',
                 'closeout':' closeout.csv',
                 'passed':' passed.csv',
                 'exits': ' exits.csv',
                 'kb': ' kb.csv',
                 'ope': ' ope.csv',
                 'ten_min':' ten_min.csv',
                 'profit':  ' profit.csv',
                 'attempts':' attempts.csv',
                 'orderlog':' orderlog.csv',
                 'capman':' capman.parquet'}
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if date + filenames[report_name] not in os.listdir(writepath):
        print('pass '+date + filenames[report_name])
        df = pd.DataFrame
    elif '.parquet' in filenames[report_name]:
        df = pd.read_parquet(writepath + date + filenames[report_name])
    else:
        df = pd.read_csv(writepath + date + filenames[report_name],index_col=0)
    return df

#df is subset of orders report
#factor is list of factors.Cannot use pd.Grouper
def calc_fill_rate(df,factor):
    u_factor = factor.copy()
    group = df.groupby(factor)[['FilledQTY_Abs','Filled$_Abs']].sum().reset_index()
    if 'SYMBOL' in u_factor:
        pass
    else:
        u_factor.append('SYMBOL')
    u_factor.append('QTY_Max_Desired')
    u_factor.append('$_Max_Desired')
    x = df[u_factor]
    x = x.drop_duplicates(u_factor).reset_index()
    if 'SYMBOL' in factor:
        pass
    else:
        x.pop('SYMBOL')
    if 'EntryTime' in x.columns:
        x = x.set_index('EntryTime')
    else:
        pass
    table = x.groupby(factor)[['QTY_Max_Desired','$_Max_Desired']].sum().reset_index()
    group = pd.merge(group,table)
    group['Fill_Rate_Shares'] = group['FilledQTY_Abs']/group['QTY_Max_Desired']
    group['Fill_Rate_Dollars'] = group['Filled$_Abs']/group['$_Max_Desired']
    return group

def calc_slippage(df,factor):
    #df is subset or orders report
    #factor is list of factors. Cannot be pd.Grouper
    df = df.reset_index()
    df['EntryTime'] = pd.to_datetime(df['EntryTime'])
    df['Long_Short'] = np.where(df['Quantity']>0,'Long','Short')
    df['Time_Diff'] = (df['EntryTime'] - df.groupby(['SYMBOL','Long_Short'])['EntryTime'].shift(1)).dt.total_seconds().fillna(100)
    df['Time_Diff_Bool'] = df['Time_Diff']>15
    df['Unique'] = df.groupby(['SYMBOL'])['Time_Diff_Bool'].cumsum()
    df['QTY_Abs_Agg'] = df.groupby(['SYMBOL','Long_Short','EntryTime'])[['QTY_Abs']].transform('sum')
    df['QTY_Max_Desired'] = df.groupby(['SYMBOL','Long_Short','Unique'])[['QTY_Abs_Agg']].transform('max')
    df['$_Max_Desired'] = df['QTY_Max_Desired'] * df['Price']
    x = df.groupby(['SYMBOL','Unique'])[['SYMBOL','Unique','Price','QTY_Max_Desired','$_Max_Desired']].head(1)
    x.rename(columns={'Price':'Start_Price'},inplace=True)
    df.drop(columns=['QTY_Max_Desired','$_Max_Desired'],inplace=True)
    df = pd.merge(df,x,how='left')
    a1 = (df['Start_Price']<5)
    a2 = (df['Start_Price'].between(5,10,inclusive='left'))
    a3 = (df['Start_Price'].between(10,15,inclusive='left'))
    a4 = (df['Start_Price'].between(15,30,inclusive='left'))
    a5 = (df['Start_Price'].between(30,60,inclusive='left'))
    a6 = (df['Start_Price']>=60)
    vals = [0,.01,.02,.03,.04,.05]
    default = 0
    df['Cross_Amount'] = np.select([a1,a2,a3,a4,a5,a6],vals,default)
    df['Start_NBBO'] = np.where(df['Quantity']>0,df['Start_Price']-df['Cross_Amount'],df['Start_Price']+df['Cross_Amount'])

    #Calculating Price Slippage from first eval
    slip = df
    slip['Start_NBBO'] = np.where(slip['AvgFillPrice']==0,np.nan,slip['Start_NBBO'])
    slip['Slippage_$'] = np.where(slip['Quantity']>0,slip['Start_NBBO']-slip['AvgFillPrice'],slip['AvgFillPrice']-slip['Start_NBBO'])
    slip['Slipage_%'] = slip['Slippage_$']/slip['Start_NBBO']
    slip['Weighted_Price'] = slip['$_Max_Desired']*slip['Start_NBBO']
    slip['Slippage_$$, $Weighted'] = slip['$_Max_Desired']*slip['Slippage_$']
    slip['Weighted_Shares'] = slip['QTY_Max_Desired']*slip['Start_NBBO']
    slip['Slippage_$,Shares Weighted'] = slip['QTY_Max_Desired']*slip['Slippage_$']

    #Summary table of price slippage by factor
    u_factor = factor.copy()
    slip_results = slip.groupby(factor)[['Weighted_Price','Slippage_$$, $Weighted','Weighted_Shares','Slippage_$,Shares Weighted']].sum()
    if 'SYMBOL' in u_factor:
        pass
    else:
        u_factor.append('SYMBOL')
    u_factor.append('QTY_Max_Desired')
    u_factor.append('$_Max_Desired')
    x = slip[u_factor]
    x = x.drop_duplicates(u_factor).reset_index()
    if 'SYMBOL' in factor:
        pass
    else:
        x.pop('SYMBOL')
    table = x.groupby(factor)[['QTY_Max_Desired','$_Max_Desired']].sum().reset_index()
    slip_results = pd.merge(slip_results.reset_index(),table)
    slip_results['Avg_Slippage_$$, $Weighted'] = slip_results['Slippage_$$, $Weighted']/slip_results['$_Max_Desired']
    slip_results['BP_Slippage_$$, $Weighted'] = slip_results['Slippage_$$, $Weighted']/slip_results['Weighted_Price']
    slip_results['Avg_Slippage_$,Shares Weighted'] = slip_results['Slippage_$,Shares Weighted']/slip_results['QTY_Max_Desired']
    slip_results['BP_Slippage_$,Shares Weighted']  = slip_results['Slippage_$,Shares Weighted']/slip_results['Weighted_Shares']
    x = slip.groupby(factor)[['SYMBOL']].nunique()
    x.rename(columns={'SYMBOL':'Count'},inplace=True)
    x = x.reset_index()
    slip_results = pd.merge(slip_results,x)
    res = slip_results.drop(columns=['Weighted_Price','Weighted_Shares','Slippage_$$, $Weighted','Slippage_$,Shares Weighted'])
    return res
