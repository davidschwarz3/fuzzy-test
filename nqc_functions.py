#!/usr/bin/env python
# coding: utf-8

# In[968]:


import core_functions as cf


# In[969]:


import os
import pandas as pd
import numpy as np


# In[970]:


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
                 'profit':  ' profit.csv'}
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if date + filenames[report_name] not in os.listdir(writepath):
        print('pass '+date + filenames[report_name])
    elif '.parquet' in filenames[report_name]:
        df = pd.read_parquet(writepath + date + filenames[report_name])
    else:
        df = pd.read_csv(writepath + date + filenames[report_name],index_col=0)
    return df


# In[971]:


def find_total_capital(date,filetype='.csv'):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if filetype == '.csv':
        capman = pd.read_csv(writepath + date + ' capman'+filetype,index_col=0)
    else:
        capman = pd.read_parquet(writepath + date + ' capman'+filetype)
    t = capman[capman['0'].str.contains('Successful ')]
    t = t.head(1)
    t  = t.drop_duplicates()
    total_capital = t['0'].str.extract('((?<=available:)\S{1,})').squeeze()
    total_capital = total_capital.replace(',','')
    total_capital = int(total_capital)
    return total_capital


# In[972]:


li = []

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
    conn = psycopg2.connect("host='possum' dbname='ticks' user='possum_ro' password='NeHm7aahfU2AM4nf'")
    sql = "select * from nasdaq_imbalances where time >= "+starttime+" and time <="+endtime2+";"
    imb = pd.read_sql_query(sql, conn)
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
        daytype = cf.find_daytype(date)
        imb['Day_Type']=daytype
        imb.to_parquet(writepath +date +' imb.parquet')

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
    tb = i0.groupby(['DATE','ADV_Brackets'])[['SYMBOL']].count()
    tb = tb.reset_index().set_index('DATE',drop=True)
    tb = tb.pivot(columns='ADV_Brackets',values='SYMBOL')
    tb = tb.reset_index().set_index('DATE',drop=True)
    res_imb = pd.merge(res.reset_index(),tb.reset_index())
    res_imb['DATE'] = res_imb['DATE'].astype('str')
    li.append(imb)
    return res_imb


# In[973]:


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
                                    "gte": date+"T19:00:50.282Z",
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
            with open(writepath + date+ ' processorlogNSDQClose_prod.csv', 'w') as csvfile:   
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


# In[974]:


def get_symbolslist(date):
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    symbols = pd.read_csv(advpath + advfile)
    symbols.columns=['CUSIP','SYMBOL','Type','Exchange','Desc','ETB','ADV']
    df = symbols[symbols['Exchange']=='R']
    df['ETB'] = np.where(df['ETB']=='ETB','ETB','H')
    #df = df[df['ETB']=='ETB']
    df['ADV'] = df['ADV'].astype('int')
    adv = df[['SYMBOL','ADV','ETB','Type']]
    adv.rename(columns={'symbol':'SYMBOL'},inplace=True)
    return adv, df


# In[975]:


#######################Summary Numbers####################
#Create slice of orders to only look at entries
#Capital Deployed
def write_summary_report(factors,total_capital,date):
    trinst = cf.read_reports(date,'trinst')
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    writer = pd.ExcelWriter(writepath + date+'_summary.xlsx')
    merge = False
    for factor in factors:
        df = trinst
        #factor=['DATE']
        u_factor = [factor]
        x_factor = [factor]
        if 'DATE' in u_factor:
            pass
        else:
            u_factor.append('DATE')
        if 'SYMBOL' in u_factor:
            pass
        else:
            u_factor.append('SYMBOL')
        df['DATE'] = df['DATE'].astype(str)
        res = df.groupby([factor])[['Filled$_Abs']].sum().reset_index()
        res.rename(columns={'Filled$_Abs':'Capital_Deployed'},inplace=True)
        try:
            total_capital
        except NameError:
            total_capital = 0
        res['Total_Capital'] = total_capital
        #RealizedPL and Basis Return
        tb = df.groupby([factor])[['RealizedPL']].sum().reset_index()
        res = pd.merge(res,tb,how='outer')
        res['Basis_Return'] = res['RealizedPL']/res['Capital_Deployed']
        #Symbols Traded
        tb = df.groupby(u_factor)[['SYMBOL']].nunique()
        tb.rename(columns={'SYMBOL':'SYMBOLS_Traded'},inplace=True)
        tb = tb.reset_index()
        tb = tb.groupby([factor])[['SYMBOLS_Traded']].sum().reset_index()
        res = pd.merge(res,tb,how='outer')
        #Fill Rates
        #tb = (df.groupby([factor])[['FilledQTY_Abs']].sum()*2).reset_index()
        #res = pd.merge(res,tb,how='outer')
        #tb = df.groupby([factor])[['FilledQTY_Abs','QTY_Abs']].sum()
        #tb['Fill_Rate_Shares'] = tb['FilledQTY_Abs']/tb['QTY_Abs']
        #tb[['Filled$_Abs','Desired_$']] = df.groupby([factor])[['Filled$_Abs','Desired_$']].sum()
        #tb['Fill_Rate_Dollars'] = tb['Filled$_Abs']/tb['Desired_$']
        #tb = tb.reset_index()
        #tb = tb[[factor,'Fill_Rate_Shares','Fill_Rate_Dollars']]
        #res = pd.merge(res,tb,how='outer')
        #Win Rates
        winners = df[df['RealizedPL']>0].reset_index()
        win_summary = winners.groupby(u_factor)[['SYMBOL']].nunique()
        win_summary.rename(columns={'SYMBOL':'Win_Count'},inplace=True)
        win_summary = win_summary.reset_index()
        win_summary = win_summary.groupby([factor])[['Win_Count']].sum().reset_index()
        win_dollars = winners.groupby([factor])[['RealizedPL']].sum().reset_index()
        win_summary = win_summary.merge(win_dollars)
        win_summary.rename(columns={'RealizedPL':'Winner_Dollars'},inplace=True)
        winmean = winners.groupby([factor])[['RealizedPL']].mean().reset_index()
        winmean.rename(columns={'RealizedPL':'WinMean'},inplace=True)
        losers = df[df['RealizedPL']<=0].reset_index()
        lose_dollars = losers.groupby([factor])[['RealizedPL']].sum().reset_index()
        lose_dollars.rename(columns={'RealizedPL':'Loser_Dollars'},inplace=True)
        losemean = losers.groupby([factor])[['RealizedPL']].mean().reset_index()
        losemean.rename(columns={'RealizedPL':'LoseMean'},inplace=True)
        win_summary = pd.merge(win_summary,lose_dollars)
        res = pd.merge(res,win_summary,how='outer')
        res['Win_Rate'] = res['Win_Count']/res['SYMBOLS_Traded']
        res['Win_Ratio'] = winmean['WinMean']/np.absolute(losemean['LoseMean'])
        #Stuck Symbols
        #tb = tb.groupby([factor])[['Stuck']].apply(lambda x: (x=='Stuck').sum()).reset_index()
        #tb.rename(columns={'Stuck':'Stuck_Count'},inplace=True)
        tb = df[df['Stuck']=='Stuck'].reset_index()
        tbsummary = tb.groupby(u_factor)[['SYMBOL']].nunique()
        tbsummary.rename(columns={'SYMBOL':'Stuck_Count'},inplace=True)
        tbsummary = tbsummary.reset_index().groupby([factor])[['Stuck_Count']].sum().reset_index()
        res = pd.merge(res,tbsummary,how='outer')
        res['Stuck_Percent_Count'] = res['Stuck_Count']/res['SYMBOLS_Traded']
        x = df[df['Stuck']=='Stuck']
        tb = x.groupby([factor])[['Filled$_Abs']].sum().reset_index()
        tb.rename(columns={'Filled$_Abs':'Stuck_Filled$_Abs'},inplace=True)
        res = pd.merge(res,tb,how='outer')
        res['Stuck_Percent_$'] = res['Stuck_Filled$_Abs']/res['Capital_Deployed']
        res['Day_Type'] = cf.find_daytype(date)
        if 'Score_Bracket' not in factor: 
            x_factor.append('Score_Bracket')
            #Grouping TR_by_Instance by score bracket
            factorslice = trinst
            group = factorslice.groupby(x_factor)
            tb = group[['RealizedPL','Filled$_Abs']].sum()
            tb['Basis_Return'] = tb['RealizedPL'] / tb['Filled$_Abs']
            tb['Count_by_Symbol'] = group['SYMBOL'].nunique()
            x = factorslice[factorslice['RealizedPL']>0]
            xwin_count = x.groupby(x_factor)['SYMBOL'].nunique()
            tb['Win_Count_by_Symbol'] = xwin_count
            tb['Win_Rate'] = tb['Win_Count_by_Symbol'] / tb['Count_by_Symbol']
            #Merging Score Bracket groupby into res dataframe 
            tb = tb.reset_index()
            tb = tb.set_index(x_factor[0])
            tbscore = tb.pivot(columns='Score_Bracket',values='Count_by_Symbol').add_suffix('_Symbol_Count').reset_index()
            res = pd.merge(res,tbscore,how='left')
            tbscore = tb.pivot(columns='Score_Bracket',values='Filled$_Abs').add_suffix('_Filled$_Abs').reset_index()
            res = pd.merge(res,tbscore,how='left',suffixes=(None,'_Filled$_Abs'))
            tbscore = tb.pivot(columns='Score_Bracket',values='RealizedPL').add_suffix('_RealizedPL').reset_index()
            res = pd.merge(res,tbscore,how='left')
            try:
                res_imb
                merge = True
            except NameError:
                pass
            if 'DATE' not in res.columns:
                pass
            elif merge == False:
                pass
            elif res['DATE'][0]!=res_imb['DATE'][0]:
                print(res['DATE'][0])
                print(res_imb['DATE'][0])
            else:
                res = pd.merge(res, res_imb)
            print(factor)
            res['DATE'] = date
            res.to_excel(writer, sheet_name=factor)
        else: 
            print(factor)
            res['DATE'] = date
            res.to_excel(writer, sheet_name=factor)

    writer.close()
    writer.handles = None


# In[976]:


hr = '19'
filedate = ''
LIVE_SIM_id = ''
#new version
def create_reports(date, hr, str_month):
    import os
    daytype = find_daytype(date)

    total_capital=None
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    pathname = date
    ######################Read Files in######################
    print(date)
    #kbpath = 'Z:/NQC/nqc-live-sim/'+pathname+'/logs/'
    kbpath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+ date + '/'
    #kbfile0 = 'processor0.log'    
    #kbfile1 = 'processor1.log'
    #kbfile2 = 'processor2.log' 
    kbfile = date +' processorlogNSDQClose_prod.csv'
    #kbpath = '.'
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    opath = 'Z:/web-guys/data/ss-reports/'
    ofile0 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_0_'+date+'_order.csv'
    ofile1 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_1_'+date+'_order.csv'
    ofile2 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_2_'+date+'_order.csv'    
    trpath = 'Z:/web-guys/data/ss-reports/'
    trfile0 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_0_'+date+'_trade_reports.csv'
    trfile1 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_1_'+date+'_trade_reports.csv'
    trfile2 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_2_'+date+'_trade_reports.csv'
    evpath = 'Z:/web-guys/data/custom_logs/nsdq-03.dc.gptbs.com/NsdqImbalanceToClose/'
    datestr = date.replace('-','')
    evfile0 = 'NsdqImbalanceToClose_0_evals.log_'+datestr
    evfile1 = 'NsdqImbalanceToClose_1_evals.log_'+datestr
    evfile2 = 'NsdqImbalanceToClose_2_evals.log_'+datestr   
    daytype = days[days['Date']==date]
    daytype = daytype['Type'].squeeze()
    if kbfile not in os.listdir(kbpath):
        print('no kbfile '+kbfile)
    elif advfile not in os.listdir(advpath):
        print('no advfile '+advfile)
    elif ofile0 not in os.listdir(opath):
        print('no ofile '+ofile0)
    elif trfile0 not in os.listdir(trpath):
        print('no trfile '+trfile0)
    elif evfile0 not in os.listdir(evpath):
        print('no evfile '+evfile0)
    else:
        kb = pd.read_csv(kbpath + kbfile,header=None)
        
        #kbfiles = [kbfile0,kbfile1,kbfile2]
        #kbli = []
        #for file in kbfiles:
        #    df = pd.read_csv(kbpath + file,header=None, sep='\t')
        #    kbli.append(df)
        #kb = pd.concat(kbli)
        
        symbols = pd.read_csv(advpath + advfile)

        ofiles = [ofile0,ofile1,ofile2]
        oli = []
        for file in ofiles:
            df = pd.read_csv(opath + file)
            oli.append(df)
        orders = pd.concat(oli)

        trfiles = [trfile0,trfile1,trfile2]
        li = []
        for file in trfiles:
            df = pd.read_csv(trpath + file)
            li.append(df)
        tr = pd.concat(li)
        
        evfiles = [evfile0,evfile1,evfile2]
        li=[]
        for file in evfiles:
            df = pd.read_csv(evpath + file,header=None)
            li.append(df)
        ev = pd.concat(li)
        print('files present')
        
        if len(kb.index) < 500:
            print('kbfile incomplete '+date)
        elif len(orders.index) == 0:
            print('ofile incomplete '+date)
        elif len(tr.index) == 0:
            print('trfile incomplete '+date)
        elif len(ev.index) == 0:
            print(len(ev.index))
        else:
            print(len(kb.index))
            ######################Prep Evals File######################
            #Prep message column
            ev['Date'] = ev[0].str.extract('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})',expand=True)
            ev = ev[['Date',0]]
            ev[0] = ev[0].str.replace('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_0 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_1 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_2 )','')
            ev[0] = ev[0].str.replace('INFO|WARN|ERROR','')
            ev['Date']=pd.to_datetime(ev['Date'])
            ev['Date']=ev['Date']+pd.Timedelta(hours=5)
            kb = pd.read_csv(kbpath + kbfile,header=None)
            kb.pop(0)
            kb.rename(columns={1:'Date',2:0},inplace=True)
            kb['Date'] = pd.to_datetime(kb['Date'])
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb['Day_UTC'] = kb['Date'].dt.tz_localize('UTC') + pd.Timedelta(hours=5)
            kb['Day_CT'] = kb['Day_UTC'].dt.tz_convert('US/Central')
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb = pd.concat([kb,ev])
            kb_original = kb
            kb = kb.sort_values('Date').reset_index(drop=True)
            #Create separate dataframes of relevant info and then remove from KB
            capman = kb[kb[0].str.contains('CapMan|BP Change',regex=True)]
            kb = kb.iloc[kb.index.drop(capman.index)]
            kb = kb.reset_index(drop=True)
            evals = kb[kb[0].str.contains('Eval')]
            kb = kb.iloc[kb.index.drop(evals.index)]
            kb = kb.reset_index(drop=True)
            orderlog = kb[kb[0].str.contains('Order-')]
            kb = kb.iloc[kb.index.drop(orderlog.index)]
            kb = kb.reset_index(drop=True)
            attempts = kb[kb[0].str.contains('Attempting|Packet-|flipped|Attempt-')]
            kb = kb.iloc[kb.index.drop(attempts.index)]
            kb = kb.reset_index(drop=True)
            exits = kb[kb[0].str.contains('Exit-|Calc-Uncovered')]
            kb = kb.iloc[kb.index.drop(exits.index)]
            kb = kb.reset_index(drop=True)
            ope = kb[kb[0].str.contains('OPE-')]
            kb = kb.iloc[kb.index.drop(ope.index)]
            kb = kb.reset_index(drop=True)
            closeout = kb[kb[0].str.contains('Closeout-')]
            kb = kb.iloc[kb.index.drop(closeout.index)]
            kb = kb.reset_index(drop=True)
            profit = kb[kb[0].str.contains('Profit-')]
            kb = kb.iloc[kb.index.drop(profit.index)]
            kb = kb.reset_index(drop=True)
            ten_min = kb[kb[0].str.contains('Set-10m-Last-Trade-Price')]
            kb = kb.iloc[kb.index.drop(profit.index)]
            kb = kb.reset_index(drop=True)            
            #kb = kb[~kb[0].str.contains('m price')]
            #Find total capital available for the day
            t = capman[capman[0].str.contains('Successful ')]
            t = t.head(1)
            t  = t.drop_duplicates()
            total_capital = t[0].str.extract('((?<=available:)\S{1,})').squeeze()
            total_capital = total_capital.replace(',','')
            total_capital = int(total_capital)            
            print('Total Capital is: {0}'.format(total_capital))
            
            ######################
            #Read in ADV files
            adv, nsdq_symbols = get_symbolslist(date)
            
            #Formatting evals files to breakout columns:
            evals[['Status','SYMBOL','message']] =evals[0].str.split(' ',n=2,expand=True)
            types = ['Eval-Passed','Oppset-Eval-Rescore']
            passed = evals[evals['Status'].isin(types)]
            passed['Auction_Size'] = passed['message'].str.extract('((?<=auction_size:)\S{1,})')
            passed['Threshold'] = passed['message'].str.extract('((?<=threshold:)\S{1,})')
            passed['MQ'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
            passed['ARTI'] = passed['message'].str.extract('((?<=arti:)\S{1,})')
            passed['ARTI'] = passed['ARTI'].astype('float')
            passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
            passed['MQAS'] = passed['message'].str.extract('((?<=mqas:)\S{1,})')
            passed['MQAS_Score'] = passed['message'].str.extract('((?<=mqas_score:)\S{1,})')
            passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
            passed['IADV_Score'] = passed['message'].str.extract('((?<=iadv_score:)\S{1,})')
            passed['PRI'] = passed['message'].str.extract('((?<=pri:)\S{1,})')
            passed['PRI_Score'] = passed['message'].str.extract('((?<=pri_score:)\S{1,})')
            passed['Total_Score'] = passed['message'].str.extract('((?<= score:)\S{1,})')
            passed['Total_Score'] = np.where(passed['Status']=='Eval-Passed',passed['Total_Score'],0)
            cols = ['MQ','ARTI','IADV','MQAS','MQAS_Score','IADV','IADV_Score','PRI','PRI_Score','Total_Score']
            passed[cols] = passed[cols].astype('float').values
            passed.sort_values('Date')
            a1 = (passed['Total_Score']>100)
            a2 = (passed['Total_Score'].between(75,100,inclusive='left'))
            a3 = (passed['Total_Score'].between(50,75,inclusive='left'))
            a4 = (passed['Total_Score'].between(20,50,inclusive='left'))
            vals = [.1,.08,.05,.02]
            default = .02
            passed['Max_Imbalance'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = [.02,.016,.01,.004]
            defaul = .004
            passed['Max_Capital'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = ['A','B','C','D']
            default = 'D'
            passed['Score_Brackets'] = np.select([a1,a2,a3,a4],vals,default=default)
            passed['Desired_Shares_Imbalance'] = passed['Max_Imbalance']*passed['ARTI']
            passed['Desired_Shares_Capital'] = passed['Max_Capital']*total_capital
            passed = passed.set_index('Date')
            passed.index = pd.to_datetime(passed.index)
            MQAS_Score(passed)
            IADV_Score(passed)
            PRI_Score(passed)
            passed = passed.reset_index().drop_duplicates().set_index('Date')
            passed['Day_Type'] = daytype

            
            fails = evals[evals[0].str.contains('Eval-Failed')]
            fails[['Status','SYMBOL','message']] =fails['message'].str.split(' ',n=2,expand=True)
            fails['Failed_Reason'] = fails['message'].str.extract('((?!:)^\D{1,20})')
            fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('-','')
            fails = fails.set_index('Date')
            fails = fails.reset_index().drop_duplicates().set_index('Date')
            fails['Day_Type'] = daytype
            
            #Breakout attempts columns
            attempts['SYMBOL']= attempts[0].str.extract('([A-Z]{2,6})')
            attempts['Desired'] = attempts[0].str.extract('((?<=desired:)\S{1,})')
            attempts['Existing'] = attempts[0].str.extract('((?<=existing:)\S{1,})')
            attempts['Existing'] = attempts['Existing'].str.replace(',','')
            attempts['Net_Desired'] = attempts[0].str.extract('((?<=net_desired:)\S{1,})')
            attempts['Flip_Count'] = attempts[0].str.extract('((?<=flipped )\S{1,})')
            attempts['Packet_Score'] = attempts[0].str.extract('((?<=score:)\S{1,})')
            attempts = attempts.drop_duplicates()
            attempts['Day_Type'] = daytype

            #Breakout attempts columns
            if len(orderlog) == 0:
                pass
            else:
                orderlog[['Status','SYMBOL','message']]=orderlog[0].str.split(' ',n=2,expand=True)
                orderlog['Price_Action'] = orderlog['message'].str.extract('((?<=price-action:)\S{1,})')
                orderlog['Order_Price'] = orderlog['message'].str.extract('((?<=price:)\S{1,})')
                orderlog['Order_Size'] = orderlog['message'].str.extract('((?<=size:)\S{1,})')
                orderlog['Abort_Reason'] = orderlog['message'].str.extract('((?<=reason:)\S{1,})') 
                orderlog = orderlog.drop_duplicates()

            #Breakout exits columns
            exits = exits.reset_index(drop=True)
            exits_params = exits[exits[0].str.contains('Starting')]
            exits = exits.iloc[exits.index.drop(exits_params.index)]
            exits = exits.reset_index(drop=True)
            exits[['Status','SYMBOL','message']]=exits[0].str.split(' ',n=2,expand=True)
            exits['Exit_Type'] = exits['message'].str.extract('((?<= type:)\S{1,})')
            exits['Exit_To_Cover'] = exits['message'].str.extract('((?<=-cover:)\S{1,})')
            exits['Gross_Position'] = exits['message'].str.extract('((?<=gross:)\S{1,})')
            exits['Uncovered_Position'] = exits['message'].str.extract('((?<=uncovered:)\S{1,})')
            exits['Skipped_LO_Size'] = exits['message'].str.extract('((?<=skipped-lo-size:)\S{1,})')
            exits['Unmarketable_Size'] = exits['message'].str.extract('((?<=unmarketable-size:)\S{1,})')
            exits['Unmarketable_on_Opposite_Side'] = exits['message'].str.extract('((?<=unmarketable-on-opposite-side:)\S{1,})')
            exits['Exit_Price'] = exits['message'].str.extract('((?<=price:)\S{1,})')
            exits['Exit_New_Price'] = exits['message'].str.extract('((?<=new-price:)\S{1,})')
            exits['Exit_Price'] = np.where(exits['Status']=='Exit-Repricer',exits['Exit_New_Price'],exits['Exit_Price'])
            exits = exits.sort_values('Date').reset_index(drop=True)
            fill = exits.groupby(['SYMBOL'])['Uncovered_Position'].fillna(method='ffill')
            exits['Uncovered_Position'] = np.where(exits['Exit_Type']=='LO',fill ,exits['Uncovered_Position'])            
            exits['Desired_Exit_Size'] = exits['message'].str.extract('((?<=desired-size:)\S{1,})')
            exits['Net_Exit_Size'] = exits['message'].str.extract('((?<=net-size:)\S{1,})')
            exits['Canceled_Exit_Size'] = exits['message'].str.extract('((?<=canceled-size:)\S{1,})')
            exits['Exit_Type'] = np.where(exits['Status']=='Exit-Replace-LO','LO',exits['Exit_Type'])
            exits['Exit_Type'] = np.where(exits['Status']=='Exit-Repricer','LO',exits['Exit_Type'])
            exits['Bid'] = exits['message'].str.extract('((?<=bid:)\S{1,})')
            exits['Ask'] = exits['message'].str.extract('((?<=ask:)\S{1,})')
            exits['Avg_Price'] = exits['message'].str.extract('((?<=avg-price:)\S{1,})')
            exits['ICP'] = exits['message'].str.extract('((?<=icp:)\S{1,})')
            exits['10minRef'] = exits['message'].str.extract('((?<=10minRef:)\S{1,})')
            exits['5minRef'] = exits['message'].str.extract('((?<=5minRef:)\S{1,})')
            num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
            exits[num_cols] = exits[num_cols].astype('float')
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LOC')
            exits['LOC_Ref_Boundary'] = np.where(condition==True,exits[['10minRef','5minRef']].min(axis=1),exits[['10minRef','5minRef']].max(axis=1))
            exits['LOC_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
            exits['LOC_Price_Ideal'] = np.where(condition==True,exits[['LOC_Cross_Price','Avg_Price']].min(axis=1),exits[['LOC_Cross_Price','Avg_Price']].max(axis=1))
            exits['LOC_Price_Calc'] = np.where(condition==True,exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].max(axis=1),exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].min(axis=1))
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='IO')            
            exits['IO_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
            exits['IO_Price_Calc'] = np.where(condition==True,exits[['IO_Cross_Price','Avg_Price']].min(axis=1),exits[['IO_Cross_Price','Avg_Price']].max(axis=1))
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LO')         
            exits['Midpoint_ICP_NBBO'] = np.where(condition==True,(exits['Ask']+exits['ICP'])/2,(exits['Bid']+exits['ICP'])/2)
            exits['Midpoint_ICP_AvgPrice'] = (exits['Avg_Price']+exits['ICP'])/2
            exits['LO_Price_Calc'] = np.where(condition==True,exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].min(axis=1),exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].max(axis=1))
            exits['LO_Price_Calc'] = round(exits['LO_Price_Calc'],2)
            num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
            exits[num_cols] = exits[num_cols].astype('float')
            exits = exits.sort_values('Date')
            exits = exits.drop_duplicates()
            exits['Day_Type'] = daytype
            
            #####Breakout profit taker columns:
            profit = profit[~profit[0].str.contains('Cycle')]
            if len(profit.index)==0:
                print('profit file empty')
            else:
                profit[['Status','SYMBOL','message']] = profit[0].str.split(' ',n=2,expand=True)
                profit['Price'] = profit['message'].str.extract('((?<= price:)\S{1,})').astype('float')
                profit['Bid'] = profit['message'].str.extract('((?<=bid:)\S{1,})').astype('float')
                profit['Ask'] = profit['message'].str.extract('((?<=ask:)\S{1,})').astype('float')
                profit['Uncovered_Position'] = profit['message'].str.extract('((?<=uncovered:)\S{1,})').astype('float')
                profit['10min_Last_Trade'] = profit['message'].str.extract('((?<=ten-min-last-trade:)\S{1,})').astype('float')
                profit['Price_Action_Multiple'] = profit['message'].str.extract('((?<=pa-mult:)\S{1,})').astype('float')
                profit['Side'] = profit['message'].str.extract('((?<=side:)\S{1,})')
                profit['Size'] = profit['message'].str.extract('((?<= size:)\S{1,})')
                profit = profit.sort_values(['SYMBOL','Date']).reset_index(drop=True)
                profit['Day_Type'] = daytype

            #Breakout ope columns
            if len(ope.index)==0:
                print('ope file empty')
            else:
                ope[['Status','SYMBOL','message']]=ope[0].str.split(' ',n=2,expand=True)
                ope['Uncovered_Position'] = ope['message'].str.extract('((?<=uncovered:)\S{1,})')
                ope['OPE_Price'] = ope['message'].str.extract('((?<= price:)\S{1,})')
                ope['Bid'] = ope['message'].str.extract('((?<=bid:)\S{1,})')
                ope['Ask'] = ope['message'].str.extract('((?<=ask:)\S{1,})')
                ope['10min_Ref'] = ope['message'].str.extract('((?<=10min-ref:)\S{1,})')
                ope['Last_Ref'] = ope['message'].str.extract('((?<=last-ref:)\S{1,})')
                ope['ICP'] = ope['message'].str.extract('((?<=icp:)\S{1,})')
                ope['Opportunity_Cost'] = ope['message'].str.extract('((?<=opportunity-cost:)\S{1,})')
                ope['Side'] = ope['message'].str.extract('((?<= action:)\S{1,})')
                ope['PA_Prepacket'] = ope['message'].str.extract('((?<=pa-prepacket:)\S{1,})')
                ope['FF_Act_On_Packet'] = np.where(ope['PA_Prepacket'].notna(),1,0)
                ope['Cross_Percent'] = ope['message'].str.extract('((?<= cross-pct:)\S{1,})')
                ope['Price_Action'] = ope['message'].str.extract('((?<= price-action:)\S{1,})')
                ten_min[['x','y','Status','SYMBOL','10min']]=ten_min[0].str.split(' ',n=4,expand=True)
                ten_min = ten_min[['SYMBOL','10min']]
                ten_min['10min'] = ten_min['10min'].astype('float')
                ope = pd.merge(ope,ten_min,how='left',on='SYMBOL')
                ope['Time_Period']  = np.where(ope["Date"]<date +' 19:55','Pre5','Post5')
                
                #ope configs
                ope_pre5_pa_prepacket_threshold= 0.0030
                ope_pre5_low_pa_cross_pct= 0.0020
                ope_pre5_high_pa_pa_cross_pct= 0.60
                ope_pre5_max_cross_pct= 0.05
                ope_pre5_order_retry_count= 2
                ope_pre5_curr_pa_threshold= -0.0115
                ope_pre5_far_cross_pct= 0.003
                ope_post5_icp_max_cross_pct= 0.01
                min_pri = 0.0020
                min_pri_multiplier = 0.25

                a1 = (ope['PA_Prepacket'].notna()) & (ope['PA_Prepacket']>ope_pre5_pa_prepacket_threshold) & (ope['Time_Period']=='Pre5')
                a2 = (ope['PA_Prepacket'].notna()) & (ope['PA_Prepacket']<ope_pre5_pa_prepacket_threshold) & (ope['Time_Period']=='Pre5')
                a3 = (ope['Price_Action'].notna()) & (ope['Price_Action']<ope_pre5_curr_pa_threshold) & (ope['Time_Period']=='Pre5')
                a4 = (ope['Price_Action'].notna()) & (ope['Price_Action']>ope_pre5_curr_pa_threshold) & (ope['Time_Period']=='Pre5')
                a5 = (ope['Time_Period']=='Post5')
                vals = ['Packet_Agressive','Packet_Soft','Reprice_Agressive','Reprice_Soft','Post5']
                ope['OPE_Price_Type'] = np.select([a1,a2,a3,a4,a5],vals,np.nan)

                ope = ope.drop_duplicates()
                
            closeout[['Status','SYMBOL','message']]=closeout[0].str.split(' ',n=2,expand=True)
            closeout['Uncovered_Position'] = closeout['message'].str.extract('((?<=uncovered:)\S{1,})')
            closeout['Bid'] = closeout['message'].str.extract('((?<=bid:)\S{1,})')
            closeout['Ask'] = closeout['message'].str.extract('((?<=ask:)\S{1,})')
            closeout['ICP'] = closeout['message'].str.extract('((?<=icp:)\S{1,})')
            closeout['Closeout_Type'] = closeout['message'].str.extract('((?<=strategy:)\S{1,})')
            closeout['Closeout_Price'] = closeout['message'].str.extract('((?<=new-price:)\S{1,})')
            closeout = closeout[~closeout[0].str.contains('Start')]
            closeout = closeout.drop_duplicates()
            closeout['Day_Type'] = daytype
            


            ######################
            #Read in Orders Report
            orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
            orders['EntryTime_ms'] = orders['EntryTime'].dt.microsecond
            orders['LastModTime'] = pd.to_datetime(orders['LastModTime'])
            orders = orders.groupby(['OrderId']).tail(1)
            orders.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            orders['QTY_Abs'] = np.absolute(orders['Quantity'])
            orders['Desired_$'] = orders.QTY_Abs * orders.Price
            orders['FilledQTY_Abs'] = np.absolute(orders['FilledQty'])
            orders['Filled$_Abs'] = orders['FilledQTY_Abs']*orders['AvgFillPrice']
            orders['Filled$'] = orders['FilledQty']*orders['AvgFillPrice']            
            orders['Fill_Rate'] = orders['FilledQTY_Abs']/orders['QTY_Abs']
            orders['DATE'] = orders['EntryTime'].dt.date
            orders['Capital_Available'] = total_capital
           
            #Identifying Exit orders on Orders Report 
            exitid = exits[exits['Exit_Type'].notna()]
            exitid = exitid[['SYMBOL','Exit_Type','Date']]
            exitid = exitid.sort_values('Date')
            exitid['TIF'] = np.where(exitid['Exit_Type']=='LO','DAY','AT_THE_CLOSE')
            exitid.rename(columns={'Exit_Type':'EntryType','Date':'EntryTime'},inplace=True)
            orders = orders.sort_values('EntryTime')
            orders = pd.merge_asof(orders, exitid, by=['SYMBOL','TIF'],left_on=['EntryTime'], right_on=['EntryTime'],direction='nearest',tolerance=pd.Timedelta("100ms"))     
            
            #Identifying Profit-Taker orders on Orders Report
            if len(profit.index) == 0:
                print('profit file is empty')
            else:
                profit['Date'] = pd.to_datetime(profit['Date'])
                profit_merge = profit[['SYMBOL','Date','Price_Action_Multiple','Status','Price']]
                profit_merge.rename(columns={'Date':'EntryTime'},inplace=True)
                profit_merge['Price'] = profit_merge['Price'].round(2)
                profit_merge = profit_merge.sort_values('EntryTime')
                orders = pd.merge_asof(orders,profit_merge,left_on=['EntryTime'],right_on=['EntryTime'],by=['SYMBOL','Price'],direction='nearest')
                orders['EntryType'] = np.where(orders['Status']=='Profit-Taker',orders['Status'],orders['EntryType'])
                orders.pop('Status')
            
            #Identifying types of OPE orders
            if len(ope.index)==0:
                print('ope file empty')
            else:
                ope_merge = ope[['Date','SYMBOL','OPE_Price_Type','OPE_Price']]
                ope_merge.rename(columns={'Date':'EntryTime','OPE_Price':'Price'},inplace=True)
                ope_merge = ope_merge.sort_values('EntryTime')
                ope_merge['EntryTime'] = pd.to_datetime(ope_merge['EntryTime'])
                ope_merge['Price'] = ope_merge['Price'].round(2)
                orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
                orders = pd.merge_asof(orders,ope_merge,left_on=['EntryTime'],right_on=['EntryTime'],by=['SYMBOL','Price'],direction='nearest')
            
            #Identifying Exit orders on Orders Report 
            orders['EntryType'] = np.where(orders['TIF']=='IOC','OPE',orders['EntryType'])
            orders['EntryType'] = np.where(orders['EntryType'].isna(),'Entry',orders['EntryType'])
            orders['TIF_in_seconds'] = (orders['LastModTime'] - orders['EntryTime']).dt.total_seconds()
            orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
            orders['Time_Diff'] = (orders['EntryTime'] - orders.groupby(['SYMBOL','Long_Short','EntryType'])['EntryTime'].shift(1)).dt.total_seconds().fillna(100)
            orders['Time_Diff_Bool'] = orders['Time_Diff']>15
            orders['Unique'] = orders.groupby(['SYMBOL'])['Time_Diff_Bool'].cumsum()
            orders['QTY_Abs_Agg'] = orders.groupby(['SYMBOL','Long_Short','EntryTime'])[['QTY_Abs']].transform('sum')
            orders['QTY_Max_Desired'] = orders.groupby(['SYMBOL','Long_Short','Unique'])[['QTY_Abs_Agg']].transform('max')
            orders['$_Max_Desired'] = orders['QTY_Max_Desired'] * orders['Price']
            x = orders.groupby(['SYMBOL','Unique'])[['SYMBOL','Unique','Price','QTY_Max_Desired','$_Max_Desired']].head(1)
            x.rename(columns={'Price':'Start_Price'},inplace=True)
            orders.drop(columns=['QTY_Max_Desired','$_Max_Desired'],inplace=True)
            orders = pd.merge(orders,x,how='left')
            a1 = (orders['Start_Price']<5)
            a2 = (orders['Start_Price'].between(5,10,inclusive='left'))
            a3 = (orders['Start_Price'].between(10,15,inclusive='left'))
            a4 = (orders['Start_Price'].between(15,30,inclusive='left'))
            a5 = (orders['Start_Price'].between(30,60,inclusive='left'))
            a6 = (orders['Start_Price']>=60)
            vals = [0,.01,.02,.03,.04,.05]
            default = 0
            orders['Cross_Amount'] = np.select([a1,a2,a3,a4,a5,a6],vals,default)
            orders['Start_NBBO'] = np.where(orders['Quantity']>0,orders['Start_Price']-orders['Cross_Amount'],orders['Start_Price']+orders['Cross_Amount'])
            orders['Start_NBBO'] = np.where(orders['AvgFillPrice']==0,np.nan,orders['Start_NBBO'])
            orders['Day_Type'] = daytype

            #Confirm no lines with unassigned EntryType
            checkx = orders[orders['EntryType']=='X']
            print('Number of unassigned order EntryTypes is {0}'.format(len(checkx))) 
            

            ######################
            #Read in Trade Reports
            tr.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            tr['EntryTime'] = pd.to_datetime(tr['EntryTime'])
            tr = tr.sort_values('EntryTime')
            tr = tr[tr['EntryTime']>date +' '+hr+':50:00']
            tr['DATE'] = tr['EntryTime'].dt.date
            tr['Day_of_Week'] = tr['EntryTime'].dt.dayofweek
            tr['Day_Type'] = daytype
            #Create slice of orders to only look at entries
            entries = orders[orders['EntryType']=='Entry']
            #deployed_by_symbol = entries[['SYMBOL','FilledQty','AvgFillPrice','LastModTime','EntryType','Filled$_Abs','FilledQTY_Abs','QTY_Abs','Capital_Available','Desired_$']].sort_values('LastModTime')
            #deployed_by_symbol.rename(columns={'FilledQty':'Quantity'},inplace=True)
            #tr = pd.merge_asof(tr,deployed_by_symbol,by=['SYMBOL','Quantity'],left_on=['EntryTime'],right_on=['LastModTime'],direction='nearest')
            tr['FilledQTY_Abs'] = np.absolute(tr['Quantity'])
            tr['Filled$_Abs'] = tr['FilledQTY_Abs'] * tr['EntryPrice']
            tr['Stuck'] = np.where(tr['ExitTime']=='1970-Jan-01 00:00:00','Stuck','Exited')
            cols = ['Quantity','RealizedPL','Filled$_Abs']
            tr[cols] = tr[cols].astype('float')
            trinst = tr.copy()
            x = tr.groupby(['SYMBOL'])[['Quantity','RealizedPL','Filled$_Abs']].sum().reset_index()
            tr.drop(columns=['Quantity','RealizedPL','Filled$_Abs'],inplace=True)
            tr = pd.merge(tr,x,how='left')
            tr.drop_duplicates(['SYMBOL'],inplace=True)
            tr['Basis_Return'] = tr['RealizedPL']/tr['Filled$_Abs']
            #Creating Trade Reports by 
            trinst['Basis_Return'] = trinst['RealizedPL']/trinst['Filled$_Abs']
            fortrinst = passed[['SYMBOL','MQ','ARTI','IADV','MQAS','MQAS_Score','IADV_Score','PRI','PRI_Score','Total_Score']].reset_index()
            fortrinst['Date'] = fortrinst['Date'].dt.tz_localize(None)
            fortrinst = fortrinst.sort_values(['SYMBOL','Date'])
            fortrinst['Total_Score'] = fortrinst['Total_Score'].replace(0,np.nan)
            fortrinst = fortrinst.fillna(method='bfill')
            fortrinst = fortrinst.sort_values('Date')
            trinst = pd.merge_asof(trinst,fortrinst,by=['SYMBOL'],left_on='EntryTime',right_on='Date',direction='nearest')
            trinst['ADV'] = trinst['ARTI'] / trinst['IADV']
            a1 = (trinst['Total_Score']>100)
            a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
            a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
            a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            trinst['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)

            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            trinst['ADV_Even_Brackets'] = pd.cut(trinst['ADV'], bins=cuts,duplicates='drop')
            trinst['MQ_Brackets'] = pd.cut(trinst['MQ'], bins=cuts,duplicates='drop')
            trinst['Imb_Brackets'] = pd.cut(trinst['ARTI'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0.00001,.1,.2,.3,.4,.5,.6,.7,.8,.9,1,2,5,np.inf]
            trinst['IADV_Even_Extended_Brackets'] = pd.cut(trinst['IADV'],bins=cuts)
            cuts = [-np.inf,0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]
            trinst['MQAS_Brackets'] = pd.cut(trinst['MQAS'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0,5,10,15,30,60,np.inf]
            trinst['Price_Brackets'] = pd.cut(trinst['EntryPrice'], bins=cuts)
            a1 = (trinst['EntryTime']>=(date + ' ' +hr + ':00')) & (trinst['EntryTime']<(date + ' ' +hr + ':55'))
            a2 = (trinst['EntryTime']>=(date + ' ' +hr + ':55')) & (trinst['EntryTime']<(date + ' ' +hr + ':58'))
            a3 = (trinst['EntryTime']>=(date + ' ' +hr + ':58'))
            vals = ['Entry50', 'Entry55', 'Entry58']
            trinst['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
            
            orders = pd.merge_asof(orders,fortrinst,by=['SYMBOL'], left_on='EntryTime', right_on='Date',direction='nearest')
            a1 = (orders['Total_Score']>100)
            a2 = (orders['Total_Score'].between(75,100,inclusive='left'))
            a3 = (orders['Total_Score'].between(50,75,inclusive='left'))
            a4 = (orders['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            orders['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)
            orders['ADV'] = orders['ARTI'] / orders['IADV']
            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            orders['ADV_Even_Brackets'] = pd.cut(orders['ADV'], bins=cuts,duplicates='drop')
            #Adding EntryTiming
            a1 = (orders['EntryTime']>=(date + ' ' +hr + ':00')) & (orders['EntryTime']<(date + ' ' +hr + ':55'))
            a2 = (orders['EntryTime']>=(date + ' ' +hr + ':55')) & (orders['EntryTime']<(date + ' ' +hr + ':58'))
            a3 = (orders['EntryTime']>=(date + ' ' +hr + ':58'))
            vals = ['Entry50', 'Entry55', 'Entry58']
            orders['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
            orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
            #Adding columns for price slippage

            orders['Slippage_$'] = np.where(orders['Quantity']>0,orders['Start_NBBO']-orders['AvgFillPrice'],orders['AvgFillPrice']-orders['Start_NBBO'])
            orders['Slipage_%'] = orders['Slippage_$']/orders['Start_NBBO']
            orders['Weighted_Price'] = orders['$_Max_Desired']*orders['Start_NBBO']
            orders['Slippage_$$, $Weighted'] = orders['$_Max_Desired']*orders['Slippage_$']
            orders['Weighted_Shares'] = orders['QTY_Max_Desired']*orders['Start_NBBO']
            orders['Slippage_$,Shares Weighted'] = orders['QTY_Max_Desired']*orders['Slippage_$']
            orders['Percent_of_Imbalance'] = orders['FilledQTY_Abs']/orders['ARTI']
            orders['Percent_of_Capital'] = orders['Filled$_Abs']/orders['Capital_Available']
            orders['on_packet'] = orders['EntryTime_ms']!=500000


            stuckpnlfile = date + ' suckpnl.csv'
            if stuckpnlfile in os.listdir('.'):
                stuckpnl = pd.read_csv(date +' stuckpnl.csv')
                stuckpnl.rename(columns={'Symbol':'SYMBOL'},inplace=True)
                trinst['Percent_of_Total'] = trinst.groupby('SYMBOL')['Quantity'].apply(lambda column: column/column.sum())
                trinst = pd.merge(trinst,stuckpnl,how='left')
                trinst['Net'] = trinst['Net'].fillna(0)
                trinst['StuckPNL'] = trinst['Percent_of_Total']*trinst['Net']
                trinst['Adj_RealizedPL'] = trinst['RealizedPL'] + trinst['StuckPNL']
            else:
                print('no stuckpnl file')

            #Combining attemps-df and orderlog-df
            combo = pd.merge(attempts, orderlog,how='outer')
            combo = combo.sort_values('Date')
            alllogs = pd.concat([orderlog, attempts, exits,ope,closeout,passed.reset_index()],keys=['orderlog', 'attempts', 'exits','ope','closeout','passed']).reset_index()
            
            alllogs.to_csv(writepath + date + ' alllogs.csv')
            tr.to_csv(writepath + date + ' tradereports.csv')
            trinst.to_csv(writepath + date + ' tr_byinstance.csv')
            orders.to_csv(writepath + date +' orders.csv')
            combo.to_csv(writepath + date +' orderslog.csv')
            exits.to_csv(writepath + date +' exits.csv')
            ope.to_csv(writepath + date +' ope.csv')
            closeout.to_csv(writepath + date +' closeout.csv')
            passed.to_csv(writepath + date +' passed.csv')
            capman.to_csv(writepath + date +' capman.csv')
            kb.to_csv(writepath + date +' kb.csv')
            profit.to_csv(writepath + date + ' profit.csv')
            ten_min.to_csv(writepath + date + ' ten_min.csv')
            entries = orders[orders['EntryType']=='Entry']            
            ope = orders[orders['EntryType']=='OPE']


# In[977]:


def write_profittaker_reports(date):
    orders = cf.read_reports(date,'orders')
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    pf = orders[orders['EntryType']=='Profit-Taker']
    pf_summary = pf.groupby(['SYMBOL','Price_Action_Multiple']).agg({'Filled$_Abs':'sum',
                                                        'TIF_in_seconds':'mean'}).reset_index()
    with pd.ExcelWriter(writepath + date+'_summary.xlsx',mode='a', if_sheet_exits='replace',engine="openpyxl") as writer:
        pf_summary.to_excel(writer, sheet_name='Profit-Taker')


# In[978]:


def write_exit_reports(date):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    orders = cf.read_reports(date,'orders')
    tr = cf.read_reports(date,'tr')
#Confirm exit orders are put out for all entry orders
    tb = orders[orders['EntryType']=='Entry']
    table = tb.groupby('SYMBOL')['FilledQty'].sum().reset_index()
    tb = orders[orders['EntryType']!='Entry']
    x = tb.groupby('SYMBOL')['FilledQty'].sum().reset_index()
    table = pd.merge(table,x,on='SYMBOL',suffixes=['_Entry','_Exit'])
    table['Stuck'] = table['FilledQty_Entry'] + table['FilledQty_Exit']
    x = tb.groupby(['SYMBOL','EntryType'])[['FilledQty']].sum().reset_index().set_index('SYMBOL',drop=True)
    x = x.pivot(columns='EntryType',values='FilledQty').reset_index()
    exittable = pd.merge(table,x)
    stucktable = exittable[exittable['Stuck']!=0]
    prices = tr[['SYMBOL','EntryPrice']]
    stucktable = pd.merge(stucktable, prices)
    stucktable['NotionalStuck'] = np.absolute(stucktable['Stuck'])*stucktable['EntryPrice']
    stucktable['Flipped_Position'] = np.absolute(stucktable['FilledQty_Entry'])-np.absolute(stucktable['FilledQty_Exit'])<0
    stuck = stucktable['SYMBOL'].nunique()
    symbolcount = exittable['SYMBOL'].nunique()
    stuck_percent = stuck/symbolcount

    #Calculate symbol count % filled on exit orders
    exittypecols = ['SYMBOL','IO', 'LO','LOC', 'MOC','OPE']
    ordercols = orders['EntryType'].unique().tolist()
    keepcols = [i for i in ordercols if i in exittypecols]
    keepcols.append('SYMBOL')

    tablemelt = exittable[keepcols]
    tablemelt = tablemelt.melt(id_vars=['SYMBOL'])
    tablemelt = tablemelt.dropna()
    res = tablemelt.groupby('variable')[['value']].count()
    res.rename(columns={'value':'Symbol_Count'},inplace=True)
    res['Whiffed_Symbols'] = tablemelt.groupby('variable')[['value']].apply(lambda column: (column ==0).sum())
    res['Percent_Whiffied'] = res['Whiffed_Symbols']/res['Symbol_Count']
    res['DATE'] = date
    res['Day_Type'] = cf.find_daytype(date)

    exitfills = res 
    #writerappend(res, date+'_summary.xlsx', 'Exit_Fills')
    with pd.ExcelWriter(writepath + date+'_summary.xlsx',mode='a', if_sheet_exits='replace',engine="openpyxl") as writer:
        stucktable.to_excel(writer, sheet_name='Exit_Table')
    with pd.ExcelWriter(writepath + date+'_summary.xlsx',mode='a', if_sheet_exits='replace',engine="openpyxl") as writer:
        exitfills.to_excel(writer, sheet_name='Exit_Fills')
    #res.to_excel(writer, sheet_name='Exits')


# In[979]:


def write_evals_reports(df):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    fails = df.reset_index()
    fails['Day_UTC'] = fails['Date'].dt.tz_localize('UTC') + pd.Timedelta(hours=4)
    fails['Date'] = fails['Day_UTC'].dt.tz_convert('US/Central')
    fails.drop(columns=['Day_UTC'],inplace=True)
    fails['Date'] = fails['Date'].dt.tz_localize(None)
    fails = fails.set_index('Date')

    #Count of scores passed and failed reasons
    table = passed.groupby([pd.Grouper(freq='1s'),'Score_Brackets'])[['SYMBOL']].count()
    table = table.reset_index().set_index('Date')
    table = table.pivot(columns='Score_Brackets',values='SYMBOL')
    x = passed.groupby([pd.Grouper(freq='1s'),'Status'])[['SYMBOL']].count()
    x = x.reset_index().set_index('Date')
    x = x.pivot(columns='Status',values='SYMBOL')
    table = pd.merge(table.reset_index(),x.reset_index())

    x = fails.groupby([pd.Grouper(freq='1s'),'Failed_Reason'])[['SYMBOL']].count()
    x = x.reset_index().set_index('Date')
    x = x.pivot(columns='Failed_Reason',values='SYMBOL')
    table = pd.merge(table,x.reset_index())

    x = orderlog[(orderlog['Abort_Reason'].notna()) & (orderlog['Abort_Reason']!='adjusted-net-desired-is-zero')]
    x = x.set_index('Date')
    x = x.groupby([pd.Grouper(freq='1s'),'Abort_Reason'])[['SYMBOL']].count()
    x = x.reset_index().set_index('Date')
    x = x.pivot(columns='Abort_Reason',values='SYMBOL')
    table = pd.merge(table,x.reset_index())

    cols = table.columns.to_list()
    i = 0
    while i < len(cols):
        if cols[i] == 'Eval-Passed':
            location = i
            i = len(cols)
        else:
            i = i+1
    table['Total'] = table[cols[location:]].sum(axis=1)
    evalsovertime = table

    with pd.ExcelWriter(writepath + date+'_summary.xlsx',mode='a', if_sheet_exits='replace',engine="openpyxl") as writer:
        evalsovertime.to_excel(writer, sheet_name='Evals_Over_Time')


# In[980]:


def write_bytrade_reports(date):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    trinst = cf.read_reports(date,'trinst')
    #Details by Symbol
    df = trinst
    factor=['SYMBOL','EntryTiming','Score_Bracket']
    u_factor = ['SYMBOL','EntryTiming','Score_Bracket']
    x_factor = ['SYMBOL','EntryTiming','Score_Bracket']
    if 'DATE' in u_factor:
        pass
    else:
        u_factor.append('DATE')
    if 'SYMBOL' in u_factor:
        pass
    else:
        u_factor.append('SYMBOL')
    df['DATE'] = df['DATE'].astype(str)
    res = df.groupby(factor)[['Filled$_Abs']].sum().reset_index()
    res.rename(columns={'Filled$_Abs':'Capital_Deployed'},inplace=True)
    try:
        total_capital
    except NameError:
        total_capital = 0
    res['Total_Capital'] = total_capital
    tb = df.groupby(factor)[['FilledQTY_Abs']].sum()*2
    res = pd.merge(res,tb.reset_index(),how='outer')
    #tb = df.groupby([factor])[['Capital_Available']].mean().reset_index()
    #res = pd.merge(res,tb,how='outer')
    #RealizedPL and Basis Return
    tb = df.groupby(factor)[['RealizedPL']].sum().reset_index()
    res = pd.merge(res,tb,how='outer')
    res['Basis_Return'] = res['RealizedPL']/res['Capital_Deployed']
    res['DATE'] = date
    res['Day_Type'] = cf.find_daytype(date)

    tb = df[df['Stuck']=='Stuck'].reset_index()
    tbsummary = tb.groupby(u_factor)[['SYMBOL']].nunique()
    tbsummary.rename(columns={'SYMBOL':'Stuck_Count'},inplace=True)
    tbsummary = tbsummary.reset_index()
    tbsummary['DATE'] = date
    res = pd.merge(res,tbsummary,how='outer')
    res['Stuck_Count'] = res['Stuck_Count'].fillna(0)
    resbytrade = res
    res['Day_Type'] = cf.find_daytype(date)

    with pd.ExcelWriter(writepath + date+'_summary.xlsx',mode='a', if_sheet_exits='replace',engine="openpyxl") as writer:
        resbytrade.to_excel(writer, sheet_name='bySymbolTrade')


# In[981]:


def write_symbol_reports(date):
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    trinst = cf.read_reports(date,'trinst')
    #Details by Symbol
    df = trinst
    factor=['SYMBOL']
    u_factor = ['SYMBOL']
    x_factor = ['SYMBOL']
    if 'DATE' in u_factor:
        pass
    else:
        u_factor.append('DATE')
    if 'SYMBOL' in u_factor:
        pass
    else:
        u_factor.append('SYMBOL')
    df['DATE'] = df['DATE'].astype(str)
    res = df.groupby(factor)[['Filled$_Abs']].sum().reset_index()
    res.rename(columns={'Filled$_Abs':'Capital_Deployed'},inplace=True)
    try:
        total_capital
    except NameError:
        total_capital = 0
    res['Total_Capital'] = total_capital
    tb = (df.groupby(factor)[['FilledQTY_Abs']].sum()*2).reset_index()
    res = pd.merge(res,tb,how='outer')
    #tb = df.groupby([factor])[['Capital_Available']].mean().reset_index()
    #res = pd.merge(res,tb,how='outer')
    #RealizedPL and Basis Return
    tb = df.groupby(factor)[['RealizedPL']].sum().reset_index()
    res = pd.merge(res,tb,how='outer')
    res['Basis_Return'] = res['RealizedPL']/res['Capital_Deployed']
    res['DATE'] = date

    tb = df[df['Stuck']=='Stuck'].reset_index()
    tbsummary = tb.groupby(u_factor)[['SYMBOL']].nunique()
    tbsummary.rename(columns={'SYMBOL':'Stuck_Count'},inplace=True)
    tbsummary = tbsummary.reset_index()
    tbsummary['DATE'] = date
    res = pd.merge(res,tbsummary,how='outer')
    res['Stuck_Count'] = res['Stuck_Count'].fillna(0)
    res['Day_Type'] = cf.find_daytype(date)
    resbysymbol = res


    with pd.ExcelWriter(writepath + date+'_summary.xlsx',mode='a', if_sheet_exits='replace',engine="openpyxl") as writer:
        resbysymbol.to_excel(writer, sheet_name='bySymbol')


# In[982]:


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


# In[983]:


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


# In[984]:


hr = '19'
filedate = ''
LIVE_SIM_id = ''
def create_reports_thru_20220519(date, hr, str_month):
    total_capital=None
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    pathname = date
    ######################Read Files in######################
    print(date)
    #kbpath = 'Z:/NQC/nqc-live-sim/'+pathname+'/logs/'
    kbpath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+ date + '/'
    #kbfile0 = 'processor0.log'    
    #kbfile1 = 'processor1.log'
    #kbfile2 = 'processor2.log' 
    kbfile = date +' processorlogNSDQClose_prod.csv'
    #kbpath = '.'
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    opath = 'Z:/web-guys/data/ss-reports/'
    ofile0 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_0_'+date+'_order.csv'
    ofile1 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_1_'+date+'_order.csv'
    ofile2 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_2_'+date+'_order.csv'    
    trpath = 'Z:/web-guys/data/ss-reports/'
    trfile0 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_0_'+date+'_trade_reports.csv'
    trfile1 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_1_'+date+'_trade_reports.csv'
    trfile2 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_2_'+date+'_trade_reports.csv'
    evpath = 'Z:/web-guys/data/custom_logs/nsdq-03.dc.gptbs.com/NsdqImbalanceToClose/'
    datestr = date.replace('-','')
    evfile0 = 'NsdqImbalanceToClose_0_evals.log_'+datestr
    evfile1 = 'NsdqImbalanceToClose_1_evals.log_'+datestr
    evfile2 = 'NsdqImbalanceToClose_2_evals.log_'+datestr   
    daytype = days[days['Date']==date]
    daytype = daytype['Type'].squeeze()
    if kbfile not in os.listdir(kbpath):
        print('no kbfile '+kbfile)
    elif advfile not in os.listdir(advpath):
        print('no advfile '+advfile)
    elif ofile0 not in os.listdir(opath):
        print('no ofile '+ofile0)
    elif trfile0 not in os.listdir(trpath):
        print('no trfile '+trfile0)
    elif evfile0 not in os.listdir(evpath):
        print('no evfile '+evfile0)
    else:
        kb = pd.read_csv(kbpath + kbfile,header=None)
        
        #kbfiles = [kbfile0,kbfile1,kbfile2]
        #kbli = []
        #for file in kbfiles:
        #    df = pd.read_csv(kbpath + file,header=None, sep='\t')
        #    kbli.append(df)
        #kb = pd.concat(kbli)
        
        symbols = pd.read_csv(advpath + advfile)

        ofiles = [ofile0,ofile1,ofile2]
        oli = []
        for file in ofiles:
            df = pd.read_csv(opath + file)
            oli.append(df)
        orders = pd.concat(oli)

        trfiles = [trfile0,trfile1,trfile2]
        li = []
        for file in trfiles:
            df = pd.read_csv(trpath + file)
            li.append(df)
        tr = pd.concat(li)
        
        evfiles = [evfile0,evfile1,evfile2]
        li=[]
        for file in evfiles:
            df = pd.read_csv(evpath + file,header=None)
            li.append(df)
        ev = pd.concat(li)
        print('files present')
        
        if len(kb.index) < 500:
            print('kbfile incomplete '+date)
        elif len(orders.index) == 0:
            print('ofile incomplete '+date)
        elif len(tr.index) == 0:
            print('trfile incomplete '+date)
        elif len(ev.index) == 0:
            print(len(ev.index))
        else:
            print(len(kb.index))
            ######################Prep Evals File######################
            #Prep message column
            ev['Date'] = ev[0].str.extract('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})',expand=True)
            ev = ev[['Date',0]]
            ev[0] = ev[0].str.replace('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_0 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_1 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_2 )','')
            ev[0] = ev[0].str.replace('INFO|WARN|ERROR','')
            ev['Date']=pd.to_datetime(ev['Date'])
            ev['Date']=ev['Date']+pd.Timedelta(hours=5)
            kb = pd.read_csv(kbpath + kbfile,header=None)
            kb.pop(0)
            kb.rename(columns={1:'Date',2:0},inplace=True)
            kb['Date'] = pd.to_datetime(kb['Date'])
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb['Day_UTC'] = kb['Date'].dt.tz_localize('UTC') + pd.Timedelta(hours=5)
            kb['Day_CT'] = kb['Day_UTC'].dt.tz_convert('US/Central')
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb = pd.concat([kb,ev])
            kb_original = kb
            kb = kb.sort_values('Date').reset_index(drop=True)
            #Create separate dataframes of relevant info and then remove from KB
            capman = kb[kb[0].str.contains('CapMan|BP Change',regex=True)]
            kb = kb.iloc[kb.index.drop(capman.index)]
            kb = kb.reset_index(drop=True)
            evals = kb[kb[0].str.contains('Eval')]
            kb = kb.iloc[kb.index.drop(evals.index)]
            kb = kb.reset_index(drop=True)
            orderlog = kb[kb[0].str.contains('Order-')]
            kb = kb.iloc[kb.index.drop(orderlog.index)]
            kb = kb.reset_index(drop=True)
            attempts = kb[kb[0].str.contains('Attempting|Packet-|flipped|Attempt-')]
            kb = kb.iloc[kb.index.drop(attempts.index)]
            kb = kb.reset_index(drop=True)
            exits = kb[kb[0].str.contains('Exit-|Calc-Uncovered')]
            kb = kb.iloc[kb.index.drop(exits.index)]
            kb = kb.reset_index(drop=True)
            ope = kb[kb[0].str.contains('OPE-')]
            kb = kb.iloc[kb.index.drop(ope.index)]
            kb = kb.reset_index(drop=True)
            closeout = kb[kb[0].str.contains('Closeout-')]
            kb = kb.iloc[kb.index.drop(closeout.index)]
            kb = kb.reset_index(drop=True)
            profit = kb[kb[0].str.contains('Profit-')]
            kb = kb.iloc[kb.index.drop(profit.index)]
            kb = kb.reset_index(drop=True)
            ten_min = kb[kb[0].str.contains('Set-10m-Last-Trade-Price')]
            kb = kb.iloc[kb.index.drop(profit.index)]
            kb = kb.reset_index(drop=True)            
            #kb = kb[~kb[0].str.contains('m price')]
            #Find total capital available for the day
            t = capman[capman[0].str.contains('Successful ')]
            t = t.head(1)
            t  = t.drop_duplicates()
            total_capital = t[0].str.extract('((?<=available:)\S{1,})').squeeze()
            total_capital = total_capital.replace(',','')
            total_capital = int(total_capital)            
            print('Total Capital is: {0}'.format(total_capital))

            #Formatting evals files to breakout columns:
            evals[['Status','SYMBOL','message']] =evals[0].str.split(' ',n=2,expand=True)
            types = ['Eval-Passed','Oppset-Eval-Rescore']
            passed = evals[evals['Status'].isin(types)]
            passed['Auction_Size'] = passed['message'].str.extract('((?<=auction_size:)\S{1,})')
            passed['Threshold'] = passed['message'].str.extract('((?<=threshold:)\S{1,})')
            passed['MQ'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
            passed['ARTI'] = passed['message'].str.extract('((?<=arti:)\S{1,})')
            passed['ARTI'] = passed['ARTI'].astype('float')
            passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
            passed['MQAS'] = passed['message'].str.extract('((?<=mqas:)\S{1,})')
            passed['MQAS_Score'] = passed['message'].str.extract('((?<=mqas_score:)\S{1,})')
            passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
            passed['IADV_Score'] = passed['message'].str.extract('((?<=iadv_score:)\S{1,})')
            passed['PRI'] = passed['message'].str.extract('((?<=pri:)\S{1,})')
            passed['PRI_Score'] = passed['message'].str.extract('((?<=pri_score:)\S{1,})')
            passed['Total_Score'] = passed['message'].str.extract('((?<= score:)\S{1,})')
            passed['Total_Score'] = np.where(passed['Status']=='Eval-Passed',passed['Total_Score'],0)
            cols = ['MQ','ARTI','IADV','MQAS','MQAS_Score','IADV','IADV_Score','PRI','PRI_Score','Total_Score']
            passed[cols] = passed[cols].astype('float').values
            passed.sort_values('Date')
            a1 = (passed['Total_Score']>100)
            a2 = (passed['Total_Score'].between(75,100,inclusive='left'))
            a3 = (passed['Total_Score'].between(50,75,inclusive='left'))
            a4 = (passed['Total_Score'].between(20,50,inclusive='left'))
            vals = [.1,.08,.05,.02]
            default = .02
            passed['Max_Imbalance'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = [.02,.016,.01,.004]
            defaul = .004
            passed['Max_Capital'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = ['A','B','C','D']
            default = 'D'
            passed['Score_Brackets'] = np.select([a1,a2,a3,a4],vals,default=default)
            passed['Desired_Shares_Imbalance'] = passed['Max_Imbalance']*passed['ARTI']
            passed['Desired_Shares_Capital'] = passed['Max_Capital']*total_capital
            passed = passed.set_index('Date')
            passed.index = pd.to_datetime(passed.index)
            MQAS_Score(passed)
            IADV_Score(passed)
            PRI_Score(passed)
            passed = passed.reset_index().drop_duplicates().set_index('Date')
            passed['Day_Type'] = daytype

            
            fails = evals[evals[0].str.contains('Eval-Failed')]
            fails[['Status','SYMBOL','message']] =fails['message'].str.split(' ',n=2,expand=True)
            fails['Failed_Reason'] = fails['message'].str.extract('((?!:)^\D{1,20})')
            fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('-','')
            fails = fails.set_index('Date')
            fails = fails.reset_index().drop_duplicates().set_index('Date')
            fails['Day_Type'] = daytype
            
            #Breakout attempts columns
            attempts['SYMBOL']= attempts[0].str.extract('([A-Z]{2,6})')
            attempts['Desired'] = attempts[0].str.extract('((?<=desired:)\S{1,})')
            attempts['Existing'] = attempts[0].str.extract('((?<=existing:)\S{1,})')
            attempts['Existing'] = attempts['Existing'].str.replace(',','')
            attempts['Net_Desired'] = attempts[0].str.extract('((?<=net_desired:)\S{1,})')
            attempts['Flip_Count'] = attempts[0].str.extract('((?<=flipped )\S{1,})')
            attempts['Packet_Score'] = attempts[0].str.extract('((?<=score:)\S{1,})')
            attempts = attempts.drop_duplicates()
            attempts['Day_Type'] = daytype

            #Breakout attempts columns
            if len(orderlog) == 0:
                pass
            else:
                orderlog[['Status','SYMBOL','message']]=orderlog[0].str.split(' ',n=2,expand=True)
                orderlog['Price_Action'] = orderlog['message'].str.extract('((?<=price-action:)\S{1,})')
                orderlog['Order_Price'] = orderlog['message'].str.extract('((?<=price:)\S{1,})')
                orderlog['Order_Size'] = orderlog['message'].str.extract('((?<=size:)\S{1,})')
                orderlog['Abort_Reason'] = orderlog['message'].str.extract('((?<=reason:)\S{1,})') 
                orderlog = orderlog.drop_duplicates()

            #Breakout exits columns
            exits = exits.reset_index(drop=True)
            exits_params = exits[exits[0].str.contains('Starting')]
            exits = exits.iloc[exits.index.drop(exits_params.index)]
            exits = exits.reset_index(drop=True)
            exits[['Status','SYMBOL','message']]=exits[0].str.split(' ',n=2,expand=True)
            exits['Exit_Type'] = exits['message'].str.extract('((?<= type:)\S{1,})')
            exits['Exit_To_Cover'] = exits['message'].str.extract('((?<=-cover:)\S{1,})')
            exits['Gross_Position'] = exits['message'].str.extract('((?<=gross:)\S{1,})')
            exits['Uncovered_Position'] = exits['message'].str.extract('((?<=uncovered:)\S{1,})')
            exits['Skipped_LO_Size'] = exits['message'].str.extract('((?<=skipped-lo-size:)\S{1,})')
            exits['Unmarketable_Size'] = exits['message'].str.extract('((?<=unmarketable-size:)\S{1,})')
            exits['Unmarketable_on_Opposite_Side'] = exits['message'].str.extract('((?<=unmarketable-on-opposite-side:)\S{1,})')
            exits['Exit_Price'] = exits['message'].str.extract('((?<=price:)\S{1,})')
            exits['Exit_New_Price'] = exits['message'].str.extract('((?<=new-price:)\S{1,})')
            exits['Exit_Price'] = np.where(exits['Status']=='Exit-Repricer',exits['Exit_New_Price'],exits['Exit_Price'])
            exits = exits.sort_values('Date').reset_index(drop=True)
            fill = exits.groupby(['SYMBOL'])['Uncovered_Position'].fillna(method='ffill')
            exits['Uncovered_Position'] = np.where(exits['Exit_Type']=='LO',fill ,exits['Uncovered_Position'])            
            exits['Desired_Exit_Size'] = exits['message'].str.extract('((?<=desired-size:)\S{1,})')
            exits['Net_Exit_Size'] = exits['message'].str.extract('((?<=net-size:)\S{1,})')
            exits['Canceled_Exit_Size'] = exits['message'].str.extract('((?<=canceled-size:)\S{1,})')
            exits['Exit_Type'] = np.where(exits['Status']=='Exit-Replace-LO','LO',exits['Exit_Type'])
            exits['Exit_Type'] = np.where(exits['Status']=='Exit-Repricer','LO',exits['Exit_Type'])
            exits['Bid'] = exits['message'].str.extract('((?<=bid:)\S{1,})')
            exits['Ask'] = exits['message'].str.extract('((?<=ask:)\S{1,})')
            exits['Avg_Price'] = exits['message'].str.extract('((?<=avg-price:)\S{1,})')
            exits['ICP'] = exits['message'].str.extract('((?<=icp:)\S{1,})')
            exits['10minRef'] = exits['message'].str.extract('((?<=10minRef:)\S{1,})')
            exits['5minRef'] = exits['message'].str.extract('((?<=5minRef:)\S{1,})')
            num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
            exits[num_cols] = exits[num_cols].astype('float')
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LOC')
            exits['LOC_Ref_Boundary'] = np.where(condition==True,exits[['10minRef','5minRef']].min(axis=1),exits[['10minRef','5minRef']].max(axis=1))
            exits['LOC_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
            exits['LOC_Price_Ideal'] = np.where(condition==True,exits[['LOC_Cross_Price','Avg_Price']].min(axis=1),exits[['LOC_Cross_Price','Avg_Price']].max(axis=1))
            exits['LOC_Price_Calc'] = np.where(condition==True,exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].max(axis=1),exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].min(axis=1))
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='IO')            
            exits['IO_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
            exits['IO_Price_Calc'] = np.where(condition==True,exits[['IO_Cross_Price','Avg_Price']].min(axis=1),exits[['IO_Cross_Price','Avg_Price']].max(axis=1))
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LO')         
            exits['Midpoint_ICP_NBBO'] = np.where(condition==True,(exits['Ask']+exits['ICP'])/2,(exits['Bid']+exits['ICP'])/2)
            exits['Midpoint_ICP_AvgPrice'] = (exits['Avg_Price']+exits['ICP'])/2
            exits['LO_Price_Calc'] = np.where(condition==True,exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].min(axis=1),exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].max(axis=1))
            exits['LO_Price_Calc'] = round(exits['LO_Price_Calc'],2)
            num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
            exits[num_cols] = exits[num_cols].astype('float')
            exits = exits.sort_values('Date')
            exits = exits.drop_duplicates()
            exits['Day_Type'] = daytype
            
            #####Breakout profit taker columns:
            if len(profit.index)==0:
                print('profit file empty')
            else:
                profit[['Status','SYMBOL','message']] = profit[0].str.split(' ',n=2,expand=True)
                profit['Price'] = profit['message'].str.extract('((?<= price:)\S{1,})').astype('float')
                profit['Bid'] = profit['message'].str.extract('((?<=bid:)\S{1,})').astype('float')
                profit['Ask'] = profit['message'].str.extract('((?<=ask:)\S{1,})').astype('float')
                profit['Uncovered_Position'] = profit['message'].str.extract('((?<=uncovered:)\S{1,})').astype('float')
                profit['10min_Last_Trade'] = profit['message'].str.extract('((?<=ten-min-last-trade:)\S{1,})').astype('float')
                profit['Price_Action_Multiple'] = profit['message'].str.extract('((?<=pa-mult:)\S{1,})').astype('float')
                profit['Side'] = profit['message'].str.extract('((?<=side:)\S{1,})')
                profit['Size'] = profit['message'].str.extract('((?<= size:)\S{1,})')
                profit = profit.sort_values(['SYMBOL','Date']).reset_index(drop=True)
                profit['Day_Type'] = daytype

            #Breakout ope columns
            if len(ope.index)==0:
                print('ope file empty')
            else:
                ope[['Status','SYMBOL','message']]=ope[0].str.split(' ',n=2,expand=True)
                ope['Uncovered_Position'] = ope['message'].str.extract('((?<=uncovered:)\S{1,})')
                ope['OPE_Price'] = ope['message'].str.extract('((?<= price:)\S{1,})')
                ope['Bid'] = ope['message'].str.extract('((?<=bid:)\S{1,})')
                ope['Ask'] = ope['message'].str.extract('((?<=ask:)\S{1,})')
                ope['10min_Ref'] = ope['message'].str.extract('((?<=10min-ref:)\S{1,})')
                ope['Last_Ref'] = ope['message'].str.extract('((?<=last-ref:)\S{1,})')
                ope['ICP'] = ope['message'].str.extract('((?<=icp:)\S{1,})')
                ope['Opportunity_Cost'] = ope['message'].str.extract('((?<=opportunity-cost:)\S{1,})')
                ope['Side'] = ope['message'].str.extract('((?<= action:)\S{1,})')
                ope['PA_Prepacket'] = ope['message'].str.extract('((?<=pa-prepacket:)\S{1,})')
                ope['FF_Act_On_Packet'] = np.where(ope['PA_Prepacket'].notna(),1,0)
                ope['Cross_Percent'] = ope['message'].str.extract('((?<= cross-pct:)\S{1,})')
                ope['Price_Action'] = ope['message'].str.extract('((?<= price-action:)\S{1,})')
                
                ope = ope.drop_duplicates()
                
            closeout[['Status','SYMBOL','message']]=closeout[0].str.split(' ',n=2,expand=True)
            closeout['Uncovered_Position'] = closeout['message'].str.extract('((?<=uncovered:)\S{1,})')
            closeout['Bid'] = closeout['message'].str.extract('((?<=bid:)\S{1,})')
            closeout['Ask'] = closeout['message'].str.extract('((?<=ask:)\S{1,})')
            closeout['ICP'] = closeout['message'].str.extract('((?<=icp:)\S{1,})')
            closeout['Closeout_Type'] = closeout['message'].str.extract('((?<=strategy:)\S{1,})')
            closeout['Closeout_Price'] = closeout['message'].str.extract('((?<=new-price:)\S{1,})')
            closeout = closeout[~closeout[0].str.contains('Start')]
            closeout = closeout.drop_duplicates()
            closeout['Day_Type'] = daytype
            
            ######################
            #Read in ADV files
            adv, nsdq_symbols = get_symbolslist(date)

            ######################
            #Read in Orders Report
            orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
            orders['EntryTime_ms'] = orders['EntryTime'].dt.microsecond
            orders['LastModTime'] = pd.to_datetime(orders['LastModTime'])
            orders = orders.groupby(['OrderId']).tail(1)
            orders.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            orders['QTY_Abs'] = np.absolute(orders['Quantity'])
            orders['Desired_$'] = orders.QTY_Abs * orders.Price
            orders['FilledQTY_Abs'] = np.absolute(orders['FilledQty'])
            orders['Filled$_Abs'] = orders['FilledQTY_Abs']*orders['AvgFillPrice']
            orders['Filled$'] = orders['FilledQty']*orders['AvgFillPrice']            
            orders['Fill_Rate'] = orders['FilledQTY_Abs']/orders['QTY_Abs']
            orders['DATE'] = orders['EntryTime'].dt.date
            orders['Capital_Available'] = total_capital
            #Identifying Exit orders on Orders Report 
            exitid = exits[exits['Exit_Type'].notna()]
            exitid = exitid[['SYMBOL','Exit_Type','Date']]
            exitid = exitid.sort_values('Date')
            exitid['TIF'] = np.where(exitid['Exit_Type']=='LO','DAY','AT_THE_CLOSE')
            exitid.rename(columns={'Exit_Type':'EntryType','Date':'EntryTime'},inplace=True)
            orders = orders.sort_values('EntryTime')
            orders = pd.merge_asof(orders, exitid, by=['SYMBOL','TIF'],left_on=['EntryTime'], right_on=['EntryTime'],direction='nearest',tolerance=pd.Timedelta("100ms"))     
            #Identifying Exit orders on Orders Report 
            #evalid = passed.reset_index()
            #evalid = evalid[['Date','SYMBOL','Status','Score_Brackets']]
            #evalid = evalid.sort_values('Date').set_index('Date').reset_index()
            #For merging eval message onto orders report to identify entry orders
            #orders = pd.merge_asof(orders, evalid, by=['SYMBOL'],left_on=['EntryTime'], right_on=['Date'],direction='backward',tolerance=pd.Timedelta("900ms"))
            #orders['Order_ms'] = np.where(orders['EntryTime_ms']==500000,'Eval-Passed',np.nan)
            orders['EntryType'] = np.where(orders['TIF']=='IOC','OPE',orders['EntryType'])
            orders['EntryType'] = np.where(orders['EntryType'].isna(),'Entry',orders['EntryType'])
            orders['TIF_in_seconds'] = (orders['LastModTime'] - orders['EntryTime']).dt.total_seconds()
            orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
            orders['Time_Diff'] = (orders['EntryTime'] - orders.groupby(['SYMBOL','Long_Short'])['EntryTime'].shift(1)).dt.total_seconds().fillna(100)
            orders['Time_Diff_Bool'] = orders['Time_Diff']>15
            orders['Unique'] = orders.groupby(['SYMBOL'])['Time_Diff_Bool'].cumsum()
            orders['QTY_Abs_Agg'] = orders.groupby(['SYMBOL','Long_Short','EntryTime'])[['QTY_Abs']].transform('sum')
            orders['QTY_Max_Desired'] = orders.groupby(['SYMBOL','Long_Short','Unique'])[['QTY_Abs_Agg']].transform('max')
            orders['$_Max_Desired'] = orders['QTY_Max_Desired'] * orders['Price']
            x = orders.groupby(['SYMBOL','Unique'])[['SYMBOL','Unique','Price','QTY_Max_Desired','$_Max_Desired']].head(1)
            x.rename(columns={'Price':'Start_Price'},inplace=True)
            orders.drop(columns=['QTY_Max_Desired','$_Max_Desired'],inplace=True)
            orders = pd.merge(orders,x,how='left')
            a1 = (orders['Start_Price']<5)
            a2 = (orders['Start_Price'].between(5,10,inclusive='left'))
            a3 = (orders['Start_Price'].between(10,15,inclusive='left'))
            a4 = (orders['Start_Price'].between(15,30,inclusive='left'))
            a5 = (orders['Start_Price'].between(30,60,inclusive='left'))
            a6 = (orders['Start_Price']>=60)
            vals = [0,.01,.02,.03,.04,.05]
            default = 0
            orders['Cross_Amount'] = np.select([a1,a2,a3,a4,a5,a6],vals,default)
            orders['Start_NBBO'] = np.where(orders['Quantity']>0,orders['Start_Price']-orders['Cross_Amount'],orders['Start_Price']+orders['Cross_Amount'])
            orders['Start_NBBO'] = np.where(orders['AvgFillPrice']==0,np.nan,orders['Start_NBBO'])
            orders['Day_Type'] = daytype

            #Confirm no lines with unassigned EntryType
            checkx = orders[orders['EntryType']=='X']
            print('Number of unassigned order EntryTypes is {0}'.format(len(checkx))) 
            

            ######################
            #Read in Trade Reports
            tr.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            tr['EntryTime'] = pd.to_datetime(tr['EntryTime'])
            tr = tr.sort_values('EntryTime')
            tr = tr[tr['EntryTime']>date +' '+hr+':50:00']
            tr['DATE'] = tr['EntryTime'].dt.date
            tr['Day_of_Week'] = tr['EntryTime'].dt.dayofweek
            tr['Day_Type'] = daytype
            #Create slice of orders to only look at entries
            entries = orders[orders['EntryType']=='Entry']
            #deployed_by_symbol = entries[['SYMBOL','FilledQty','AvgFillPrice','LastModTime','EntryType','Filled$_Abs','FilledQTY_Abs','QTY_Abs','Capital_Available','Desired_$']].sort_values('LastModTime')
            #deployed_by_symbol.rename(columns={'FilledQty':'Quantity'},inplace=True)
            #tr = pd.merge_asof(tr,deployed_by_symbol,by=['SYMBOL','Quantity'],left_on=['EntryTime'],right_on=['LastModTime'],direction='nearest')
            tr['FilledQTY_Abs'] = np.absolute(tr['Quantity'])
            tr['Filled$_Abs'] = tr['FilledQTY_Abs'] * tr['EntryPrice']
            tr['Stuck'] = np.where(tr['ExitTime']=='1970-Jan-01 00:00:00','Stuck','Exited')
            cols = ['Quantity','RealizedPL','Filled$_Abs']
            tr[cols] = tr[cols].astype('float')
            trinst = tr.copy()
            x = tr.groupby(['SYMBOL'])[['Quantity','RealizedPL','Filled$_Abs']].sum().reset_index()
            tr.drop(columns=['Quantity','RealizedPL','Filled$_Abs'],inplace=True)
            tr = pd.merge(tr,x,how='left')
            tr.drop_duplicates(['SYMBOL'],inplace=True)
            tr['Basis_Return'] = tr['RealizedPL']/tr['Filled$_Abs']
            #Creating Trade Reports by 
            trinst['Basis_Return'] = trinst['RealizedPL']/trinst['Filled$_Abs']
            fortrinst = passed[['SYMBOL','MQ','ARTI','IADV','MQAS','MQAS_Score','IADV_Score','PRI','PRI_Score','Total_Score']].reset_index()
            fortrinst['Date'] = fortrinst['Date'].dt.tz_localize(None)
            fortrinst = fortrinst.sort_values(['SYMBOL','Date'])
            fortrinst['Total_Score'] = fortrinst['Total_Score'].replace(0,np.nan)
            fortrinst = fortrinst.fillna(method='bfill')
            fortrinst = fortrinst.sort_values('Date')
            trinst = pd.merge_asof(trinst,fortrinst,by=['SYMBOL'],left_on='EntryTime',right_on='Date',direction='nearest')
            trinst['ADV'] = trinst['ARTI'] / trinst['IADV']
            a1 = (trinst['Total_Score']>100)
            a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
            a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
            a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            trinst['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)

            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            trinst['ADV_Even_Brackets'] = pd.cut(trinst['ADV'], bins=cuts,duplicates='drop')
            trinst['MQ_Brackets'] = pd.cut(trinst['MQ'], bins=cuts,duplicates='drop')
            trinst['Imb_Brackets'] = pd.cut(trinst['ARTI'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0.00001,.1,.2,.3,.4,.5,.6,.7,.8,.9,1,2,5,np.inf]
            trinst['IADV_Even_Extended_Brackets'] = pd.cut(trinst['IADV'],bins=cuts)
            cuts = [-np.inf,0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]
            trinst['MQAS_Brackets'] = pd.cut(trinst['MQAS'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0,5,10,15,30,60,np.inf]
            trinst['Price_Brackets'] = pd.cut(trinst['EntryPrice'], bins=cuts)
            a1 = (trinst['EntryTime']>=(date + ' ' +hr + ':00')) & (trinst['EntryTime']<(date + ' ' +hr + ':55'))
            a2 = (trinst['EntryTime']>=(date + ' ' +hr + ':55')) & (trinst['EntryTime']<(date + ' ' +hr + ':58'))
            a3 = (trinst['EntryTime']>=(date + ' ' +hr + ':58'))
            vals = ['Entry50', 'Entry55', 'Entry58']
            trinst['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
            
            orders = pd.merge_asof(orders,fortrinst,by=['SYMBOL'], left_on='EntryTime', right_on='Date',direction='nearest')
            a1 = (orders['Total_Score']>100)
            a2 = (orders['Total_Score'].between(75,100,inclusive='left'))
            a3 = (orders['Total_Score'].between(50,75,inclusive='left'))
            a4 = (orders['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            orders['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)
            orders['ADV'] = orders['ARTI'] / orders['IADV']
            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            orders['ADV_Even_Brackets'] = pd.cut(orders['ADV'], bins=cuts,duplicates='drop')
            #Adding EntryTiming
            a1 = (orders['EntryTime']>=(date + ' ' +hr + ':00')) & (orders['EntryTime']<(date + ' ' +hr + ':55'))
            a2 = (orders['EntryTime']>=(date + ' ' +hr + ':55')) & (orders['EntryTime']<(date + ' ' +hr + ':58'))
            a3 = (orders['EntryTime']>=(date + ' ' +hr + ':58'))
            vals = ['Entry50', 'Entry55', 'Entry58']
            orders['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
            orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
            #Adding columns for price slippage

            orders['Slippage_$'] = np.where(orders['Quantity']>0,orders['Start_NBBO']-orders['AvgFillPrice'],orders['AvgFillPrice']-orders['Start_NBBO'])
            orders['Slipage_%'] = orders['Slippage_$']/orders['Start_NBBO']
            orders['Weighted_Price'] = orders['$_Max_Desired']*orders['Start_NBBO']
            orders['Slippage_$$, $Weighted'] = orders['$_Max_Desired']*orders['Slippage_$']
            orders['Weighted_Shares'] = orders['QTY_Max_Desired']*orders['Start_NBBO']
            orders['Slippage_$,Shares Weighted'] = orders['QTY_Max_Desired']*orders['Slippage_$']
            orders['Percent_of_Imbalance'] = orders['FilledQTY_Abs']/orders['ARTI']
            orders['Percent_of_Capital'] = orders['Filled$_Abs']/orders['Capital_Available']
            orders['on_packet'] = orders['EntryTime_ms']!=500000


            stuckpnlfile = date + ' suckpnl.csv'
            if stuckpnlfile in os.listdir('.'):
                stuckpnl = pd.read_csv(date +' stuckpnl.csv')
                stuckpnl.rename(columns={'Symbol':'SYMBOL'},inplace=True)
                trinst['Percent_of_Total'] = trinst.groupby('SYMBOL')['Quantity'].apply(lambda column: column/column.sum())
                trinst = pd.merge(trinst,stuckpnl,how='left')
                trinst['Net'] = trinst['Net'].fillna(0)
                trinst['StuckPNL'] = trinst['Percent_of_Total']*trinst['Net']
                trinst['Adj_RealizedPL'] = trinst['RealizedPL'] + trinst['StuckPNL']
            else:
                print('no stuckpnl file')

            #Combining attemps-df and orderlog-df
            combo = pd.merge(attempts, orderlog,how='outer')
            combo = combo.sort_values('Date')
            alllogs = pd.concat([orderlog, attempts, exits,ope,closeout,passed.reset_index()],keys=['orderlog', 'attempts', 'exits','ope','closeout','passed']).reset_index()
            
            alllogs.to_csv(writepath + date + ' alllogs.csv')
            tr.to_csv(writepath + date + ' tradereports.csv')
            trinst.to_csv(writepath + date + ' tr_byinstance.csv')
            orders.to_csv(writepath + date +' orders.csv')
            combo.to_csv(writepath + date +' orderslog.csv')
            exits.to_csv(writepath + date +' exits.csv')
            ope.to_csv(writepath + date +' ope.csv')
            closeout.to_csv(writepath + date +' closeout.csv')
            passed.to_csv(writepath + date +' passed.csv')
            capman.to_csv(writepath + date +' capman.csv')
            kb.to_csv(writepath + date +' kb.csv')
            profit.to_csv(writepath + date + ' profit.csv')
            ten_min.to_csv(writepath + date + ' ten_min.csv')
            entries = orders[orders['EntryType']=='Entry']            
            ope = orders[orders['EntryType']=='OPE']


# In[985]:


hr = '19'
filedate = ''
LIVE_SIM_id = ''
def create_reports_220228(date, hr, str_month):
    total_capital=None
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    pathname = date
    ######################Read Files in######################
    print(date)
    #kbpath = 'Z:/NQC/nqc-live-sim/'+pathname+'/logs/'
    kbpath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+ date + '/'
    #kbfile0 = 'processor0.log'    
    #kbfile1 = 'processor1.log'
    #kbfile2 = 'processor2.log' 
    kbfile = date +' processorlogNSDQClose_prod.csv'
    #kbpath = '.'
    #advfile = '210916 NSDQ ADV.csv'
    advfile = date+'_master-symbol-list.csv'
    #advpath = 'Z:/NASDAQ/NSDQ 0.1/'
    advpath = 'Z:/web-guys/memo/symbols/'
    opath = 'Z:/web-guys/data/ss-reports/'
    #opath = 'Z:/capman/2022-02-14_capman-overhaul/ss-reports/'
    ofile0 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_0_'+date+'_order.csv'
    ofile1 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_1_'+date+'_order.csv'
    ofile2 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_2_'+date+'_order.csv'    
    trpath = 'Z:/web-guys/data/ss-reports/'
    #trpath = 'Z:/capman/2022-02-14_capman-overhaul/ss-reports/'
    trfile0 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_0_'+date+'_trade_reports.csv'
    trfile1 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_1_'+date+'_trade_reports.csv'
    trfile2 = 'nsdq-03.dc.gptbs.com_NsdqImbalanceToClose_2_'+date+'_trade_reports.csv'
    evpath = 'Z:/web-guys/data/custom_logs/nsdq-03.dc.gptbs.com/NsdqImbalanceToClose/'
    datestr = date.replace('-','')
    evfile0 = 'NsdqImbalanceToClose_0_evals_'+datestr
    evfile1 = 'NsdqImbalanceToClose_1_evals_'+datestr
    evfile2 = 'NsdqImbalanceToClose_2_evals_'+datestr   
    daytype = days[days['Date']==date]
    daytype = daytype['Type'].squeeze()
    if kbfile not in os.listdir(kbpath):
        print('no kbfile '+kbfile)
    elif advfile not in os.listdir(advpath):
        print('no advfile '+advfile)
    elif ofile0 not in os.listdir(opath):
        print('no ofile '+ofile0)
    elif trfile0 not in os.listdir(trpath):
        print('no trfile '+trfile0)
    elif evfile0 not in os.listdir(evpath):
        print('no evfile '+evfile0)
    else:
        kb = pd.read_csv(kbpath + kbfile,header=None)
        
        #kbfiles = [kbfile0,kbfile1,kbfile2]
        #kbli = []
        #for file in kbfiles:
        #    df = pd.read_csv(kbpath + file,header=None, sep='\t')
        #    kbli.append(df)
        #kb = pd.concat(kbli)
        
        symbols = pd.read_csv(advpath + advfile)

        ofiles = [ofile0,ofile1,ofile2]
        oli = []
        for file in ofiles:
            df = pd.read_csv(opath + file)
            oli.append(df)
        orders = pd.concat(oli)

        trfiles = [trfile0,trfile1,trfile2]
        li = []
        for file in trfiles:
            df = pd.read_csv(trpath + file)
            li.append(df)
        tr = pd.concat(li)
        
        evfiles = [evfile0,evfile1,evfile2]
        li=[]
        for file in evfiles:
            df = pd.read_csv(evpath + file,header=None)
            li.append(df)
        ev = pd.concat(li)
        print('files present')
        
        if len(kb.index) < 500:
            print('kbfile incomplete '+date)
        elif len(orders.index) == 0:
            print('ofile incomplete '+date)
        elif len(tr.index) == 0:
            print('trfile incomplete '+date)
        elif len(ev.index) == 0:
            print(len(ev.index))
        else:
            print(len(kb.index))
            ######################Prep Evals File######################
            #Prep message column
            ev['Date'] = ev[0].str.extract('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})',expand=True)
            ev = ev[['Date',0]]
            ev[0] = ev[0].str.replace('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_0 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_1 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_2 )','')
            ev[0] = ev[0].str.replace('INFO|WARN|ERROR','')
            ev['Date']=pd.to_datetime(ev['Date'])
            ev['Date']=ev['Date']+pd.Timedelta(hours=5)
            kb = pd.read_csv(kbpath + kbfile,header=None)
            kb.pop(0)
            kb.rename(columns={1:'Date',2:0},inplace=True)
            kb['Date'] = pd.to_datetime(kb['Date'])
            #kb['Day_UTC'] = kb['Date'].dt.tz_localize('UTC') + pd.Timedelta(hours=5)
            #kb['Day_CT'] = kb['Day_UTC'].dt.tz_convert('US/Central')
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb = pd.concat([kb,ev])
            kb_original = kb
            #Create separate dataframes of relevant info and then remove from KB
            capman = kb[kb[0].str.contains('CapMan|BP Change',regex=True)]
            kb = kb.iloc[kb.index.drop(capman.index)]
            kb = kb.reset_index(drop=True)
            evals = kb[kb[0].str.contains('Eval')]
            kb = kb.iloc[kb.index.drop(evals.index)]
            kb = kb.reset_index(drop=True)
            orderlog = kb[kb[0].str.contains('Order-')]
            kb = kb.iloc[kb.index.drop(orderlog.index)]
            kb = kb.reset_index(drop=True)
            attempts = kb[kb[0].str.contains('Attempting|Packet-|flipped|Attempt-')]
            kb = kb.iloc[kb.index.drop(attempts.index)]
            kb = kb.reset_index(drop=True)
            exits = kb[kb[0].str.contains('Exit-|Calc-Uncovered')]
            kb = kb.iloc[kb.index.drop(exits.index)]
            kb = kb.reset_index(drop=True)
            ope = kb[kb[0].str.contains('OPE-')]
            kb = kb.iloc[kb.index.drop(ope.index)]
            kb = kb.reset_index(drop=True)
            closeout = kb[kb[0].str.contains('Closeout-')]
            kb = kb.iloc[kb.index.drop(closeout.index)]
            kb = kb.reset_index(drop=True)
            kb = kb[~kb[0].str.contains('m price')]
            #Find total capital available for the day
            t = capman[capman[0].str.contains('Successful ')]
            t = t.head(1)
            t  = t.drop_duplicates()
            total_capital = t[0].str.extract('((?<=available:)\S{1,})').squeeze()
            total_capital = total_capital.replace(',','')
            total_capital = int(total_capital)            
            print('Total Capital is: {0}'.format(total_capital))

            #Formatting evals files to breakout columns:
            evals[['Status','SYMBOL','message']] =evals[0].str.split(' ',n=2,expand=True)
            types = ['Eval-Passed','Oppset-Eval-Rescore']
            passed = evals[evals['Status'].isin(types)]
            passed['Auction_Size'] = passed['message'].str.extract('((?<=auction_size:)\S{1,})')
            passed['Threshold'] = passed['message'].str.extract('((?<=threshold:)\S{1,})')
            passed['MQ'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
            passed['ARTI'] = passed['message'].str.extract('((?<=arti:)\S{1,})')
            passed['ARTI'] = passed['ARTI'].astype('float')
            passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
            passed['MQAS'] = passed['message'].str.extract('((?<=mqas:)\S{1,})')
            passed['MQAS_Score'] = passed['message'].str.extract('((?<=mqas_score:)\S{1,})')
            passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
            passed['IADV_Score'] = passed['message'].str.extract('((?<=iadv_score:)\S{1,})')
            passed['PRI'] = passed['message'].str.extract('((?<=pri:)\S{1,})')
            passed['PRI_Score'] = passed['message'].str.extract('((?<=pri_score:)\S{1,})')
            passed['Total_Score'] = passed['message'].str.extract('((?<= score:)\S{1,})')
            passed['Total_Score'] = np.where(passed['Status']=='Eval-Passed',passed['Total_Score'],0)
            cols = ['MQ','ARTI','IADV','MQAS','MQAS_Score','IADV','IADV_Score','PRI','PRI_Score','Total_Score']
            passed[cols] = passed[cols].astype('float').values
            passed.sort_values('Date')
            a1 = (passed['Total_Score']>100)
            a2 = (passed['Total_Score'].between(75,100,inclusive='left'))
            a3 = (passed['Total_Score'].between(50,75,inclusive='left'))
            a4 = (passed['Total_Score'].between(20,50,inclusive='left'))
            vals = [.1,.08,.05,.02]
            default = .02
            passed['Max_Imbalance'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = [.02,.016,.01,.004]
            defaul = .004
            passed['Max_Capital'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = ['A','B','C','D']
            default = 'D'
            passed['Score_Brackets'] = np.select([a1,a2,a3,a4],vals,default=default)
            passed['Desired_Shares_Imbalance'] = passed['Max_Imbalance']*passed['ARTI']
            passed['Desired_Shares_Capital'] = passed['Max_Capital']*total_capital
            passed = passed.set_index('Date')
            passed.index = pd.to_datetime(passed.index)
            MQAS_Score(passed)
            IADV_Score(passed)
            PRI_Score(passed)
            passed = passed.reset_index().drop_duplicates().set_index('Date')
            passed['Day_Type'] = daytype

            
            fails = evals[evals[0].str.contains('Eval-Failed')]
            fails[['Status','SYMBOL','message']] =fails['message'].str.split(' ',n=2,expand=True)
            fails['Failed_Reason'] = fails['message'].str.extract('((?!:)^\D{1,20})')
            fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('-','')
            fails = fails.set_index('Date')
            fails = fails.reset_index().drop_duplicates().set_index('Date')
            fails['Day_Type'] = daytype



            print('evals done')

            #Breakout attempts columns
            attempts['SYMBOL']= attempts[0].str.extract('([A-Z]{2,6})')
            attempts['Desired'] = attempts[0].str.extract('((?<=desired:)\S{1,})')
            attempts['Existing'] = attempts[0].str.extract('((?<=existing:)\S{1,})')
            attempts['Existing'] = attempts['Existing'].str.replace(',','')
            attempts['Net_Desired'] = attempts[0].str.extract('((?<=net_desired:)\S{1,})')
            attempts['Flip_Count'] = attempts[0].str.extract('((?<=flipped )\S{1,})')
            attempts['Packet_Score'] = attempts[0].str.extract('((?<=score:)\S{1,})')
            attempts = attempts.drop_duplicates()
            attempts['Day_Type'] = daytype

            #Breakout attempts columns
            if len(orderlog) == 0:
                pass
            else:
                orderlog[['Status','SYMBOL','message']]=orderlog[0].str.split(' ',n=2,expand=True)
                orderlog['Price_Action'] = orderlog['message'].str.extract('((?<=price-action:)\S{1,})')
                orderlog['Order_Price'] = orderlog['message'].str.extract('((?<=price:)\S{1,})')
                orderlog['Order_Size'] = orderlog['message'].str.extract('((?<=size:)\S{1,})')
                orderlog['Abort_Reason'] = orderlog['message'].str.extract('((?<=reason:)\S{1,})') 
                orderlog = orderlog.drop_duplicates()



            #Breakout exits columns
            print(len(exits.index))
            exits = exits.reset_index(drop=True)
            exits_params = exits[exits[0].str.contains('Starting')]
            exits = exits.iloc[exits.index.drop(exits_params.index)]
            exits = exits.reset_index(drop=True)
            exits[['Status','SYMBOL','message']]=exits[0].str.split(' ',n=2,expand=True)
            exits['Exit_Type'] = exits['message'].str.extract('((?<= type:)\S{1,})')
            exits['Exit_To_Cover'] = exits['message'].str.extract('((?<=-cover:)\S{1,})')
            exits['Gross_Position'] = exits['message'].str.extract('((?<=gross:)\S{1,})')
            exits['Uncovered_Position'] = exits['message'].str.extract('((?<=uncovered:)\S{1,})')
            exits['Skipped_LO_Size'] = exits['message'].str.extract('((?<=skipped-lo-size:)\S{1,})')
            exits['Unmarketable_Size'] = exits['message'].str.extract('((?<=unmarketable-size:)\S{1,})')
            exits['Unmarketable_on_Opposite_Side'] = exits['message'].str.extract('((?<=unmarketable-on-opposite-side:)\S{1,})')
            exits['Exit_Price'] = exits['message'].str.extract('((?<=price:)\S{1,})')
            exits['Exit_New_Price'] = exits['message'].str.extract('((?<=new-price:)\S{1,})')
            exits['Exit_Price'] = np.where(exits['Status']=='Exit-Repricer',exits['Exit_New_Price'],exits['Exit_Price'])
            exits = exits.sort_values('Date').reset_index(drop=True)
            fill = exits.groupby(['SYMBOL'])['Uncovered_Position'].fillna(method='ffill')
            exits['Uncovered_Position'] = np.where(exits['Exit_Type']=='LO',fill ,exits['Uncovered_Position'])            
            exits['Desired_Exit_Size'] = exits['message'].str.extract('((?<=desired-size:)\S{1,})')
            exits['Net_Exit_Size'] = exits['message'].str.extract('((?<=net-size:)\S{1,})')
            exits['Canceled_Exit_Size'] = exits['message'].str.extract('((?<=canceled-size:)\S{1,})')
            exits['Exit_Type'] = np.where(exits['Status']=='Exit-Replace-LO','LO',exits['Exit_Type'])
            exits['Exit_Type'] = np.where(exits['Status']=='Exit-Repricer','LO',exits['Exit_Type'])
            exits['Bid'] = exits['message'].str.extract('((?<=bid:)\S{1,})')
            exits['Ask'] = exits['message'].str.extract('((?<=ask:)\S{1,})')
            exits['Avg_Price'] = exits['message'].str.extract('((?<=avg-price:)\S{1,})')
            exits['ICP'] = exits['message'].str.extract('((?<=icp:)\S{1,})')
            exits['10minRef'] = exits['message'].str.extract('((?<=10minRef:)\S{1,})')
            exits['5minRef'] = exits['message'].str.extract('((?<=5minRef:)\S{1,})')
            num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
            exits[num_cols] = exits[num_cols].astype('float')
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LOC')
            exits['LOC_Ref_Boundary'] = np.where(condition==True,exits[['10minRef','5minRef']].min(axis=1),exits[['10minRef','5minRef']].max(axis=1))
            exits['LOC_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
            exits['LOC_Price_Ideal'] = np.where(condition==True,exits[['LOC_Cross_Price','Avg_Price']].min(axis=1),exits[['LOC_Cross_Price','Avg_Price']].max(axis=1))
            exits['LOC_Price_Calc'] = np.where(condition==True,exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].max(axis=1),exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].min(axis=1))
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='IO')            
            exits['IO_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
            exits['IO_Price_Calc'] = np.where(condition==True,exits[['IO_Cross_Price','Avg_Price']].min(axis=1),exits[['IO_Cross_Price','Avg_Price']].max(axis=1))
            condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LO')         
            exits['Midpoint_ICP_NBBO'] = np.where(condition==True,(exits['Ask']+exits['ICP'])/2,(exits['Bid']+exits['ICP'])/2)
            exits['Midpoint_ICP_AvgPrice'] = (exits['Avg_Price']+exits['ICP'])/2
            exits['LO_Price_Calc'] = np.where(condition==True,exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].min(axis=1),exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].max(axis=1))
            exits['LO_Price_Calc'] = round(exits['LO_Price_Calc'],2)
            num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
            exits[num_cols] = exits[num_cols].astype('float')
            exits = exits.sort_values('Date')
            exits = exits.drop_duplicates()
            exits['Day_Type'] = daytype

            #Breakout ope columns
            if len(ope.index)==0:
                pass
            else:
                print('start ope')
                print(len(ope.index))
                ope[['Status','SYMBOL','message']]=ope[0].str.split(' ',n=2,expand=True)
                ope['Uncovered_Position'] = ope['message'].str.extract('((?<=uncovered:)\S{1,})')
                ope['OPE_Price'] = ope['message'].str.extract('((?<=price:)\S{1,})')
                ope['Bid'] = ope['message'].str.extract('((?<=bid:)\S{1,})')
                ope['Ask'] = ope['message'].str.extract('((?<=ask:)\S{1,})')
                ope['10min_Ref'] = ope['message'].str.extract('((?<=10min-ref:)\S{1,})')
                ope['Last_Ref'] = ope['message'].str.extract('((?<=last-ref:)\S{1,})')
                ope['ICP'] = ope['message'].str.extract('((?<=icp:)\S{1,})')
                ope['Opportunity_Cost'] = ope['message'].str.extract('((?<=opportunity-cost:)\S{1,})')
                ope = ope.drop_duplicates()
                print(len(ope.index))
                print('end ope')
                
            closeout[['Status','SYMBOL','message']]=closeout[0].str.split(' ',n=2,expand=True)
            closeout['Uncovered_Position'] = closeout['message'].str.extract('((?<=uncovered:)\S{1,})')
            closeout['Bid'] = closeout['message'].str.extract('((?<=bid:)\S{1,})')
            closeout['Ask'] = closeout['message'].str.extract('((?<=ask:)\S{1,})')
            closeout['ICP'] = closeout['message'].str.extract('((?<=icp:)\S{1,})')
            closeout['Closeout_Type'] = closeout['message'].str.extract('((?<=strategy:)\S{1,})')
            closeout['Closeout_Price'] = closeout['message'].str.extract('((?<=new-price:)\S{1,})')
            closeout = closeout[~closeout[0].str.contains('Start')]
            closeout = closeout.drop_duplicates()
            closeout['Day_Type'] = daytype
            
            ######################
            #Read in ADV files
            symbols.columns=['CUSIP','SYMBOL','Type','Exchange','Desc','ETB','ADV']
            nsdq = symbols[symbols['Exchange']=='R']
            nsdq['ETB'] = np.where(nsdq['ETB']=='ETB','ETB','H')
            #nsdq = nsdq[nsdq['ETB']=='ETB']
            nsdq['ADV'] = nsdq['ADV'].astype('int')
            adv = nsdq[['SYMBOL','ADV','ETB']]
            adv.rename(columns={'SYMBOL':'symbol'},inplace=True)

            ######################
            #Read in Orders Report
            orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
            orders['EntryTime_ms'] = orders['EntryTime'].dt.microsecond
            orders['LastModTime'] = pd.to_datetime(orders['LastModTime'])
            orders = orders.groupby(['OrderId']).tail(1)
            orders.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            orders['QTY_Abs'] = np.absolute(orders['Quantity'])
            orders['Desired_$'] = orders.QTY_Abs * orders.Price
            orders['FilledQTY_Abs'] = np.absolute(orders['FilledQty'])
            orders['Filled$_Abs'] = orders['FilledQTY_Abs']*orders['AvgFillPrice']
            orders['Filled$'] = orders['FilledQty']*orders['AvgFillPrice']            
            orders['Fill_Rate'] = orders['FilledQTY_Abs']/orders['QTY_Abs']
            orders['DATE'] = orders['EntryTime'].dt.date
            orders['Capital_Available'] = total_capital
            #Identifying Exit orders on Orders Report 
            exitid = exits[exits['Exit_Type'].notna()]
            exitid = exitid[['SYMBOL','Exit_Type','Date']]
            exitid = exitid.sort_values('Date')
            exitid['TIF'] = np.where(exitid['Exit_Type']=='LO','DAY','AT_THE_CLOSE')
            exitid.rename(columns={'Exit_Type':'EntryType','Date':'EntryTime'},inplace=True)
            orders = orders.sort_values('EntryTime')
            orders = pd.merge_asof(orders, exitid, by=['SYMBOL','TIF'],left_on=['EntryTime'], right_on=['EntryTime'],direction='nearest',tolerance=pd.Timedelta("100ms"))     
            #Identifying Exit orders on Orders Report 
            #evalid = passed.reset_index()
            #evalid = evalid[['Date','SYMBOL','Status','Score_Brackets']]
            #evalid = evalid.sort_values('Date').set_index('Date').reset_index()
            #For merging eval message onto orders report to identify entry orders
            #orders = pd.merge_asof(orders, evalid, by=['SYMBOL'],left_on=['EntryTime'], right_on=['Date'],direction='backward',tolerance=pd.Timedelta("900ms"))
            #orders['Order_ms'] = np.where(orders['EntryTime_ms']==500000,'Eval-Passed',np.nan)
            orders['EntryType'] = np.where(orders['TIF']=='IOC','OPE',orders['EntryType'])
            orders['EntryType'] = np.where(orders['EntryType'].isna(),'Entry',orders['EntryType'])
            orders['TIF_in_seconds'] = (orders['LastModTime'] - orders['EntryTime']).dt.total_seconds()
            orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
            orders['Time_Diff'] = (orders['EntryTime'] - orders.groupby(['SYMBOL','Long_Short'])['EntryTime'].shift(1)).dt.total_seconds().fillna(100)
            orders['Time_Diff_Bool'] = orders['Time_Diff']>15
            orders['Unique'] = orders.groupby(['SYMBOL'])['Time_Diff_Bool'].cumsum()
            orders['QTY_Abs_Agg'] = orders.groupby(['SYMBOL','Long_Short','EntryTime'])[['QTY_Abs']].transform('sum')
            orders['QTY_Max_Desired'] = orders.groupby(['SYMBOL','Long_Short','Unique'])[['QTY_Abs_Agg']].transform('max')
            orders['$_Max_Desired'] = orders['QTY_Max_Desired'] * orders['Price']
            x = orders.groupby(['SYMBOL','Unique'])[['SYMBOL','Unique','Price','QTY_Max_Desired','$_Max_Desired']].head(1)
            x.rename(columns={'Price':'Start_Price'},inplace=True)
            orders.drop(columns=['QTY_Max_Desired','$_Max_Desired'],inplace=True)
            orders = pd.merge(orders,x,how='left')
            a1 = (orders['Start_Price']<5)
            a2 = (orders['Start_Price'].between(5,10,inclusive='left'))
            a3 = (orders['Start_Price'].between(10,15,inclusive='left'))
            a4 = (orders['Start_Price'].between(15,30,inclusive='left'))
            a5 = (orders['Start_Price'].between(30,60,inclusive='left'))
            a6 = (orders['Start_Price']>=60)
            vals = [0,.01,.02,.03,.04,.05]
            default = 0
            orders['Cross_Amount'] = np.select([a1,a2,a3,a4,a5,a6],vals,default)
            orders['Start_NBBO'] = np.where(orders['Quantity']>0,orders['Start_Price']-orders['Cross_Amount'],orders['Start_Price']+orders['Cross_Amount'])
            orders['Start_NBBO'] = np.where(orders['AvgFillPrice']==0,np.nan,orders['Start_NBBO'])

            orders['Day_Type'] = daytype

            
            
            #Confirm no lines with unassigned EntryType
            checkx = orders[orders['EntryType']=='X']
            len(checkx) == 0
            print('Number of unassigned order EntryTypes is {0}'.format(len(checkx))) 
            

            ######################
            #Read in Trade Reports
            tr.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            tr['EntryTime'] = pd.to_datetime(tr['EntryTime'])
            tr = tr.sort_values('EntryTime')
            tr = tr[tr['EntryTime']>date +' '+hr+':50:00']
            tr['DATE'] = tr['EntryTime'].dt.date
            tr['Day_of_Week'] = tr['EntryTime'].dt.dayofweek
            tr['Day_Type'] = daytype
            #Create slice of orders to only look at entries
            entries = orders[orders['EntryType']=='Entry']
            #deployed_by_symbol = entries[['SYMBOL','FilledQty','AvgFillPrice','LastModTime','EntryType','Filled$_Abs','FilledQTY_Abs','QTY_Abs','Capital_Available','Desired_$']].sort_values('LastModTime')
            #deployed_by_symbol.rename(columns={'FilledQty':'Quantity'},inplace=True)
            #tr = pd.merge_asof(tr,deployed_by_symbol,by=['SYMBOL','Quantity'],left_on=['EntryTime'],right_on=['LastModTime'],direction='nearest')
            tr['FilledQTY_Abs'] = np.absolute(tr['Quantity'])
            tr['Filled$_Abs'] = tr['FilledQTY_Abs'] * tr['EntryPrice']
            tr['Stuck'] = np.where(tr['ExitTime']=='1970-Jan-01 00:00:00','Stuck','Exited')
            cols = ['Quantity','RealizedPL','Filled$_Abs']
            tr[cols] = tr[cols].astype('float')
            trinst = tr.copy()
            x = tr.groupby(['SYMBOL'])[['Quantity','RealizedPL','Filled$_Abs']].sum().reset_index()
            tr.drop(columns=['Quantity','RealizedPL','Filled$_Abs'],inplace=True)
            tr = pd.merge(tr,x,how='left')
            tr.drop_duplicates(['SYMBOL'],inplace=True)
            tr['Basis_Return'] = tr['RealizedPL']/tr['Filled$_Abs']
            #Creating Trade Reports by 
            trinst['Basis_Return'] = trinst['RealizedPL']/trinst['Filled$_Abs']
            fortrinst = passed[['SYMBOL','MQ','ARTI','IADV','MQAS','MQAS_Score','IADV_Score','PRI','PRI_Score','Total_Score']].reset_index()
            fortrinst['Date'] = fortrinst['Date'].dt.tz_localize(None)
            fortrinst = fortrinst.sort_values(['SYMBOL','Date'])
            fortrinst['Total_Score'] = fortrinst['Total_Score'].replace(0,np.nan)
            fortrinst = fortrinst.fillna(method='bfill')
            fortrinst = fortrinst.sort_values('Date')
            trinst = pd.merge_asof(trinst,fortrinst,by=['SYMBOL'],left_on='EntryTime',right_on='Date',direction='nearest')
            trinst['ADV'] = trinst['ARTI'] / trinst['IADV']
            a1 = (trinst['Total_Score']>100)
            a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
            a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
            a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            trinst['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)

            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            trinst['ADV_Even_Brackets'] = pd.cut(trinst['ADV'], bins=cuts,duplicates='drop')
            trinst['MQ_Brackets'] = pd.cut(trinst['MQ'], bins=cuts,duplicates='drop')
            trinst['Imb_Brackets'] = pd.cut(trinst['ARTI'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0.00001,.1,.2,.3,.4,.5,.6,.7,.8,.9,1,2,5,np.inf]
            trinst['IADV_Even_Extended_Brackets'] = pd.cut(trinst['IADV'],bins=cuts)
            cuts = [-np.inf,0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]
            trinst['MQAS_Brackets'] = pd.cut(trinst['MQAS'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0,5,10,15,30,60,np.inf]
            trinst['Price_Brackets'] = pd.cut(trinst['EntryPrice'], bins=cuts)
            a1 = (trinst['EntryTime']>=(date + ' ' +hr + ':00')) & (trinst['EntryTime']<(date + ' ' +hr + ':55'))
            a2 = (trinst['EntryTime']>=(date + ' ' +hr + ':55')) & (trinst['EntryTime']<(date + ' ' +hr + ':58'))
            a3 = (trinst['EntryTime']>=(date + ' ' +hr + ':58'))
            vals = ['Entry50', 'Entry55', 'Entry58']
            trinst['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
            
            orders = pd.merge_asof(orders,fortrinst,by=['SYMBOL'], left_on='EntryTime', right_on='Date',direction='nearest')
            a1 = (orders['Total_Score']>100)
            a2 = (orders['Total_Score'].between(75,100,inclusive='left'))
            a3 = (orders['Total_Score'].between(50,75,inclusive='left'))
            a4 = (orders['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            orders['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)
            orders['ADV'] = orders['ARTI'] / orders['IADV']
            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            orders['ADV_Even_Brackets'] = pd.cut(orders['ADV'], bins=cuts,duplicates='drop')
            #Adding EntryTiming
            a1 = (orders['EntryTime']>=(date + ' ' +hr + ':00')) & (orders['EntryTime']<(date + ' ' +hr + ':55'))
            a2 = (orders['EntryTime']>=(date + ' ' +hr + ':55')) & (orders['EntryTime']<(date + ' ' +hr + ':58'))
            a3 = (orders['EntryTime']>=(date + ' ' +hr + ':58'))
            vals = ['Entry50', 'Entry55', 'Entry58']
            orders['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
            orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
            #Adding columns for price slippage

            orders['Slippage_$'] = np.where(orders['Quantity']>0,orders['Start_NBBO']-orders['AvgFillPrice'],orders['AvgFillPrice']-orders['Start_NBBO'])
            orders['Slipage_%'] = orders['Slippage_$']/orders['Start_NBBO']
            orders['Weighted_Price'] = orders['$_Max_Desired']*orders['Start_NBBO']
            orders['Slippage_$$, $Weighted'] = orders['$_Max_Desired']*orders['Slippage_$']
            orders['Weighted_Shares'] = orders['QTY_Max_Desired']*orders['Start_NBBO']
            orders['Slippage_$,Shares Weighted'] = orders['QTY_Max_Desired']*orders['Slippage_$']
            orders['Percent_of_Imbalance'] = orders['FilledQTY_Abs']/orders['ARTI']
            orders['Percent_of_Capital'] = orders['Filled$_Abs']/orders['Capital_Available']
            orders['on_packet'] = orders['EntryTime_ms']!=500000


            
            stuckpnlfile = date + ' suckpnl.csv'
            if stuckpnlfile in os.listdir('.'):
                stuckpnl = pd.read_csv(date +' stuckpnl.csv')
                stuckpnl.rename(columns={'Symbol':'SYMBOL'},inplace=True)
                trinst['Percent_of_Total'] = trinst.groupby('SYMBOL')['Quantity'].apply(lambda column: column/column.sum())
                trinst = pd.merge(trinst,stuckpnl,how='left')
                trinst['Net'] = trinst['Net'].fillna(0)
                trinst['StuckPNL'] = trinst['Percent_of_Total']*trinst['Net']
                trinst['Adj_RealizedPL'] = trinst['RealizedPL'] + trinst['StuckPNL']
            else:
                print('no stuckpnl file')

            print('tr done')

            #Combining attemps-df and orderlog-df
            combo = pd.merge(attempts, orderlog,how='outer')
            combo = combo.sort_values('Date')
            alllogs = pd.concat([orderlog, attempts, exits,ope,closeout,passed.reset_index()],keys=['orderlog', 'attempts', 'exits','ope','closeout','passed']).reset_index()
            
            alllogs.to_csv(writepath + date + ' alllogs.csv')
            tr.to_csv(writepath + date + ' tradereports.csv')
            trinst.to_csv(writepath + date + ' tr_byinstance.csv')
            orders.to_csv(writepath + date +' orders.csv')
            combo.to_csv(writepath + date +' orderslog.csv')
            exits.to_csv(writepath + date +' exits.csv')
            ope.to_csv(writepath + date +' ope.csv')
            closeout.to_csv(writepath + date +' closeout.csv')
            passed.to_csv(writepath + date +' passed.csv')
            capman.to_csv(writepath + date +' capman.csv')
            kb.to_csv(writepath + date +' kb.csv')    

            entries = orders[orders['EntryType']=='Entry']            
            ope = orders[orders['EntryType']=='OPE']


# In[986]:


def create_reports_nqc01(date): 
    ######################Read Files in######################
    writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    kbfile = date+ ' processorlogNSDQClose.csv'
    kbpath = writepath
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    opath = 'Z:/web-guys/data/ss-reports/'
    ofile0 = 'gptbs-nsdq-03_NsdqImbalanceToClose_0_' + date +'_order.csv'
    ofile1 = 'gptbs-nsdq-03_NsdqImbalanceToClose_1_' + date +'_order.csv'
    ofile2 = 'gptbs-nsdq-03_NsdqImbalanceToClose_2_' + date +'_order.csv'    
    trpath = 'Z:/web-guys/data/ss-reports/'
    trfile0 = 'gptbs-nsdq-03_NsdqImbalanceToClose_0_' + date + '_trade_reports.csv'
    trfile1 = 'gptbs-nsdq-03_NsdqImbalanceToClose_1_' + date + '_trade_reports.csv'
    trfile2 = 'gptbs-nsdq-03_NsdqImbalanceToClose_2_' + date + '_trade_reports.csv'
    if kbfile not in os.listdir(kbpath):
        print('no kbfile '+date)
    elif advfile not in os.listdir(advpath):
        print('no advfile '+date)
    elif ofile0 not in os.listdir(opath):
        print('no ofile '+date)
    elif trfile0 not in os.listdir(trpath):
        print('no trfile '+date)
    else:
        kb = pd.read_csv(kbfile,header=None)

        symbols = pd.read_csv(advpath + advfile)

        ofiles = [ofile0,ofile1,ofile2]
        oli = []
        for file in ofiles:
            df = pd.read_csv(opath + file)
            oli.append(df)
        orders = pd.concat(oli)

        trfiles = [trfile0,trfile1,trfile2]
        li = []
        for file in trfiles:
            df = pd.read_csv(trpath + file)
            li.append(df)
        tr = pd.concat(li)
        print('files present')
        
        if len(kb.index) < 500:
            print('kbfile incomplete '+date)
        elif len(orders.index) == 0:
            print('ofile incomplete '+date)
        elif len(tr.index) == 0:
            print('trfile incomplete '+date)
        else:
            print(len(kb.index))
            ######################Prep Evals File######################
            #Prep message column
            kb[1]=kb[1].astype('str')
            kb.columns = ['Date',0]
            kb_original = kb
            #Create separate dataframes of relevant info and then remove from KB
            capman = kb[kb[0].str.contains('CapMan')]
            kb = kb.iloc[kb.index.drop(capman.index)]
            kb = kb.reset_index(drop=True)
            evals = kb[kb[0].str.contains('Eval')]
            kb = kb.iloc[kb.index.drop(evals.index)]
            kb = kb.reset_index(drop=True)
            orderlog = kb[kb[0].str.contains('^Order')]
            kb = kb.iloc[kb.index.drop(orderlog.index)]
            kb = kb.reset_index(drop=True)
            attempts = kb[kb[0].str.contains('Attempting|Packet-|flipped')]
            kb = kb.iloc[kb.index.drop(attempts.index)]
            kb = kb.reset_index(drop=True)
            exits = kb[kb[0].str.contains('LOC-|IO-')]
            kb = kb.iloc[kb.index.drop(exits.index)]
            kb = kb.reset_index(drop=True)
            ope = kb[kb[0].str.contains('^OPE')]
            kb = kb.iloc[kb.index.drop(ope.index)]
            kb = kb.reset_index(drop=True)
            kb = kb[~kb[0].str.contains('m price')]

            #Find total capital available for the day
            t = capman[capman[0].str.contains('Succ')]
            t  = t.drop_duplicates()
            t = t.head(1)
            total_capital = t[0].str.extract('((?<=available:)\S{1,})').squeeze()
            total_capital = total_capital
            total_capital = int(total_capital)            
            print('Total Capital is: {0}'.format(total_capital))

            #Formatting evals files to breakout columns:
            evals[['Status','SYMBOL','message']] =evals[0].str.split(' ',n=2,expand=True)
            evals['Auction_Size'] = evals['message'].str.extract('((?<=auction_size:)\S{1,})')
            evals['Threshold'] = evals['message'].str.extract('((?<=threshold:)\S{1,})')
            evals['MQ'] = evals['message'].str.extract('((?<=mq:)\S{1,})')
            evals['ARTI'] = evals['message'].str.extract('((?<=arti:)\S{1,})')
            evals['ARTI'] = evals['ARTI'].astype('float')
            evals['IADV'] = evals['message'].str.extract('((?<=iadv:)\S{1,})')
            evals['MQAS'] = evals['message'].str.extract('((?<=mqas:)\S{1,})')
            evals['MQAS_Score'] = evals['message'].str.extract('((?<=mqas_score:)\S{1,})')
            evals['IADV'] = evals['message'].str.extract('((?<=iadv:)\S{1,})')
            evals['IADV_Score'] = evals['message'].str.extract('((?<=iadv_score:)\S{1,})')
            evals['PRI'] = evals['message'].str.extract('((?<=pri:)\S{1,})')
            evals['PRI_Score'] = evals['message'].str.extract('((?<=pri_score:)\S{1,})')
            evals['Total_Score'] = evals['message'].str.extract('((?<= score:)\S{1,})')
            evals['Total_Score'] = np.where(evals['Status']=='Eval-Passed',evals['Total_Score'],0)
            cols = ['MQ','ARTI','IADV','MQAS','MQAS_Score','IADV','IADV_Score','PRI','PRI_Score','Total_Score']
            evals[cols] = evals[cols].astype('float').values
            evals['Failed_Reason'] = evals['message'].str.extract('([a-z]*)')
            evals['Failed_Reason'] = np.where(evals['Status']=='Eval-Failed',evals['Failed_Reason'],np.nan)
            evals.sort_values('Date')
            a1 = (evals['Total_Score']>100)
            a2 = (evals['Total_Score'].between(75,100,inclusive='left'))
            a3 = (evals['Total_Score'].between(50,75,inclusive='left'))
            a4 = (evals['Total_Score'].between(20,50,inclusive='left'))
            vals = [.1,.08,.05,.02]
            default = .02
            evals['Max_Imbalance'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = [.02,.016,.01,.004]
            defaul = .004
            evals['Max_Capital'] = np.select([a1,a2,a3,a4],vals,default=default)
            vals = ['A','B','C','D']
            default = 'D'
            evals['Score_Brackets'] = np.select([a1,a2,a3,a4],vals,default=default)
            evals['Desired_Shares_Imbalance'] = evals['Max_Imbalance']*evals['ARTI']
            evals['Desired_Shares_Capital'] = evals['Max_Capital']*total_capital
            evals = evals.set_index('Date')
            evals.index = pd.to_datetime(evals.index)
            types = ['Eval-Passed','Oppset-Eval-Rescore']
            passed = evals[evals['Status'].isin(types)]
            fails = evals[evals['Status']=='Eval-Failed']
            MQAS_Score(passed)
            IADV_Score(passed)
            PRI_Score(passed)
            print('evals done')

            #Breakout attempts columns
            attempts['SYMBOL']= attempts[0].str.extract('([A-Z]{2,6})')
            attempts['Desired'] = attempts[0].str.extract('((?<=desired:)\S{1,})')
            attempts['Existing'] = attempts[0].str.extract('((?<=existing:)\S{1,})')
            attempts['Existing'] = attempts['Existing'].str.replace(',','')
            attempts['Net_Desired'] = attempts[0].str.extract('((?<=net_desired:)\S{1,})')
            attempts['Flip_Count'] = attempts[0].str.extract('((?<=flipped )\S{1,})')
            attempts['Packet_Score'] = attempts[0].str.extract('((?<=score:)\S{1,})')

            #Breakout attempts columns
            orderlog[['Status','SYMBOL','message']]=orderlog[0].str.split(' ',n=2,expand=True)
            orderlog['Price_Action'] = orderlog['message'].str.extract('((?<=price-action:)\S{1,})')
            orderlog['Order_Price'] = orderlog['message'].str.extract('((?<=price:)\S{1,})')
            orderlog['Order_Size'] = orderlog['message'].str.extract('((?<=size:)\S{1,})')

            #Combining attemps-df and orderlog-df
            combo = pd.merge(attempts, orderlog,how='outer')
            combo = combo.sort_values('Date')

            #Breakout exits columns
            exits = exits.reset_index(drop=True)
            exits_params = exits[exits[0].str.contains('Starting')]
            exits = exits.iloc[exits.index.drop(exits_params.index)]
            exits = exits.reset_index(drop=True)
            exits[['drop','Status','SYMBOL','message']]=exits[0].str.split(' ',n=3,expand=True)
            exits.pop('drop')
            exits['Position'] = exits['message'].str.extract('((?<=position:)\S{1,})')
            exits['Exit_LOCs'] = exits['message'].str.extract('((?<=exit_locs:)\S{1,})')
            exits['Additional_LOCs'] = exits['message'].str.extract('((?<=additional_locs:)\S{1,})')
            exits = exits.sort_values('Date')

            #Breakout ope columns
            if len(ope) == 0:
                pass
            else:
                ope[['drop','Status','SYMBOL','message']]=ope[0].str.split(' ',n=3,expand=True)
                ope.pop('drop')
                ope['Position'] = ope['message'].str.extract('((?<=position:)\S{1,})')
                ope['Uncovered_Position'] = ope['message'].str.extract('((?<=uncovered_position:)\S{1,})')

            ######################
            #Read in ADV files
            symbols.columns=['CUSIP','SYMBOL','Type','Exchange','Desc','ETB','ADV']
            nsdq = symbols[symbols['Exchange']=='R']
            nsdq = nsdq[nsdq['ETB']=='ETB']
            nsdq['ADV'] = nsdq['ADV'].astype('int')
            adv = nsdq[['SYMBOL','ADV']]

            ######################
            #Read in Orders Report
            orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
            orders['EntryTime_ms'] = orders['EntryTime'].dt.microsecond
            orders['LastModTime'] = pd.to_datetime(orders['LastModTime'])
            orders = orders.groupby(['OrderId']).tail(1)
            orders.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            orders['QTY_Abs'] = np.absolute(orders['Quantity'])
            orders['Desired_$'] = orders.QTY_Abs * orders.Price
            orders['FilledQTY_Abs'] = np.absolute(orders['FilledQty'])
            orders['Filled$_Abs'] = orders['FilledQTY_Abs']*orders['AvgFillPrice']
            orders['Fill_Rate'] = orders['FilledQTY_Abs']/orders['QTY_Abs']
            orders['DATE'] = orders['EntryTime'].dt.date
            orders['Capital_Available'] = total_capital

            a1 = (orders['EntryTime']>=(date + ' 20:50:00.000')) & (orders['EntryTime']<(date + ' 20:54:29.600')) & (orders['TIF']=='DAY')
            a2 = (orders['EntryTime']>=(date + ' 20:50:00.000')) & (orders['EntryTime']<(date + ' 20:54:30')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
            a3 = (orders['EntryTime']>=(date + ' 20:54:29.600')) & (orders['EntryTime']<(date + ' 20:55:00.000')) & (orders['TIF']=='DAY')
            a4 = (orders['EntryTime']==(date + ' 20:54:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
            a5 = (orders['EntryTime']==(date + ' 20:54:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='MARKET')
            a6 = (orders['EntryTime']>=(date + ' 20:55:00.000')) & (orders['EntryTime']<(date + ' 20:57:59.600')) & (orders['TIF']=='DAY')
            a7 = (orders['EntryTime']>=(date + ' 20:55:00.000')) & (orders['EntryTime']<(date + ' 20:57:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
            a8 = (orders['EntryTime']==(date + ' 20:57:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
            a9 = (orders['EntryTime']>=(date + ' 20:58:00.000'))  & (orders['EntryTime']<(date + ' 20:59:59'))& (orders['TIF']=='DAY') & (orders['EntryTime_ms']!=0)
            a10 = (orders['EntryTime']>=(date + ' 20:58:00.000')) & (orders['TIF']=='DAY') & (orders['EntryTime_ms']==0)
            a11 = (orders['EntryTime']>=(date + ' 20:58:00.000')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['EntryTime_ms']==0)
            a12 = (orders['EntryTime']>=(date + ' 20:59:00.000')) & (orders['TIF']=='DAY') 
            a13 = (orders['TIF']=='IOC')
            vals = ['Entry50','LOC1','Entry5430','LOC2','MOC','Entry55','LOC3','LOC4','Entry58','LO1','IO1','Reprice','OPE'] 
            orders['EntryType'] = np.select([a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13],vals,default='X')
            orders = orders.sort_values('EntryTime')
            #Confirm no lines with unassigned EntryType
            checkx = orders[orders['EntryType']=='X']
            len(checkx) == 0
            print('Number of unassigned order EntryTypes is {0}'.format(len(checkx))) 

            ######################
            #Read in Trade Reports
            tr.rename(columns={'Symbol':'SYMBOL'},inplace=True)
            tr['EntryTime'] = pd.to_datetime(tr['EntryTime'])
            tr = tr.sort_values('EntryTime')
            tr = tr[tr['EntryTime']>date + ' 20:50:00']
            tr['DATE'] = tr['EntryTime'].dt.date
            tr['Day_of_Week'] = tr['EntryTime'].dt.dayofweek
            #Create slice of orders to only look at entries
            entry_times = ['Entry50','Entry5430','Entry55','Entry58']
            entries = orders[orders['EntryType'].isin(entry_times)]
            deployed_by_symbol = entries[['SYMBOL','FilledQty','AvgFillPrice','LastModTime','EntryType','Filled$_Abs','FilledQTY_Abs','QTY_Abs','Capital_Available']].sort_values('LastModTime')
            deployed_by_symbol.rename(columns={'FilledQty':'Quantity'},inplace=True)
            tr = pd.merge_asof(tr,deployed_by_symbol,by=['SYMBOL','Quantity'],left_on=['EntryTime'],right_on=['LastModTime'],direction='nearest')
            tr['Stuck'] = np.where(tr['ExitTime']=='1970-Jan-01 00:00:00','Stuck','Exited')
            cols = ['Quantity','RealizedPL','AvgFillPrice','Filled$_Abs']
            tr[cols] = tr[cols].astype('float')
            trinst = tr.copy()
            x = tr.groupby(['SYMBOL'])[['Quantity','RealizedPL','Filled$_Abs']].sum().reset_index()
            tr.drop(columns=['Quantity','RealizedPL','Filled$_Abs'],inplace=True)
            tr = pd.merge(tr,x)
            tr.drop_duplicates(['SYMBOL'],inplace=True)
            tr['Basis_Return'] = tr['RealizedPL']/tr['Filled$_Abs']
            #Creating Trade Reports by 
            trinst['Basis_Return'] = trinst['RealizedPL']/trinst['Filled$_Abs']
            fortrinst = passed[['SYMBOL','MQ','ARTI','IADV','MQAS','MQAS_Score','IADV_Score','PRI','PRI_Score','Total_Score']].reset_index()
            fortrinst['Date'] = fortrinst['Date'].dt.tz_localize(None)
            fortrinst = fortrinst.sort_values(['SYMBOL','Date'])
            fortrinst['Total_Score'] = fortrinst['Total_Score'].replace(0,np.nan)
            fortrinst = fortrinst.fillna(method='bfill')
            fortrinst = fortrinst.sort_values('Date')
            trinst = pd.merge_asof(trinst,fortrinst,by=['SYMBOL'],left_on='EntryTime',right_on='Date',direction='nearest')
            trinst['ADV'] = trinst['ARTI'] * trinst['IADV']
            a1 = (trinst['Total_Score']>100)
            a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
            a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
            a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
            vals = ['A','B','C','D']
            default = 'D'
            trinst['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)

            cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
            trinst['ADV_Even_Brackets'] = pd.cut(trinst['ADV'], bins=cuts,duplicates='drop')
            trinst['MQ_Brackets'] = pd.cut(trinst['MQ'], bins=cuts,duplicates='drop')
            trinst['Imb_Brackets'] = pd.cut(trinst['ARTI'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0.00001,.1,.2,.3,.4,.5,.6,.7,.8,.9,1,2,5,np.inf]
            trinst['IADV_Even_Extended_Brackets'] = pd.cut(trinst['IADV'],bins=cuts)
            cuts = [-np.inf,0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]
            trinst['MQAS_Brackets'] = pd.cut(trinst['MQAS'], bins=cuts,duplicates='drop')
            cuts = [-np.inf,0,5,10,15,30,60,np.inf]
            trinst['Price_Brackets'] = pd.cut(trinst['EntryPrice'], bins=cuts)    

            stuckpnlfile = date + ' suckpnl.csv'
            if stuckpnlfile in os.listdir('.'):
                stuckpnl = pd.read_csv(date +' stuckpnl.csv')
                stuckpnl.rename(columns={'Symbol':'SYMBOL'},inplace=True)
                trinst['Percent_of_Total'] = trinst.groupby('SYMBOL')['Quantity'].apply(lambda column: column/column.sum())
                trinst = pd.merge(trinst,stuckpnl,how='left')
                trinst['Net'] = trinst['Net'].fillna(0)
                trinst['StuckPNL'] = trinst['Percent_of_Total']*trinst['Net']
                trinst['Adj_RealizedPL'] = trinst['RealizedPL'] + trinst['StuckPNL']
            else:
                print('no stuckpnl file')

            print('tr done')

            tr.to_csv(writepath + date + ' tradereports.csv')
            trinst.to_csv(writepath + date + ' tr_byinstance.csv')
            orders.to_csv(writepath + date +' orders.csv')
            combo.to_csv(writepath + date +' orderslog.csv')
            exits.to_csv(writepath + date +' exits.csv')
            ope.to_csv(writepath + date +' ope.csv')
            passed.to_csv(writepath + date +' passed.csv')
            capman.to_csv(writepath + date +' capman.csv')
            kb.to_csv(writepath + date +' kb.csv')    


# In[987]:


def MQAS_Score(df):
    a1 = (df.index.isin(df.between_time('20:50:00','20:55:00', include_end=False).index)) & (df['MQAS']<.20)   
    a2 = (df.index.isin(df.between_time('20:55:00','20:57:30', include_end=False).index)) & (df['MQAS']<.20) 
    a3 = (df.index.isin(df.between_time('20:57:30','20:00:00', include_end=False).index)) & (df['MQAS']<.20)   
    a4 = (df.index.isin(df.between_time('20:50:00','20:55:00', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left'))   
    a5 = (df.index.isin(df.between_time('20:55:00','20:57:30', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left'))
    a6 = (df.index.isin(df.between_time('20:57:30','20:00:00', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left'))
    a7 = (df.index.isin(df.between_time('20:50:00','20:55:00', include_end=False).index)) & (df['MQAS']>.5)   
    a8 = (df.index.isin(df.between_time('20:55:00','20:58:00', include_end=False).index)) & (df['MQAS']>.5)
    a9 = (df.index.isin(df.between_time('20:58:00','20:00:00', include_end=False).index)) & (df['MQAS']>.5)
    
    vals = [95,150,95,20,125,150,30,40,25]
    default = [np.nan]
    
    df['MQAS_Score_Check'] = np.select([a1,a2,a3,a4,a5,a6,a7,a8,a9],vals,default=default)
    df['MQAS_Score_Check'] = df['MQAS_Score_Check'].astype(float)


# In[988]:


def IADV_Score(df):
    a1 = (df.index.isin(df.between_time('20:50:00','20:55:00', include_end=False).index)) & (df['IADV']<2)   
    a2 = (df.index.isin(df.between_time('20:55:00','20:56:30', include_end=False).index)) & (df['IADV']<2) 
    a3 = (df.index.isin(df.between_time('20:56:30','20:58:30', include_end=False).index)) & (df['IADV']<2)
    a4 = (df.index.isin(df.between_time('20:58:30','20:00:00', include_end=False).index)) & (df['IADV']<2)
    a5 = (df.index.isin(df.between_time('20:50:00','20:58:30', include_end=False).index)) & (df['IADV']>=2)   
    a6 = (df.index.isin(df.between_time('20:58:30','20:00:00', include_end=False).index)) & (df['IADV']>=2)
    
    vals = [20,50,60,40,115,50]
    default = [np.nan]
    
    df['IADV_Score_Check'] = np.select([a1,a2,a3,a4,a5,a6],vals,default=default)
    df['IADV_Score_Check'] = df['IADV_Score_Check'].astype(float)


# In[989]:


def PRI_Score(df):
    a0 = (df.index.isin(df.between_time('20:50:00','20:55:00', include_end=False).index))
    a1 = (df.index.isin(df.between_time('20:55:00','20:57:30', include_end=False).index)) & (df['PRI']<.003)   
    a2 = (df.index.isin(df.between_time('20:57:30','20:00:00', include_end=False).index)) & (df['PRI']<.003) 
    a3 = (df.index.isin(df.between_time('20:55:00','20:57:30', include_end=False).index)) & (df['PRI'].between(.003,.005))   
    a4 = (df.index.isin(df.between_time('20:57:30','20:00:00', include_end=False).index)) & (df['PRI'].between(.003,.005))
    a5 = (df.index.isin(df.between_time('20:55:00','20:57:30', include_end=False).index)) & (df['PRI'].between(.005,.0075))   
    a6 = (df.index.isin(df.between_time('20:57:30','20:00:00', include_end=False).index)) & (df['PRI'].between(.005,.0075))
    a7 = (df.index.isin(df.between_time('20:55:00','20:57:30', include_end=False).index)) & (df['PRI'].between(.0075,.01))   
    a8 = (df.index.isin(df.between_time('20:57:30','20:00:00', include_end=False).index)) & (df['PRI'].between(.0075,.01))
    a9 = (df.index.isin(df.between_time('20:55:00','20:58:00', include_end=False).index)) & (df['PRI']>.01)   
    a10 = (df.index.isin(df.between_time('20:58:00','20:00:00', include_end=False).index)) & (df['PRI']>.01)  
    
    vals = [0,5,15,30,20,45,30,55,45,70,115]
    default = [np.nan]
    
    df['PRI_Score_Check'] = np.select([a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10],vals, default=default)
    df['PRI_Score_Check'] = df['PRI_Score_Check'].astype(float)


# In[990]:


def apply_scores(df,date):    
    first = (df['MQAS_Score'] + df['IADV_Score'])/2
    last = (df['MQAS_Score'] + df['IADV_Score'] + df['PRI_Score']*2)/4
    a1 = (df.index<date + ' 19:55:00')
    a2 = (df.index>date + ' 19:55:00') & (df['Indicated_Clearing_Price']!=0) & (df['ADV']<6000000)
    a3 = (df.index>date + ' 19:55:00') & (df['Indicated_Clearing_Price']!=0) & (df['ADV']>6000000)
    vals = [first,last,last]
    df['Total_Score'] = np.select([a1,a2,a3],vals,0)
    df['Total_Score'] = df['Total_Score']*10000
    df['Total_Score'] = np.where(df['Real_Time_Imbalance'].isna(),0,df['Total_Score'])


# In[991]:


def apply_nqc02_scorebrackets(df):
    a1 = (df['Total_Score']>100)
    a2 = (df['Total_Score'].between(75,100,inclusive='left'))
    a3 = (df['Total_Score'].between(50,75,inclusive='left'))
    a4 = (df['Total_Score'].between(20,50,inclusive='left'))
    a5 = (df['Total_Score'].between(0,20,inclusive='left'))

    vals = ['A','B','C','D','E']
    default = 'X'
    df['Score_Brackets'] = np.select([a1,a2,a3,a4,a5],vals,default=default)    


# In[992]:


def apply_boosts(df,date,clearing_spread_boost=True,preferred_boost=False):
    if clearing_spread_boost==False:
        df['Clearing_Spread_Boost'] = 0
        pass
    else:
        overliquid = ((df['Over_Liquid_25M_Hurdles']==True) | (df['Over_Liquid_6M_Hurdles']==True))
        cond = (df['Abs_Clearing_Spread']>.99) & (df.index>date+' 19:58:00') & overliquid
        df['Clearing_Spread_Boost'] = np.where(cond & clearing_spread_boost,100,0)
    if 'Type' in df.columns and preferred_boost==True:
        df['Preferred_Boost'] = np.where((df['Type']=='R') & preferred_boost,100,0)
    else:
        df['Preferred_Boost'] = 0
    df['Total_Score'] = df['Total_Score'] + df['Clearing_Spread_Boost'] + df['Preferred_Boost']


# In[993]:


def flip_hurdles_updated(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['is_Flip']>'NoFlip')
    
    vals = [False]
    default = True
    
    df['Over_Flip_Hurdle'] = np.select([a1],vals,default=default)


# In[994]:


def fpa_hurdles_updated(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:50:10', include_end=False).index)) & (df['direction_from_10']>.025)
    a2 = (df.index.isin(df.between_time('19:50:10','19:50:30', include_end=False).index)) & (df['direction_from_10']>.015)
    a3 = (df.index.isin(df.between_time('19:50:30','19:54:30', include_end=False).index)) & (df['direction_from_10']>.025)
    a4 = (df.index.isin(df.between_time('19:54:30','19:55:00', include_end=False).index)) & (df['direction_from_10']>.02)
    a5 = (df.index.isin(df.between_time('19:55:00','20:00', include_end=False).index)) & (df['direction_from_10']>.025)
    

    vals = [False,False,False,False,False]
    default = True

    df['Over_fpa_Hurdle'] = np.select([a1,a2,a3,a4,a5], vals, default=default)


# In[995]:


def fpa_hurdles_updated(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:50:10', include_end=False).index)) & (df['Total_Score']>75) & (df['direction_from_10']>.025)
    a2 = (df.index.isin(df.between_time('19:50:00','19:50:10', include_end=False).index)) & (df['Total_Score']<=75) & (df['direction_from_10']>.018)
    a3 = (df.index.isin(df.between_time('19:50:10','19:50:30', include_end=False).index)) & (df['direction_from_10']>.015)
    a4 = (df.index.isin(df.between_time('19:50:30','20:00:00', include_end=False).index)) & (df['Total_Score']>75) & (df['direction_from_10']>.025)
    a5 = (df.index.isin(df.between_time('19:50:30','20:00:00', include_end=False).index)) & (df['Total_Score']<=75) & (df['direction_from_10']>.018)

    

    vals = [False,False,False,False,False]
    default = True

    df['Over_fpa_Hurdle'] = np.select([a1,a2,a3,a4,a5], vals, default=default)


# In[996]:


def MQAS_hurdles_updated(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['MQAS']>.7)
    a2 = (df.index.isin(df.between_time('19:51:00','19:54:30', include_end=False).index)) & (df['MQAS']>.3) 
    a3 = (df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['MQAS']>.95)   
    a4 = (df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['MQAS']>.90)   

    vals = [False,False,False,False]
    default = True

    df['Over_MQAS_Hurdle'] = np.select([a1,a2,a3,a4], vals, default=default)


# In[997]:


def IADV_hurdles_updated_wscript(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:50:01', include_end=False).index)) & (df['IADV']<.41)
    a2 = (df.index.isin(df.between_time('19:50:01','19:50:30', include_end=False).index)) & (df['IADV']<.44)
    a3 = (df.index.isin(df.between_time('19:50:30','19:51:00', include_end=False).index)) & (df['IADV']<.50)
    a4 = (df.index.isin(df.between_time('19:51:00','19:54:30', include_end=False).index)) & (df['IADV']<.4)
    a5 = (df.index.isin(df.between_time('19:54:30','19:55:00', include_end=False).index)) & (df['IADV']<.4)
    a6 = (df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['IADV']<.16)
    a7 = (df.index.isin(df.between_time('19:57:00','19:57:40', include_end=False).index)) & (df['IADV']<.26)
    a8 = (df.index.isin(df.between_time('19:57:40','19:58:00', include_end=False).index)) & (df['IADV']<.16)
    a9 = (df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index)) & (df['IADV']<.08)
    a10 = (df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['IADV']<.12)


    vals = [False,False,False,False,False,False,False,False,False,False]
    default = True

    df['Over_IADV_Hurdle'] = np.select([a1,a2,a3,a4,a5,a6,a7,a8,a9,a10], vals, default=default)


# In[998]:


def pri_hurdles_updated(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index))
    a2 = (df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['Per_Return_Exp']<.001)
    #a3 = (df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index)) & (df['Per_Return_Exp']<.003)
    #a4 = (df.index.isin(df.between_time('19:55:15','19:57:00', include_end=False).index)) & (df['Per_Return_Exp']<.01)
    #a5 = (df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index)) & (df['Per_Return_Exp']<.005)
    
    vals = [True,False]
    default = True

    df['Over_PRI_Hurdle'] = np.select([a1,a2], vals, default=default)


# In[999]:


def iadv_score_updated(df):
    rd = pd.read_csv('nqc_iadv_score_update.csv',index_col=0).reset_index()
    cols = rd.columns.to_list()
    rd = pd.melt(rd,id_vars='index',value_vars=cols[1:])
    rd.rename(columns={'index':'Time'},inplace=True)
    rd = rd.sort_values(['Time','variable'])
    rd['variable'] = rd['variable'].astype(float)
    rd['Time'] = pd.to_datetime(rd['Time'])
    rd['Time2'] = rd['Time']+ + pd.Timedelta('15s')
    rd['variable2'] = rd['variable'].shift(-1)
    rd['variable2'] = np.where(rd['variable2']==-0.001,np.inf,rd['variable2'])
    rd['variable2'] = rd['variable2'].fillna(np.inf)
    rd[['Time','Time2','variable','variable2']] = rd[['Time','Time2','variable','variable2']].astype(str)
    stat1 = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index))"
    stat2 = "(df['IADV'].between("+rd['variable']+","+rd['variable2']+"))"
    rd['stat'] = stat1 + "&" + stat2 
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    vals = rd['value'].astype('float').to_list()
    a0=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a1=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a2=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a3=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a4=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a5=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a6=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a7=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a8=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a9=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a10=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a11=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a12=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a13=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a14=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a15=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a16=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a17=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a18=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a19=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a20=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a21=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a22=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a23=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a24=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a25=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a26=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a27=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a28=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a29=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a30=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a31=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a32=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a33=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a34=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a35=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a36=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a37=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a38=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a39=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a40=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a41=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a42=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a43=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a44=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a45=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a46=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a47=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a48=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a49=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a50=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a51=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a52=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a53=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a54=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a55=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a56=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a57=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a58=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a59=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a60=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a61=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a62=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a63=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a64=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a65=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a66=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a67=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a68=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a69=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a70=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a71=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a72=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a73=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a74=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a75=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a76=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a77=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a78=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a79=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a80=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a81=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a82=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a83=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a84=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a85=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a86=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a87=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a88=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a89=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a90=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a91=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a92=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a93=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a94=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a95=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a96=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a97=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a98=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a99=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a100=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a101=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a102=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a103=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a104=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a105=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a106=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a107=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a108=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a109=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a110=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a111=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a112=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a113=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a114=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a115=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a116=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a117=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a118=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a119=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a120=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a121=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a122=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a123=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a124=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a125=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a126=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a127=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a128=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a129=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a130=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a131=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a132=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a133=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a134=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a135=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a136=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a137=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a138=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a139=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a140=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a141=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a142=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a143=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a144=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a145=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a146=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a147=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a148=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a149=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a150=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a151=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a152=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a153=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a154=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a155=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a156=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a157=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a158=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a159=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a160=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a161=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a162=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a163=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a164=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a165=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a166=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a167=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a168=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a169=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a170=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a171=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a172=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a173=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a174=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a175=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a176=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a177=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a178=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a179=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a180=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a181=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a182=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a183=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a184=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a185=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a186=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a187=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a188=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a189=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a190=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a191=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a192=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a193=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a194=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['IADV'].between(2.0,np.inf))
    a195=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['IADV'].between(-0.001,0.4))
    a196=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['IADV'].between(0.4,0.7))
    a197=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['IADV'].between(0.7,0.9))
    a198=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['IADV'].between(0.9,2.0))
    a199=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['IADV'].between(2.0,np.inf))

    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,
    a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,a62,a63,a64,a65,
    a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87,a88,a89,a90,a91,a92,a93,a94,a95,a96,a97,a98,a99,a100,a101,
    a102,a103,a104,a105,a106,a107,a108,a109,a110,a111,a112,a113,a114,a115,a116,a117,a118,a119,a120,a121,a122,a123,a124,a125,a126,a127,a128,a129,a130,a131,a132,a133,a134,a135,a136,a137,a138,a139,a140,a141,a142,a143,a144,a145,a146,a147,a148,a149,a150,a151,
    a152,a153,a154,a155,a156,a157,a158,a159,a160,a161,a162,a163,a164,a165,a166,a167,a168,a169,a170,a171,a172,a173,a174,a175,a176,a177,a178,a179,a180,a181,a182,a183,a184,a185,a186,a187,a188,a189,a190,a191,a192,a193,a194,a195,a196,a197,a198,a199]

    df['IADV_Score'] = np.select(statements,vals,'X')
    df['IADV_Score'] = df['IADV_Score'].astype(float)


# In[1000]:


def mqas_score_updated(df):
    rd = pd.read_csv('nqc_mqas_score_update.csv',index_col=0).reset_index()
    cols = rd.columns.to_list()
    rd = pd.melt(rd,id_vars='index',value_vars=cols[1:])
    rd.rename(columns={'index':'Time'},inplace=True)
    rd = rd.sort_values(['Time','variable'])
    rd['variable'] = rd['variable'].astype(float)
    rd['Time'] = pd.to_datetime(rd['Time'])
    rd['Time2'] = rd['Time']+ + pd.Timedelta('15s')
    rd['variable2'] = rd['variable'].shift(-1)
    rd['variable2'] = np.where(rd['variable2']==-0.001,np.inf,rd['variable2'])
    rd['variable2'] = rd['variable2'].fillna(np.inf)
    rd[['Time','Time2','variable','variable2']] = rd[['Time','Time2','variable','variable2']].astype(str)
    stat1 = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index))"
    stat2 = "(df['MQAS'].between("+rd['variable']+","+rd['variable2']+"))"
    rd['stat'] = stat1 + "&" + stat2 
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    vals = rd['value'].astype('float').to_list()
    a0=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a1=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a2=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a3=(df.index.isin(df.between_time('19:50:00','19:50:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a4=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a5=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a6=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a7=(df.index.isin(df.between_time('19:50:15','19:50:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a8=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a9=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a10=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a11=(df.index.isin(df.between_time('19:50:30','19:50:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a12=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a13=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a14=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a15=(df.index.isin(df.between_time('19:50:45','19:51:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a16=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a17=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a18=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a19=(df.index.isin(df.between_time('19:51:00','19:51:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a20=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a21=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a22=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a23=(df.index.isin(df.between_time('19:51:15','19:51:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a24=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a25=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a26=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a27=(df.index.isin(df.between_time('19:51:30','19:51:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a28=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a29=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a30=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a31=(df.index.isin(df.between_time('19:51:45','19:52:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a32=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a33=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a34=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a35=(df.index.isin(df.between_time('19:52:00','19:52:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a36=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a37=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a38=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a39=(df.index.isin(df.between_time('19:52:15','19:52:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a40=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a41=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a42=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a43=(df.index.isin(df.between_time('19:52:30','19:52:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a44=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a45=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a46=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a47=(df.index.isin(df.between_time('19:52:45','19:53:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a48=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a49=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a50=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a51=(df.index.isin(df.between_time('19:53:00','19:53:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a52=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a53=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a54=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a55=(df.index.isin(df.between_time('19:53:15','19:53:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a56=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a57=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a58=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a59=(df.index.isin(df.between_time('19:53:30','19:53:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a60=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a61=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a62=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a63=(df.index.isin(df.between_time('19:53:45','19:54:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a64=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a65=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a66=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a67=(df.index.isin(df.between_time('19:54:00','19:54:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a68=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a69=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a70=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a71=(df.index.isin(df.between_time('19:54:15','19:54:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a72=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a73=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a74=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a75=(df.index.isin(df.between_time('19:54:30','19:54:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a76=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a77=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a78=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a79=(df.index.isin(df.between_time('19:54:45','19:55:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a80=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a81=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a82=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a83=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a84=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a85=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a86=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a87=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a88=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a89=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a90=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a91=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a92=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a93=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a94=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a95=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a96=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a97=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a98=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a99=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a100=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a101=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a102=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a103=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a104=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a105=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a106=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a107=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a108=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a109=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a110=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a111=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a112=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a113=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a114=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a115=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a116=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a117=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a118=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a119=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a120=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a121=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a122=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a123=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a124=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a125=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a126=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a127=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a128=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a129=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a130=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a131=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a132=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a133=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a134=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a135=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a136=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a137=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a138=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a139=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a140=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a141=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a142=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a143=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a144=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a145=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a146=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a147=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a148=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a149=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a150=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a151=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a152=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a153=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a154=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a155=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    a156=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['MQAS'].between(-0.001,0.1))
    a157=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['MQAS'].between(0.1,0.4))
    a158=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['MQAS'].between(0.4,0.6))
    a159=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['MQAS'].between(0.6,np.inf))
    
    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,
    a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,a62,a63,a64,a65,
    a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87,a88,a89,a90,a91,a92,a93,a94,a95,a96,a97,a98,a99,a100,a101,
    a102,a103,a104,a105,a106,a107,a108,a109,a110,a111,a112,a113,a114,a115,a116,a117,a118,a119,a120,a121,a122,a123,a124,a125,a126,a127,a128,a129,a130,a131,a132,a133,a134,a135,a136,a137,a138,a139,a140,a141,a142,a143,a144,a145,a146,a147,a148,a149,a150,a151,
    a152,a153,a154,a155,a156,a157,a158,a159]

    df['MQAS_Score'] = np.select(statements,vals,'X')
    df['MQAS_Score'] = df['MQAS_Score'].astype(float)


# In[1001]:


def pri_score_updated(df):
    rd = pd.read_csv('nqc_pri_score_update.csv',index_col=0).reset_index()
    cols = rd.columns.to_list()
    rd = pd.melt(rd,id_vars='index',value_vars=cols[1:])
    rd.rename(columns={'index':'Time'},inplace=True)
    rd = rd.sort_values(['Time','variable'])
    rd['variable'] = rd['variable'].astype(float)
    rd['Time'] = pd.to_datetime(rd['Time'])
    rd['Time2'] = rd['Time']+ + pd.Timedelta('15s')
    rd['variable2'] = rd['variable'].shift(-1)
    rd['variable2'] = np.where(rd['variable2']==0.00,np.inf,rd['variable2'])
    rd['variable2'] = rd['variable2'].fillna(np.inf)
    rd[['Time','Time2','variable','variable2']] = rd[['Time','Time2','variable','variable2']].astype(str)
    stat1 = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index))"
    stat2 = "(df['Per_Return_Exp'].between("+rd['variable']+","+rd['variable2']+"))"
    rd['stat'] = stat1 + "&" + stat2 
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    vals = rd['value'].astype('float').to_list()

    a0=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a1=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a2=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a3=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a4=(df.index.isin(df.between_time('19:55:00','19:55:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a5=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a6=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a7=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a8=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a9=(df.index.isin(df.between_time('19:55:15','19:55:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a10=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a11=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a12=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a13=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a14=(df.index.isin(df.between_time('19:55:30','19:55:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a15=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a16=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a17=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a18=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a19=(df.index.isin(df.between_time('19:55:45','19:56:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a20=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a21=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a22=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a23=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a24=(df.index.isin(df.between_time('19:56:00','19:56:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a25=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a26=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a27=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a28=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a29=(df.index.isin(df.between_time('19:56:15','19:56:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a30=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a31=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a32=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a33=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a34=(df.index.isin(df.between_time('19:56:30','19:56:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a35=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a36=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a37=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a38=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a39=(df.index.isin(df.between_time('19:56:45','19:57:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a40=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a41=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a42=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a43=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a44=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a45=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a46=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a47=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a48=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a49=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a50=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a51=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a52=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a53=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a54=(df.index.isin(df.between_time('19:57:30','19:57:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a55=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a56=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a57=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a58=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a59=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a60=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a61=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a62=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a63=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a64=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a65=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a66=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a67=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a68=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a69=(df.index.isin(df.between_time('19:58:15','19:58:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a70=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a71=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a72=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a73=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a74=(df.index.isin(df.between_time('19:58:30','19:58:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a75=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a76=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a77=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a78=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a79=(df.index.isin(df.between_time('19:58:45','19:59:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a80=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a81=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a82=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a83=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a84=(df.index.isin(df.between_time('19:59:00','19:59:15', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a85=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a86=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a87=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a88=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a89=(df.index.isin(df.between_time('19:59:15','19:59:30', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a90=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a91=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a92=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a93=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a94=(df.index.isin(df.between_time('19:59:30','19:59:45', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    a95=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0,0.0015))
    a96=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.0015,0.003))
    a97=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.003,0.005))
    a98=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.005,0.01))
    a99=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['Per_Return_Exp'].between(0.01,np.inf))
    
    #manually adding a value for the first five minutes
    a100 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index))
    vals.append(0)

    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,
    a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,a62,a63,a64,a65,
    a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87,a88,a89,a90,a91,a92,a93,a94,a95,a96,a97,a98,a99,a100]
    
    df['PRI_Score'] = np.select(statements,vals,'X').astype(float)


# In[1002]:


def write_kb_splits_backtests(date,path,hr,str_month,pathname,filedate,back_id):
    if os.path.exists(path):
        pass
    else:
        os.mkdir(path)
    ######################Read Files in######################
    print(date)
    kbfile = 'processor1.log'
    kbpath = 'Z:/NQC/nqc-backtest/'+pathname+'/logs/'
    advfile = '210916 NSDQ ADV.csv'
    advpath = 'Z:/NASDAQ/NSDQ 0.1/'
    opath = 'Z:/NQC/nqc-backtest/'+pathname+'/ss-reports/'
    ofile0 = 'BACK_NsdqImbalanceToClose_0_'+filedate+'_'+back_id+'_start_09-17-2021_end_09-17-2021_order.csv'
    ofile1 = 'BACK_NsdqImbalanceToClose_1_'+filedate+'_'+back_id+'_start_09-17-2021_end_09-17-2021_order.csv'
    ofile2 = 'BACK_NsdqImbalanceToClose_2_'+filedate+'_'+back_id+'_start_09-17-2021_end_09-17-2021_order.csv'    
    trpath = 'Z:/NQC/nqc-backtest/'+pathname+'/ss-reports/'
    trfile0 = 'BACK_NsdqImbalanceToClose_0_'+filedate+'_'+back_id+'_start_09-17-2021_end_09-17-2021_trade_reports.csv'
    trfile1 = 'BACK_NsdqImbalanceToClose_1_'+filedate+'_'+back_id+'_start_09-17-2021_end_09-17-2021_trade_reports.csv'
    trfile2 = 'BACK_NsdqImbalanceToClose_2_'+filedate+'_'+back_id+'_start_09-17-2021_end_09-17-2021_trade_reports.csv'
    evpath = 'Z:/NQC/nqc-backtest/'+pathname+'/custom_logs/NsdqImbalanceToClose/'
    evfile0 = 'NsdqImbalanceToClose_0_evals.log'
    evfile1 = 'NsdqImbalanceToClose_1_evals.log'
    evfile2 = 'NsdqImbalanceToClose_2_evals.log'    
    if kbfile not in os.listdir(kbpath):
        print('no kbfile '+date)
    elif advfile not in os.listdir(advpath):
        print('no advfile '+date)
    elif ofile0 not in os.listdir(opath):
        print('no ofile '+date)
    elif trfile0 not in os.listdir(trpath):
        print('no trfile '+date)
    elif evfile0 not in os.listdir(evpath):
        print('no evfile '+date)
    else:
        kb = pd.read_csv(kbpath + kbfile,header=None, sep='\t')

        symbols = pd.read_csv(advpath + advfile)

        ofiles = [ofile0,ofile1,ofile2]
        oli = []
        for file in ofiles:
            df = pd.read_csv(opath + file)
            oli.append(df)
        orders = pd.concat(oli)
        orders.to_parquet(path+date+' orders.parquet')

        trfiles = [trfile0,trfile1,trfile2]
        li = []
        for file in trfiles:
            df = pd.read_csv(trpath + file)
            li.append(df)
        tr = pd.concat(li)
        tr.to_parquet(path+date+' tr.parquet')

        evfiles = [evfile0,evfile1,evfile2]
        li=[]
        for file in evfiles:
            df = pd.read_csv(evpath + file,header=None)
            li.append(df)
        ev = pd.concat(li)
        print('files present')

        if len(kb.index) < 20000:
            print('kbfile incomplete '+date)
        elif len(orders.index) == 0:
            print('ofile incomplete '+date)
        elif len(tr.index) == 0:
            print('trfile incomplete '+date)
        elif len(ev.index) == 0:
            print(len(ev.index))
        else:
            print(len(kb.index))
            ######################Prep Evals File######################
            #Prep message column
            kb = pd.concat([kb,ev])
            kb = kb.reset_index()
            kb[0]=kb[0].astype('str')
            kb['Date'] = kb[0].str.extract('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})',expand=True)
            kb = kb[['Date',0]]
            kb[0] = kb[0].str.replace('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})','')
            kb['Instance'] = kb[0].str.extract('(NsdqImbalanceToClose_[0,1,2] )')
            kb[0] = kb[0].str.replace('(NsdqImbalanceToClose_0 )','')
            kb[0] = kb[0].str.replace('(NsdqImbalanceToClose_1 )','')
            kb[0] = kb[0].str.replace('(NsdqImbalanceToClose_2 )','')
            kb[0] = kb[0].str.replace('INFO|WARN|ERROR','')
            kb['Date'] = kb[0].str.extract('((?<=EPOCH_MICROS )\d*)')
            kb['Date'] = pd.to_datetime(kb['Date'].astype('float'), unit='us')
            kb['Date'] = kb['Date'].fillna(method='ffill')
            kb['Date'] = kb['Date'].fillna(method='bfill')
            kb.rename(columns={0:'0'},inplace=True)
            kb.to_parquet(path+date+' kb.parquet')


# In[1003]:


def write_raw_reports_20220601(date,path):
    kb = pd.read_parquet(path+date+' kb.parquet')
    evals = kb[kb['0'].str.contains('Eval|Hurdles')]
    kb = kb.iloc[kb.index.drop(evals.index)]
    kb = kb.reset_index(drop=True)
    evals.to_parquet(path + date + ' evals.parquet')
    capman = kb[kb['0'].str.contains('CapMan')]
    kb = kb.iloc[kb.index.drop(capman.index)]
    kb = kb.reset_index(drop=True)
    capman.to_parquet(path + date + ' capman.parquet')
    orderlog = kb[kb['0'].str.contains('Order-')]
    kb = kb.iloc[kb.index.drop(orderlog.index)]
    kb = kb.reset_index(drop=True)
    orderlog.to_parquet(path + date + ' orderlog.parquet')
    attempts = kb[kb['0'].str.contains('Attempting|Packet-|flipped|Attempt-')]
    kb = kb.iloc[kb.index.drop(attempts.index)]
    kb = kb.reset_index(drop=True)
    attempts.to_parquet(path + date + ' attempts.parquet')
    exits = kb[kb['0'].str.contains('Exit-')]
    kb = kb.iloc[kb.index.drop(exits.index)]
    kb = kb.reset_index(drop=True)
    exits.to_parquet(path + date + ' exits.parquet')
    ope = kb[kb['0'].str.contains('OPE-')]
    kb = kb.iloc[kb.index.drop(ope.index)]
    kb = kb.reset_index(drop=True)
    ope.to_parquet(path + date + ' ope.parquet')
    closeout = kb[kb['0'].str.contains('Closeout-')]
    kb = kb.iloc[kb.index.drop(closeout.index)]
    kb = kb.reset_index(drop=True)
    kb = kb[~kb['0'].str.contains('m price')]
    closeout.to_parquet(path + date + ' closeout.parquet')


# In[1004]:


def get_total_capital(date,path):
    capman = pd.read_parquet(path+date+' capman.parquet')
    t = capman[capman['0'].str.contains('Successful')]
    t = t.head(1)
    t  = t.drop_duplicates()
    total_capital = t['0'].str.extract('((?<=available:)\S{1,})').squeeze()
    total_capital = total_capital.replace(',','')
    total_capital = int(total_capital)            
    return total_capital


# In[1005]:


def write_evals_20220601(date,path,total_capital): 
    #Formatting evals files to breakout columns:
    evals = pd.read_parquet(path+date+' evals.parquet')
    evals[[0,1,'Status','SYMBOL','message']] = evals['0'].str.split(' ',n=4,expand=True)
    evals.drop(columns=[0,1],inplace=True)
    evals = evals[~evals['Status'].str.contains('Oppset-Eval-Starting')]
    evals = evals[~evals['Status'].str.contains('Oppset-Eval-Finished')]
    evals['Hurdle_Set'] = evals['message'].str.extract('((?<=set:)\S{1,})')
    evals['Auction_Size'] = evals['message'].str.extract('((?<=auction_size:)\S{1,})')
    evals['Threshold'] = evals['message'].str.extract('((?<=threshold:)\S{1,})')
    evals['MQ'] = evals['message'].str.extract('((?<=mq:)\S{1,})')
    evals['ARTI'] = evals['message'].str.extract('((?<=arti:)\S{1,})')
    evals['ARTI'] = evals['ARTI'].astype('float')
    evals['IADV'] = evals['message'].str.extract('((?<=iadv:)\S{1,})')
    evals['MQAS'] = evals['message'].str.extract('((?<=mqas:)\S{1,})')
    evals['MQAS_Score'] = evals['message'].str.extract('((?<=mqas_score:)\S{1,})')
    evals['IADV'] = evals['message'].str.extract('((?<=iadv:)\S{1,})')
    evals['IADV_Score'] = evals['message'].str.extract('((?<=iadv_score:)\S{1,})')
    evals['PRI'] = evals['message'].str.extract('((?<=pri:)\S{1,})')
    evals['PRI_Score'] = evals['message'].str.extract('((?<=pri_score:)\S{1,})')
    evals['Total_Score'] = evals['message'].str.extract('((?<= score:)\S{1,})')
    evals['Total_Score'] = np.where(evals['Status']=='Eval-Passed',evals['Total_Score'],0)
    cols = ['MQ','ARTI','IADV','MQAS','MQAS_Score','IADV','IADV_Score','PRI','PRI_Score','Total_Score']
    evals[cols] = evals[cols].astype('float').values
    evals['Failed_Reason'] = evals['message'].str.extract('([a-z]*)')
    evals['Failed_Reason'] = np.where(evals['Status']=='Eval-Failed',evals['Failed_Reason'],np.nan)
    evals.sort_values('Date')
    a1 = (evals['Total_Score']>100)
    a2 = (evals['Total_Score'].between(75,100,inclusive='left'))
    a3 = (evals['Total_Score'].between(50,75,inclusive='left'))
    a4 = (evals['Total_Score'].between(20,50,inclusive='left'))
    vals = [.1,.08,.05,.02]
    default = .02
    evals['Max_Imbalance'] = np.select([a1,a2,a3,a4],vals,default=default)
    vals = [.02,.016,.01,.004]
    defaul = .004
    evals['Max_Capital'] = np.select([a1,a2,a3,a4],vals,default=default)
    vals = ['A','B','C','D']
    default = 'D'
    evals['Score_Brackets'] = np.select([a1,a2,a3,a4],vals,default=default)
    evals['Desired_Shares_Imbalance'] = evals['Max_Imbalance']*evals['ARTI']
    evals['Desired_Shares_Capital'] = evals['Max_Capital']*total_capital
    evals = evals.set_index('Date')
    evals.index = pd.to_datetime(evals.index)
    types = ['Eval-Passed','Oppset-Eval-Rescore','Hurdles-Passed']
    passed = evals[evals['Status'].isin(types)]
    fails = evals[evals['Status'].str.contains('Eval-Failed')]
    MQAS_Score(passed)
    IADV_Score(passed)
    PRI_Score(passed)
    passed.to_parquet(path+date+' passed.parquet')
    


# In[1006]:


def write_orderlog_20220601(date,path):
    attempts = pd.read_parquet(path+date+' attempts.parquet')
    #Breakout attempts columns
    attempts['SYMBOL']= attempts['0'].str.extract('([A-Z]{2,6})')
    attempts['Desired'] = attempts['0'].str.extract('((?<=desired_size:)\S{1,})')
    attempts['Existing'] = attempts['0'].str.extract('((?<=existing:)\S{1,})')
    attempts['Existing'] = attempts['Existing'].str.replace(',','')
    attempts['Net_Desired'] = attempts['0'].str.extract('((?<=net_desired_size:)\S{1,})')
    attempts['Flip_Count'] = attempts['0'].str.extract('((?<=flipped )\S{1,})')
    attempts['Packet_Score'] = attempts['0'].str.extract('((?<=score:)\S{1,})')

    #Breakout attempts columns
    orderlog = pd.read_parquet(path+date+' orderlog.parquet')
    if len(orderlog) == 0:
        pass
    else:
        orderlog[['Status','SYMBOL','message']]=orderlog['0'].str.split(' ',n=2,expand=True)
        orderlog['Price_Action'] = orderlog['message'].str.extract('((?<=price-action:)\S{1,})')
        orderlog['Order_Price'] = orderlog['message'].str.extract('((?<=price:)\S{1,})')
        orderlog['Order_Size'] = orderlog['message'].str.extract('((?<=size:)\S{1,})')
        orderlog['Abort_Reason'] = orderlog['message'].str.extract('((?<=reason:)\S{1,})')

    #Combining attemps-df and orderlog-df
    combo = pd.merge(attempts, orderlog,how='outer')
    combo = combo.sort_values('Date')
    combo.to_csv(path+date+' orderlog.csv')


# In[1007]:


def write_exits_20220601(date,path):
    exits = pd.read_parquet(path+date+' exits.parquet')
    exits = exits.reset_index(drop=True)
    exits_params = exits[exits['0'].str.contains('Starting')]
    exits = exits.iloc[exits.index.drop(exits_params.index)]
    exits = exits.reset_index(drop=True)
    exits[[0,1,'Status','SYMBOL','message']]=exits['0'].str.split(' ',n=4,expand=True)
    exits.drop(columns=[0,1],inplace=True)
    exits['Exit_Type'] = exits['message'].str.extract('((?<= type:)\S{1,})')
    exits['Exit_To_Cover'] = exits['message'].str.extract('((?<=to-cover:)\S{1,})')
    exits['Gross_Position'] = exits['message'].str.extract('((?<=gross:)\S{1,})')
    exits['Uncovered_Position'] = exits['message'].str.extract('((?<=uncovered:)\S{1,})')
    exits['Skipped_LO_Size'] = exits['message'].str.extract('((?<=skipped-lo-size:)\S{1,})')
    exits['Unmarketable_Size'] = exits['message'].str.extract('((?<=unmarketable-size:)\S{1,})')
    exits['Unmarketable_on_Opposite_Side'] = exits['message'].str.extract('((?<=unmarketable-on-opposite-side:)\S{1,})')
    exits['Exit_Price'] = exits['message'].str.extract('((?<=price:)\S{1,})')
    exits['Desired_Exit_Size'] = exits['message'].str.extract('((?<=desired-size:)\S{1,})')
    exits['Net_Exit_Size'] = exits['message'].str.extract('((?<=net-size:)\S{1,})')
    exits['Canceled_Exit_Size'] = exits['message'].str.extract('((?<=canceled-size:)\S{1,})')
    exits['Bid'] = exits['message'].str.extract('((?<=bid:)\S{1,})')
    exits['Ask'] = exits['message'].str.extract('((?<=ask:)\S{1,})')
    exits['ICP'] = exits['message'].str.extract('((?<=icp:)\S{1,})')
    exits['Avg_Price'] = exits['message'].str.extract('((?<=avg-price:)\S{1,})')
    #exits['Mid_ICP_NBBO'] = np.where(exits['Exit_To_Cover']<0,(exits['Ask']+exits['ICP'])/2,(exits['Bid']+exits['ICP'])/2)
    #exits['Mid_ICP_AvgPrice'] = (exits['Avg_Price'] + exits['ICP'])/2
    #exits['Calc_LO_Exit_Price'] = np.where(exits['Exit_To_Cover']<0,np.minimum(exits['Mid_ICP_NBBO'],exits['Mid_ICP_AvgPrice'],exits['ICP']),np.maximum(exits['Mid_ICP_NBBO'],exits['Mid_ICP_AvgPrice'],exits['ICP']))
    #exits[['Mid_ICP_NBBO','Mid_ICP_AvgPrice','Calc_LO_Exit_Price']] = np.where(exits['Exit_Type']=='LO',exits[['Mid_ICP_NBBO','Mid_ICP_AvgPrice','Calc_LO_Exit_Price']],np.nan)
    exits = exits.sort_values('Date')
    exits.to_csv(path+date+' exits.csv')


# In[1008]:


def write_orders_20220601(date,path,total_capital):
    orders = pd.read_parquet(path+date+' orders.parquet')
    tr = pd.read_parquet(path+date+' tr.parquet')
    passed = pd.read_parquet(path+date+' passed.parquet')
    adv,nsdq = cf.get_symbolslist(date)

    ######################
    #Read in Orders Report
    orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
    orders['EntryTime_ms'] = orders['EntryTime'].dt.microsecond
    orders['LastModTime'] = pd.to_datetime(orders['LastModTime'])
    orders = orders.groupby(['OrderId']).tail(1)
    orders.rename(columns={'Symbol':'SYMBOL'},inplace=True)
    orders['QTY_Abs'] = np.absolute(orders['Quantity'])
    orders['Desired_$'] = orders.QTY_Abs * orders.Price
    orders['FilledQTY_Abs'] = np.absolute(orders['FilledQty'])
    orders['Filled$_Abs'] = orders['FilledQTY_Abs']*orders['AvgFillPrice']
    orders['Fill_Rate'] = orders['FilledQTY_Abs']/orders['QTY_Abs']
    orders['DATE'] = orders['EntryTime'].dt.date
    orders['Capital_Available'] = total_capital

    a1 = (orders['EntryTime']>=(date +' '+hr+':50:00.000')) & (orders['EntryTime']<(date +' '+hr+':54:29.600')) & (orders['TIF']=='DAY')
    a2 = (orders['EntryTime']>=(date +' '+hr+':50:00.000')) & (orders['EntryTime']<(date +' '+hr+':54:30')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
    a3 = (orders['EntryTime']>=(date +' '+hr+':54:29.600')) & (orders['EntryTime']<(date +' '+hr+':55:00.000')) & (orders['TIF']=='DAY')
    a4 = (orders['EntryTime']==(date +' '+hr+':54:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
    a5 = (orders['EntryTime']==(date +' '+hr+':54:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='MARKET')
    a6 = (orders['EntryTime']>=(date +' '+hr+':55:00.000')) & (orders['EntryTime']<(date +' '+hr+':57:59.600')) & (orders['TIF']=='DAY')
    a7 = (orders['EntryTime']>=(date +' '+hr+':55:00.000')) & (orders['EntryTime']<(date +' '+hr+':57:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
    a8 = (orders['EntryTime']==(date +' '+hr+':57:59')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['Type']=='LIMIT')
    a9 = (orders['EntryTime']>=(date +' '+hr+':58:00.000'))  & (orders['EntryTime']<(date +' '+hr+':59:59'))& (orders['TIF']=='DAY') & (orders['EntryTime_ms']!=0)
    a10 = (orders['EntryTime']>=(date +' '+hr+':58:00.000')) & (orders['TIF']=='DAY') & (orders['EntryTime_ms']==0)
    a11 = (orders['EntryTime']>=(date +' '+hr+':58:00.000')) & (orders['TIF']=='AT_THE_CLOSE') & (orders['EntryTime_ms']==0)
    a12 = (orders['EntryTime']>=(date +' '+hr+':59:00.000')) & (orders['TIF']=='DAY') 
    a13 = (orders['TIF']=='IOC')
    vals = ['Entry50','LOC1','Entry5430','LOC2','MOC','Entry55','LOC3','LOC4','Entry58','LO1','IO1','Reprice','OPE'] 
    orders['EntryType'] = np.select([a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13],vals,default='X')
    orders = orders.sort_values('EntryTime')
    #Confirm no lines with unassigned EntryType
    checkx = orders[orders['EntryType']=='X']
    len(checkx) == 0
    print('Number of unassigned order EntryTypes is {0}'.format(len(checkx))) 

    ######################
    #Read in Trade Reports
    tr.rename(columns={'Symbol':'SYMBOL'},inplace=True)
    tr['EntryTime'] = pd.to_datetime(tr['EntryTime'])
    tr = tr.sort_values('EntryTime')
    tr = tr[tr['EntryTime']>date +' '+hr+':50:00']
    tr['DATE'] = tr['EntryTime'].dt.date
    tr['Day_of_Week'] = tr['EntryTime'].dt.dayofweek
    #Create slice of orders to only look at entries
    entry_times = ['Entry50','Entry5430','Entry55','Entry58']
    entries = orders[orders['EntryType'].isin(entry_times)]
    deployed_by_symbol = entries[['SYMBOL','FilledQty','AvgFillPrice','LastModTime','EntryType','Filled$_Abs','FilledQTY_Abs','QTY_Abs','Capital_Available']].sort_values('LastModTime')
    deployed_by_symbol.rename(columns={'FilledQty':'Quantity'},inplace=True)
    tr = pd.merge_asof(tr,deployed_by_symbol,by=['SYMBOL','Quantity'],left_on=['EntryTime'],right_on=['LastModTime'],direction='nearest')
    tr['Stuck'] = np.where(tr['ExitTime']=='1970-Jan-01 00:00:00','Stuck','Exited')
    cols = ['Quantity','RealizedPL','AvgFillPrice','Filled$_Abs']
    tr[cols] = tr[cols].astype('float')
    trinst = tr.copy()
    x = tr.groupby(['SYMBOL'])[['Quantity','RealizedPL','Filled$_Abs']].sum().reset_index()
    tr.drop(columns=['Quantity','RealizedPL','Filled$_Abs'],inplace=True)
    tr = pd.merge(tr,x)
    tr.drop_duplicates(['SYMBOL'],inplace=True)
    tr['Basis_Return'] = tr['RealizedPL']/tr['Filled$_Abs']
    #Creating Trade Reports by 
    trinst['Basis_Return'] = trinst['RealizedPL']/trinst['Filled$_Abs']
    fortrinst = passed[['SYMBOL','MQ','ARTI','IADV','MQAS','MQAS_Score','IADV_Score','PRI','PRI_Score','Total_Score']].reset_index()
    fortrinst['Date'] = fortrinst['Date'].dt.tz_localize(None)
    fortrinst = fortrinst.sort_values(['SYMBOL','Date'])
    fortrinst['Total_Score'] = fortrinst['Total_Score'].replace(0,np.nan)
    fortrinst = fortrinst.fillna(method='bfill')
    fortrinst = fortrinst.sort_values('Date')
    trinst = pd.merge_asof(trinst,fortrinst,by=['SYMBOL'],left_on='EntryTime',right_on='Date',direction='nearest')
    trinst['ADV'] = trinst['ARTI'] * trinst['IADV']
    a1 = (trinst['Total_Score']>100)
    a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
    a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
    a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
    vals = ['A','B','C','D']
    default = 'D'
    trinst['Score_Bracket'] = np.select([a1,a2,a3,a4],vals,default=default)

    cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
    trinst['ADV_Even_Brackets'] = pd.cut(trinst['ADV'], bins=cuts,duplicates='drop')
    trinst['MQ_Brackets'] = pd.cut(trinst['MQ'], bins=cuts,duplicates='drop')
    trinst['Imb_Brackets'] = pd.cut(trinst['ARTI'], bins=cuts,duplicates='drop')
    cuts = [-np.inf,0.00001,.1,.2,.3,.4,.5,.6,.7,.8,.9,1,2,5,np.inf]
    trinst['IADV_Even_Extended_Brackets'] = pd.cut(trinst['IADV'],bins=cuts)
    cuts = [-np.inf,0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]
    trinst['MQAS_Brackets'] = pd.cut(trinst['MQAS'], bins=cuts,duplicates='drop')
    cuts = [-np.inf,0,5,10,15,30,60,np.inf]
    trinst['Price_Brackets'] = pd.cut(trinst['EntryPrice'], bins=cuts)    

    stuckpnlfile = date + ' suckpnl.csv'
    if stuckpnlfile in os.listdir('.'):
        stuckpnl = pd.read_csv(date +' stuckpnl.csv')
        stuckpnl.rename(columns={'Symbol':'SYMBOL'},inplace=True)
        trinst['Percent_of_Total'] = trinst.groupby('SYMBOL')['Quantity'].apply(lambda column: column/column.sum())
        trinst = pd.merge(trinst,stuckpnl,how='left')
        trinst['Net'] = trinst['Net'].fillna(0)
        trinst['StuckPNL'] = trinst['Percent_of_Total']*trinst['Net']
        trinst['Adj_RealizedPL'] = trinst['RealizedPL'] + trinst['StuckPNL']
    orders.to_csv(path+date+' orders.csv')
    tr.to_csv(path+date+' tradereports.csv')
    trinst.to_csv(path+date+' tr_byinstance.csv')


# In[1009]:


def write_closeout(date,path,environment='production'):

    closeout = pd.read_parquet(path+date+' closeout.parquet')
    if environment == 'backtest':
        splits = 4
        closeout[[0,1,'Status','SYMBOL','message']]=closeout['0'].str.split(' ',n=splits,expand=True)
        closeout.drop(columns=[0,1],inplace=True)
    else:
        splits = 2
        closeout[['Status','SYMBOL','message']]=closeout['0'].str.split(' ',n=splits,expand=True)
    closeout['Uncovered_Position'] = closeout['message'].str.extract('((?<=uncovered:)\S{1,})')
    closeout['Bid'] = closeout['message'].str.extract('((?<=bid:)\S{1,})')
    closeout['Ask'] = closeout['message'].str.extract('((?<=ask:)\S{1,})')
    closeout['ICP'] = closeout['message'].str.extract('((?<=icp:)\S{1,})')
    closeout['Closeout_Type'] = closeout['message'].str.extract('((?<=strategy:)\S{1,})')
    closeout['Closeout_Price'] = closeout['message'].str.extract('((?<=new-price:)\S{1,})')
    closeout['Day_Type'] = cf.get_daytype(date)
    closeout.to_csv(path+date+' closeout.csv')


# In[1010]:


def write_ope_20220601(date,path):
    ope = pd.read_parquet(path+date+' ope.parquet')
    if len(ope.index)==0:
        pass
    else:
        ope[[0,1,'Status','SYMBOL','message']]=ope['0'].str.split(' ',n=4,expand=True)
        ope.drop(columns=[0,1],inplace=True)
        ope['Position'] = ope['message'].str.extract('((?<=position:)\S{1,})')
        ope['Uncovered_Position'] = ope['message'].str.extract('((?<=uncovered:)\S{1,})')
        ope['Bid'] = ope['message'].str.extract('((?<=bid:)\S{1,})')
        ope['Ask'] = ope['message'].str.extract('((?<=ask:)\S{1,})')
        ope['10min_Ref'] = ope['message'].str.extract('((?<=10min-ref:)\S{1,})')
        ope['Last_Ref'] = ope['message'].str.extract('((?<=last-ref:)\S{1,})')
        ope['ICP'] = ope['message'].str.extract('((?<=icp:)\S{1,})')
        ope['Opportunity_Cost'] = ope['message'].str.extract('((?<=opportunity-cost:)\S{1,})')
        ope.to_csv(path+date+' ope.csv')


# In[1011]:


def write_kb_splits(date, path, hr, str_month,environment,hostname):
    hr = '19'
    filedate = ''
    LIVE_SIM_id = ''
    #new version
    total_capital=None
    #writepath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+date +'/'.
    writepath = path
    if not os.path.exists(writepath):
        os.makedirs(writepath)
    pathname = date
    ######################Read Files in######################
    print(date)
    #kbpath = 'Z:/NQC/nqc-live-sim/'+pathname+'/logs/'
    kbpath = 'Z:/NASDAQ/NSDQ Close/DayResults/'+ date + '/'
    #kbfile0 = 'processor0.log'    
    #kbfile1 = 'processor1.log'
    #kbfile2 = 'processor2.log' 
    kbfile = date +' processorlogNSDQClose_'+environment+'.csv'
    #kbpath = '.'
    advfile = date+'_master-symbol-list.csv'
    advpath = 'Z:/web-guys/memo/symbols/'
    opath = 'T:/runs/'+environment+'/'+date+'/'+hostname+'.dc.gptbs.com/ss-reports/'
    ofile0 = 'NsdqImbalanceToClose_0_order.csv'
    ofile1 = 'NsdqImbalanceToClose_1_order.csv'
    ofile2 = 'NsdqImbalanceToClose_2_order.csv'    
    trpath = 'T:/runs/'+environment+'/'+date+'/'+hostname+'.dc.gptbs.com/ss-reports/'
    trfile0 = 'NsdqImbalanceToClose_0_trade_reports.csv'
    trfile1 = 'NsdqImbalanceToClose_1_trade_reports.csv'
    trfile2 = 'NsdqImbalanceToClose_2_trade_reports.csv'
    evpath = 'T:/runs/'+environment+'/'+date+'/'+hostname+'.dc.gptbs.com/custom_logs/NsdqImbalanceToClose/'
    datestr = date.replace('-','')
    evfile0 = 'NsdqImbalanceToClose_0_evals.log'
    evfile1 = 'NsdqImbalanceToClose_1_evals.log'
    evfile2 = 'NsdqImbalanceToClose_2_evals.log'   
    daytype = cf.get_daytype(date)
    if kbfile not in os.listdir(kbpath):
        print('no kbfile '+kbfile)
    elif advfile not in os.listdir(advpath):
        print('no advfile '+advfile)
    elif ofile0 not in os.listdir(opath):
        print('no ofile '+ofile0)
    elif trfile0 not in os.listdir(trpath):
        print('no trfile '+trfile0)
    elif evfile0 not in os.listdir(evpath):
        print('no evfile '+evfile0)
    else:
        kb = pd.read_csv(kbpath + kbfile,header=None)

        #kbfiles = [kbfile0,kbfile1,kbfile2]
        #kbli = []
        #for file in kbfiles:
        #    df = pd.read_csv(kbpath + file,header=None, sep='\t')
        #    kbli.append(df)
        #kb = pd.concat(kbli)

        symbols = pd.read_csv(advpath + advfile)

        ofiles = [ofile0,ofile1,ofile2]
        oli = []
        for file in ofiles:
            df = pd.read_csv(opath + file)
            oli.append(df)
        orders = pd.concat(oli)
        orders.to_parquet(path+date+' orders.parquet')

        trfiles = [trfile0,trfile1,trfile2]
        li = []
        for file in trfiles:
            df = pd.read_csv(trpath + file)
            li.append(df)
        tr = pd.concat(li)
        tr.to_parquet(path+date+' tr.parquet')

        evfiles = [evfile0,evfile1,evfile2]
        li=[]
        for file in evfiles:
            df = pd.read_csv(evpath + file,header=None)
            li.append(df)
        ev = pd.concat(li)
        print('files present')

        if len(kb.index) < 500:
            print('kbfile incomplete '+date)
        elif len(orders.index) == 0:
            print('ofile incomplete '+date)
        elif len(tr.index) == 0:
            print('trfile incomplete '+date)
        elif len(ev.index) == 0:
            print(len(ev.index))
        else:
            print(len(kb.index))
            ######################Prep Evals File######################
            #Prep message column
            ev['Date'] = ev[0].str.extract('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})',expand=True)
            ev = ev[['Date',0]]
            ev[0] = ev[0].str.replace('([2022]+-'+str_month+'+-[0-9]+ [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}.[0-9]{0,7})','')
            ev['Instance'] = ev[0].str.extract('(NsdqImbalanceToClose_[0,1,2] )')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_0 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_1 )','')
            ev[0] = ev[0].str.replace('(NsdqImbalanceToClose_2 )','')
            ev[0] = ev[0].str.replace('INFO|WARN|ERROR','')
            ev['backup_date'] = ev['Date'].str.extract(
                '([0-9]{4}-[A-z]{2,3}-[0-9]{2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2} )')
            ev['backup_date'] = ev['backup_date'].str.rstrip()
            ev['backup_date'] = np.where(ev['backup_date'].isna(), np.nan, ev['backup_date'] + '.000001')
            ev['backup_date'] = pd.to_datetime(ev['backup_date'])
            ev['Date'] = ev['Date'].str.replace('15:50:00 ', '15:50:00.000000')
            ev['Date']=pd.to_datetime(ev['Date'],format='%Y-%b-%d %H:%M:%S.%f',errors='coerce')
            ev['Date'] = np.where(ev["Date"].isna(), ev['backup_date'], ev["Date"])
            x = int(hr) - ev['Date'].min().hour
            ev['Date'] = ev['Date'] + pd.Timedelta(hours=x)
            kb = pd.read_csv(kbpath + kbfile,header=None)
            kb.pop(0)
            kb.rename(columns={1:'Date',2:0},inplace=True)
            kb['Date'] = pd.to_datetime(kb['Date'])
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb['Day_UTC'] = kb['Date'].dt.tz_localize('UTC') + pd.Timedelta(hours=5)
            kb['Day_CT'] = kb['Day_UTC'].dt.tz_convert('US/Central')
            kb['Date'] = kb['Date'].dt.tz_localize(None)
            kb = pd.concat([kb,ev])
            kb_original = kb
            kb.rename(columns={0:'0'},inplace=True)
            kb.to_parquet(path+date+' kb.parquet')


# In[1012]:


def write_raw_reports(date,path):
    kb = pd.read_parquet(path+date+' kb.parquet')
    kb = kb.sort_values('Date').reset_index(drop=True)
    #Create separate dataframes of relevant info and then remove from KB
    capman = kb[kb['0'].str.contains('CapMan|BP Change',regex=True)]
    kb = kb.iloc[kb.index.drop(capman.index)]
    kb = kb.reset_index(drop=True)
    capman.to_parquet(path+date+' capman.parquet')
    evals = kb[kb['0'].str.contains('Eval|Hurdles|Packet-Failed|Quote-Failed')]
    kb = kb.iloc[kb.index.drop(evals.index)]
    kb = kb.reset_index(drop=True)
    evals.to_parquet(path+date+' evals.parquet')
    orderlog = kb[kb['0'].str.contains('Order-')]
    kb = kb.iloc[kb.index.drop(orderlog.index)]
    kb = kb.reset_index(drop=True)
    orderlog.to_parquet(path+date+' orderlog.parquet')
    attempts = kb[kb['0'].str.contains('Attempting|Packet-|flipped|Attempt-')]
    kb = kb.iloc[kb.index.drop(attempts.index)]
    kb = kb.reset_index(drop=True)
    attempts.to_parquet(path+date+' attempts.parquet')
    exits = kb[kb['0'].str.contains('Exit-|Calc-Uncovered')]
    kb = kb.iloc[kb.index.drop(exits.index)]
    kb = kb.reset_index(drop=True)
    exits.to_parquet(path+date+' exits.parquet')
    ope = kb[kb['0'].str.contains('OPE-')]
    kb = kb.iloc[kb.index.drop(ope.index)]
    kb = kb.reset_index(drop=True)
    ope.to_parquet(path+date+' ope.parquet')
    closeout = kb[kb['0'].str.contains('Closeout-')]
    kb = kb.iloc[kb.index.drop(closeout.index)]
    kb = kb.reset_index(drop=True)
    closeout.to_parquet(path+date+' closeout.parquet')
    profit = kb[kb['0'].str.contains('Profit-')]
    kb = kb.iloc[kb.index.drop(profit.index)]
    kb = kb.reset_index(drop=True)
    profit.to_parquet(path+date+' profit.parquet')
    ten_min = kb[kb['0'].str.contains('Set-10m-Last-Trade-Price')]
    kb = kb.iloc[kb.index.drop(profit.index)]
    kb = kb.reset_index(drop=True)     
    ten_min.to_parquet(path+date+' ten_min.parquet')
    kb.to_parquet(path+date+' kb_remaining.parquet')


# In[1013]:


def write_evals_outdated(date,path,total_capital,mqas_score_func,iadv_score_func,pri_score_func,environment='production',):
    evals = pd.read_parquet(path+date+' evals.parquet')
    if environment == 'backtest':
        splits = 4
        evals[[0,1,'Status','SYMBOL','message']] =evals['0'].str.split(' ',n=splits,expand=True)
        evals.drop(columns=[0,1],inplace=True)
    else:
        splits = 2
        evals[['Status','SYMBOL','message']] =evals['0'].str.split(' ',n=splits,expand=True)
    evals['Hurdle_Set'] = evals['message'].str.extract('((?<=set:)\S{1,})')
    evals.reset_index(inplace=True,drop=True)
    types = ['Eval-Passed','Oppset-Eval-Rescore','Hurdles-Passed']
    passed = evals[evals['Status'].isin(types)]
    passed['Auction_Size'] = passed['message'].str.extract('((?<=auction_size:)\S{1,})')
    passed['Threshold'] = passed['message'].str.extract('((?<=threshold:)\S{1,})')
    passed['MQ'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
    passed['ARTI'] = passed['message'].str.extract('((?<=imbal:)\S{1,})')
    passed['ARTI'] = passed['ARTI'].astype('float')
    passed['ARTI'] = np.abs(passed['ARTI'])
    passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
    passed['MQAS'] = passed['message'].str.extract('((?<=mqas:)\S{1,})')
    passed['MQAS_Score_Actual'] = passed['message'].str.extract('((?<=mqas_score:)\S{1,})')
    passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
    passed['IADV_Score_Actual'] = passed['message'].str.extract('((?<=iadv_score:)\S{1,})')
    passed['PRI'] = passed['message'].str.extract('((?<=pri:)\S{1,})')
    passed['PRI_Score_Actual'] = passed['message'].str.extract('((?<=pri_score:)\S{1,})')
    passed['Total_Score_Actual'] = passed['message'].str.extract('((?<= score:)\S{1,})')
    passed['Total_Score_Actual'] = np.where(passed['Status']=='Eval-Passed',passed['Total_Score_Actual'],0)
    passed['ADV'] = passed['message'].str.extract('((?<= adv:)\S{1,})')
    passed['Flips'] = passed['message'].str.extract('((?<=flips:)\S{1,})')
    passed['direction_from_10'] = passed['message'].str.extract('((?<=10min-trade-pa:)\S{1,})')
    passed['Notional_Auction_Size'] = passed['message'].str.extract('((?<=notional-auction:)\S{1,})')
    passed['matched_quantity'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
    passed['min-pri'] = passed['message'].str.extract('((?<=min-pri:)\S{1,})')
    numcols = ['ADV','Flips','direction_from_10','Notional_Auction_Size','matched_quantity','min-pri']
    passed[numcols] = passed[numcols].astype('float')
    cols = ['MQ','ARTI','IADV','MQAS','MQAS_Score_Actual','IADV','IADV_Score_Actual','PRI','PRI_Score_Actual','Total_Score_Actual']
    passed[cols] = passed[cols].astype('float').values
    passed.sort_values('Date')
    a1 = (passed['Total_Score_Actual']>100)
    a2 = (passed['Total_Score_Actual'].between(75,100,inclusive='left'))
    a3 = (passed['Total_Score_Actual'].between(50,75,inclusive='left'))
    a4 = (passed['Total_Score_Actual'].between(20,50,inclusive='left'))
    vals = [.1,.08,.05,.02]
    default = .02
    passed['Max_Imbalance'] = np.select([a1,a2,a3,a4],vals,default=default)
    vals = [.02,.016,.01,.004]
    defaul = .004
    passed['Max_Capital'] = np.select([a1,a2,a3,a4],vals,default=default)
    vals = ['A','B','C','D']
    default = 'D'
    passed['Score_Brackets'] = np.select([a1,a2,a3,a4],vals,default=default)
    passed['Desired_Shares_Imbalance'] = passed['Max_Imbalance']*passed['ARTI']
    passed['Desired_Shares_Capital'] = passed['Max_Capital']*total_capital
    passed = passed.set_index('Date')
    passed.index = pd.to_datetime(passed.index)
    mqas_score_func(passed)
    iadv_score_func(passed)
    pri_score_func(passed)
    passed = passed.reset_index().drop_duplicates().set_index('Date')
    passed['Day_Type'] = cf.get_daytype(date)
    passed = passed[passed.index<date+' 20:00']

    fails['Failed_Reason'] = fails['message'].str.replace('set:liquid ', '')
    fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('set:base ', '')
    fails['Failed_Reason'] = fails['Failed_Reason'].str.extract('((?!:)^\D{1,20})')
    fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('-', '')
    fails['Failed_Reason_Value'] = fails['message'].str.extract('((?<=threshold:)\S{1,})')
    fails.to_parquet(path+date+' fails.parquet')
    passed.to_parquet(path+date+' passed.parquet')
    passed.to_csv(path+date+' passed.csv')

def write_evals(date,path,total_capital,environment='production',):
    evals = pd.read_parquet(path+date+' evals.parquet')
    if environment == 'backtest':
        splits = 4
        evals[[0,1,'Status','SYMBOL','message']] =evals['0'].str.split(' ',n=splits,expand=True)
        evals.drop(columns=[0,1],inplace=True)
    else:
        splits = 2
        evals[['Status','SYMBOL','message']] =evals['0'].str.split(' ',n=splits,expand=True)
    evals['Hurdle_Set'] = evals['message'].str.extract('((?<=set:)\S{1,})')
    evals.reset_index(inplace=True,drop=True)
    types = ['Eval-Passed','Oppset-Eval-Rescore','Hurdles-Passed']
    passed = evals[evals['Status'].isin(types)]
    fails = evals.iloc[evals.index.drop(passed.index)]
    fails = fails.reset_index(drop=True)
    passed['Auction_Size'] = passed['message'].str.extract('((?<=auction_size:)\S{1,})')
    passed['Threshold'] = passed['message'].str.extract('((?<=threshold:)\S{1,})')
    passed['MQ'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
    passed['ARTI'] = passed['message'].str.extract('((?<=imbal:)\S{1,})')
    passed['ARTI'] = passed['ARTI'].astype('float')
    passed['ARTI'] = np.abs(passed['ARTI'])
    passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
    passed['MQAS'] = passed['message'].str.extract('((?<=mqas:)\S{1,})')
    passed['IADV'] = passed['message'].str.extract('((?<=iadv:)\S{1,})')
    passed['Per_Return_Exp'] = passed['message'].str.extract('((?<=pri:)\S{1,})')
    passed['ADV'] = passed['message'].str.extract('((?<= adv:)\S{1,})')
    passed['Flips'] = passed['message'].str.extract('((?<=flips:)\S{1,})')
    passed['direction_from_10'] = passed['message'].str.extract('((?<=10min-trade-pa:)\S{1,})')
    passed['Notional_Auction_Size'] = passed['message'].str.extract('((?<=notional-auction:)\S{1,})')
    passed['matched_quantity'] = passed['message'].str.extract('((?<=mq:)\S{1,})')
    passed['min-pri'] = passed['message'].str.extract('((?<=min-pri:)\S{1,})')
    passed['Bid'] = passed['message'].str.extract('((?<=bid:)\S{1,})')
    passed['Ask'] = passed['message'].str.extract('((?<=ask:)\S{1,})')
    passed['Indicated_Clearing_Price'] = passed['message'].str.extract('((?<=icp:)\S{1,})')
    passed['direction_from_10'] = passed['message'].str.extract('((?<=10min-trade-pa:)\S{1,})')

    numcols = ['ADV','Flips','direction_from_10','Notional_Auction_Size','matched_quantity','min-pri','Bid','Ask','Indicated_Clearing_Price','direction_from_10']
    passed[numcols] = passed[numcols].astype('float').values
    cols = ['MQ','ARTI','IADV','MQAS','IADV','Per_Return_Exp']
    passed[cols] = passed[cols].astype('float').values
    passed.sort_values('Date')
    passed = passed.set_index('Date')
    passed.index = pd.to_datetime(passed.index)
    passed = passed.reset_index().drop_duplicates().set_index('Date')
    passed['Day_Type'] = cf.get_daytype(date)
    passed = passed[passed.index<date+' 20:00']
    fails = pd.read_parquet(path+date+' evals.parquet')
    fails = fails[fails['0'].str.contains('Failed')]
    splits = 4
    fails[[0, 1, 'Status', 'SYMBOL', 'message']] = fails['0'].str.split(' ', n=splits, expand=True)
    fails.drop(columns=[0, 1], inplace=True)
    fails['Hurdle_Set'] = fails['message'].str.extract('((?<=set:)\S{1,})')
    fails.reset_index(inplace=True, drop=True)
    fails['Failed_Reason'] = fails['message'].str.replace('set:liquid ','')
    fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('set:base ','')
    fails['Failed_Reason'] = fails['Failed_Reason'].str.extract('((?!:)^\D{1,20})')
    fails['Failed_Reason'] = fails['Failed_Reason'].str.replace('-','')
    fails['Failed_Reason_Value'] = fails['message'].str.extract('((?<=threshold:)\S{1,})')
    logs = pd.read_parquet(path+date+' orderlog.parquet')
    logs['Date'] = pd.to_datetime(logs['Date'])
    logs = logs[logs['0'].str.contains('Attempt-Position')]
    logsmerge = logs[['Date', 'SYMBOL', 'MQAS_Score_Actual', 'IADV_Score_Actual', 'PRI_Score_Actual',
                      'Boost_Actual', 'Total_Score_Actual']]
    logsmerge.set_index('Date', inplace=True)
    passed = pd.merge_asof(passed.reset_index('Date'), logsmerge.reset_index('Date'), by='SYMBOL', left_on='Date',
                           right_on='Date', direction='forward', tolerance=pd.Timedelta('500ms')).set_index('Date')

    fails.to_parquet(path+date+' fails.parquet')
    passed.to_parquet(path+date+' passed.parquet')
    passed.to_csv(path+date+' passed.csv')


# In[1014]:


def write_orderlog(date,path,total_capital,environment='production'):

    attempts = pd.read_parquet(path+date+' attempts.parquet')
    orderlog = pd.read_parquet(path+date+' orderlog.parquet')
    attempts['SYMBOL']= attempts['0'].str.extract('([A-Z]{2,6})')
    attempts['Desired'] = attempts['0'].str.extract('((?<=desired:)\S{1,})')
    attempts['Existing'] = attempts['0'].str.extract('((?<=existing:)\S{1,})')
    attempts['Existing'] = attempts['Existing'].str.replace(',','')
    attempts['Net_Desired'] = attempts['0'].str.extract('((?<=net_desired:)\S{1,})')
    attempts['is_Flip'] = attempts['0'].str.extract('((?<=flipped )\S{1,})')
    attempts['Packet_Score'] = attempts['0'].str.extract('((?<=score:)\S{1,})')
    attempts = attempts.drop_duplicates()
    attempts['MQAS_Score_Actual'] = attempts['0'].str.extract('((?<=mqas-score:)\S{1,})')
    attempts['IADV_Score_Actual'] = attempts['0'].str.extract('((?<=iadv-score:)\S{1,})')
    attempts['PRI_Score_Actual'] = attempts['0'].str.extract('((?<=pri-score:)\S{1,})')
    attempts['Boost_Actual'] = attempts['0'].str.extract('((?<=hurdle-boost-score:)\S{1,})')
    attempts['Total_Score_Actual'] = attempts['0'].str.extract('((?<= score:)\S{1,})')
    numcols = ['MQAS_Score_Actual','IADV_Score_Actual','PRI_Score_Actual','Boost_Actual','Total_Score_Actual']
    attempts[numcols] = attempts[numcols].astype('float')
    attempts['Day_Type'] = cf.get_daytype(date)

    #Breakout attempts columns
    if len(orderlog) == 0:
        pass
    else:
        if environment == 'backtest':
            splits = 4
            orderlog[[0,1,'Status','SYMBOL','message']]=orderlog['0'].str.split(' ',n=splits,expand=True)
            orderlog.drop(columns=[0,1],inplace=True)
        else:
            splits = 2
            orderlog[['Status','SYMBOL','message']]=orderlog['0'].str.split(' ',n=splits,expand=True)
        orderlog['Price_Action'] = orderlog['message'].str.extract('((?<=price-action:)\S{1,})')
        orderlog['Order_Price'] = orderlog['message'].str.extract('((?<=price:)\S{1,})')
        orderlog['Order_Size'] = orderlog['message'].str.extract('((?<=size:)\S{1,})')
        orderlog['Abort_Reason'] = orderlog['message'].str.extract('((?<=reason:)\S{1,})')
        orderlog['Bid'] = orderlog['message'].str.extract('((?<=bid:)\S{1,})')
        orderlog['Ask'] = orderlog['message'].str.extract('((?<=ask:)\S{1,})')
        orderlog['Allowed_Capital_Pct'] = orderlog['message'].str.extract('((?<=allowed-capital-pct:)\S{1,})')
        orderlog = orderlog.drop_duplicates()
    combo = pd.merge(attempts, orderlog,how='outer')
    combo = combo.sort_values('Date')
    combo.to_csv(path+date+' orderlog.csv')
    combo.to_parquet(path+date+' orderlog.parquet')
    attempts.to_parquet(path+date+' attempts.parquet')


# In[1015]:


def write_exits(date,path,environment='production'):

    exits = pd.read_parquet(path+date+' exits.parquet')    
    exits = exits.reset_index(drop=True)
    exits_params = exits[exits['0'].str.contains('Starting')]
    exits = exits.iloc[exits.index.drop(exits_params.index)]
    exits = exits.reset_index(drop=True)
    if environment == 'backtest':
        splits = 4
        exits[[0,1,'Status','SYMBOL','message']]=exits['0'].str.split(' ',n=splits,expand=True)
        exits.drop(columns=[0,1],inplace=True)
    else:
        splits = 2
        exits[['Status','SYMBOL','message']]=exits['0'].str.split(' ',n=splits,expand=True)
    exits['Exit_Type'] = exits['message'].str.extract('((?<= type:)\S{1,})')
    exits['Exit_To_Cover'] = exits['message'].str.extract('((?<=-cover:)\S{1,})')
    exits['Gross_Position'] = exits['message'].str.extract('((?<=gross:)\S{1,})')
    exits['Uncovered_Position'] = exits['message'].str.extract('((?<=uncovered:)\S{1,})')
    exits['Skipped_LO_Size'] = exits['message'].str.extract('((?<=skipped-lo-size:)\S{1,})')
    exits['Unmarketable_Size'] = exits['message'].str.extract('((?<=unmarketable-size:)\S{1,})')
    exits['Unmarketable_on_Opposite_Side'] = exits['message'].str.extract('((?<=unmarketable-on-opposite-side:)\S{1,})')
    exits['Exit_Price'] = exits['message'].str.extract('((?<=price:)\S{1,})')
    exits['Exit_New_Price'] = exits['message'].str.extract('((?<=new-price:)\S{1,})')
    exits['Exit_Price'] = np.where(exits['Status']=='Exit-Repricer',exits['Exit_New_Price'],exits['Exit_Price'])
    exits = exits.sort_values('Date').reset_index(drop=True)
    fill = exits.groupby(['SYMBOL'])['Uncovered_Position'].fillna(method='ffill')
    exits['Uncovered_Position'] = np.where(exits['Exit_Type']=='LO',fill ,exits['Uncovered_Position'])            
    exits['Desired_Exit_Size'] = exits['message'].str.extract('((?<=desired-size:)\S{1,})')
    exits['Net_Exit_Size'] = exits['message'].str.extract('((?<=net-size:)\S{1,})')
    exits['Canceled_Exit_Size'] = exits['message'].str.extract('((?<=canceled-size:)\S{1,})')
    exits['Exit_Type'] = np.where(exits['Status']=='Exit-Replace-LO','LO',exits['Exit_Type'])
    exits['Exit_Type'] = np.where(exits['Status']=='Exit-Repricer','LO',exits['Exit_Type'])
    exits['Bid'] = exits['message'].str.extract('((?<=bid:)\S{1,})')
    exits['Ask'] = exits['message'].str.extract('((?<=ask:)\S{1,})')
    exits['Avg_Price'] = exits['message'].str.extract('((?<=avg-price:)\S{1,})')
    exits['ICP'] = exits['message'].str.extract('((?<=icp:)\S{1,})')
    exits['10minRef'] = exits['message'].str.extract('((?<=10minRef:)\S{1,})')
    exits['5minRef'] = exits['message'].str.extract('((?<=5minRef:)\S{1,})')
    num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
    exits[num_cols] = exits[num_cols].astype('float')
    condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LOC')
    exits['LOC_Ref_Boundary'] = np.where(condition==True,exits[['10minRef','5minRef']].min(axis=1),exits[['10minRef','5minRef']].max(axis=1))
    exits['LOC_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
    exits['LOC_Price_Ideal'] = np.where(condition==True,exits[['LOC_Cross_Price','Avg_Price']].min(axis=1),exits[['LOC_Cross_Price','Avg_Price']].max(axis=1))
    exits['LOC_Price_Calc'] = np.where(condition==True,exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].max(axis=1),exits[['LOC_Ref_Boundary','LOC_Price_Ideal']].min(axis=1))
    condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='IO')            
    exits['IO_Cross_Price'] = np.where(condition==True,exits['Bid']*.975,exits['Ask']*1.025)
    exits['IO_Price_Calc'] = np.where(condition==True,exits[['IO_Cross_Price','Avg_Price']].min(axis=1),exits[['IO_Cross_Price','Avg_Price']].max(axis=1))
    condition = (exits['Uncovered_Position']>0) & (exits['Exit_Type']=='LO')         
    exits['Midpoint_ICP_NBBO'] = np.where(condition==True,(exits['Ask']+exits['ICP'])/2,(exits['Bid']+exits['ICP'])/2)
    exits['Midpoint_ICP_AvgPrice'] = (exits['Avg_Price']+exits['ICP'])/2
    exits['LO_Price_Calc'] = np.where(condition==True,exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].min(axis=1),exits[['Midpoint_ICP_NBBO','Midpoint_ICP_AvgPrice','ICP']].max(axis=1))
    exits['LO_Price_Calc'] = round(exits['LO_Price_Calc'],2)
    num_cols = ['Exit_Price','Exit_To_Cover','Gross_Position','Uncovered_Position','Bid','Ask','Avg_Price','ICP','10minRef','5minRef'] 
    exits[num_cols] = exits[num_cols].astype('float')
    exits = exits.sort_values('Date')
    exits = exits.drop_duplicates()
    exits['Day_Type'] = cf.get_daytype(date)
    exits = exits.sort_values('Date')
    exits.to_csv(path+date+' exits.csv')
    exits.to_parquet(path+date+' exits.parquet')


# In[1016]:


def write_profittaker(date,path,environment='production'):
    profit = pd.read_parquet(path+date+' profit.parquet')

    #####Breakout profit taker columns:
    profit = profit[~profit['0'].str.contains('Cycle')]
    if len(profit.index)==0:
        print('profit file empty')
    else:
        if environment == 'backtest':
            splits=4
            profit[[0,1,'Status','SYMBOL','message']] = profit['0'].str.split(' ',n=splits,expand=True)
            profit.drop(columns=[0,1],inplace=True)
        else:
            splits = 2
            profit[['Status','SYMBOL','message']] = profit['0'].str.split(' ',n=splits,expand=True)
        profit['Price'] = profit['message'].str.extract('((?<= price:)\S{1,})').astype('float')
        profit['Bid'] = profit['message'].str.extract('((?<=bid:)\S{1,})').astype('float')
        profit['Ask'] = profit['message'].str.extract('((?<=ask:)\S{1,})').astype('float')
        profit['Uncovered_Position'] = profit['message'].str.extract('((?<=uncovered:)\S{1,})').astype('float')
        profit['10min_Last_Trade'] = profit['message'].str.extract('((?<=ten-min-last-trade:)\S{1,})').astype('float')
        profit['Price_Action_Multiple'] = profit['message'].str.extract('((?<=pa-mult:)\S{1,})').astype('float')
        profit['Side'] = profit['message'].str.extract('((?<=side:)\S{1,})')
        profit['Size'] = profit['message'].str.extract('((?<= size:)\S{1,})')
        profit = profit.sort_values(['SYMBOL','Date']).reset_index(drop=True)
        profit['Day_Type'] = cf.get_daytype(date)
        profit.to_csv(path+date+' profit.csv')
        profit.to_parquet(path+date+' profit.parquet')


# In[1017]:


def write_ope(date,path,environment='production'):
    ope = pd.read_parquet(path+date+' ope.parquet')
    ten_min = pd.read_parquet(path+date+' ten_min.parquet')

    if len(ope.index)==0:
        print('ope file empty')
    else:
        if environment == 'backtest':
            splits = 4
            ope[[0,1,'Status','SYMBOL','message']]=ope['0'].str.split(' ',n=splits,expand=True)
            ope.drop(columns=[0,1],inplace=True)
        else:
            splits = 2
            ope[['Status','SYMBOL','message']]=ope['0'].str.split(' ',n=splits,expand=True)
        ope['Uncovered_Position'] = ope['message'].str.extract('((?<=uncovered:)\S{1,})')
        ope['OPE_Price'] = ope['message'].str.extract('((?<= price:)\S{1,})').astype('float')
        ope['Bid'] = ope['message'].str.extract('((?<=bid:)\S{1,})').astype('float')
        ope['Ask'] = ope['message'].str.extract('((?<=ask:)\S{1,})').astype('float')
        # ope['10min_Ref'] = ope['message'].str.extract('((?<=10min-ref:)\S{1,})').astype('float')
        ope['Last_Ref'] = ope['message'].str.extract('((?<=last-ref:)\S{1,})').astype('float')
        ope['ICP'] = ope['message'].str.extract('((?<=icp:)\S{1,})').astype('float')
        ope['Opportunity_Cost'] = ope['message'].str.extract('((?<=opportunity-cost:)\S{1,})').astype('float')
        ope['Side'] = ope['message'].str.extract('((?<= action:)\S{1,})')
        ope['PA_Prepacket'] = ope['message'].str.extract('((?<=pa-prepacket:)\S{1,})').astype('float')
        ope['FF_Act_On_Packet'] = np.where(ope['PA_Prepacket'].notna(),1,0)
        ope['Cross_Percent'] = ope['message'].str.extract('((?<= cross-pct:)\S{1,})').astype('float')
        ope['Price_Action'] = ope['message'].str.extract('((?<= price-action:)\S{1,})').astype('float')
        ten_min[['x','y','Status','SYMBOL','10min']]=ten_min['0'].str.split(' ',n=4,expand=True)
        ten_min = ten_min[['SYMBOL','10min']]
        ten_min['10min'] = ten_min['10min'].astype('float')
        ope = pd.merge(ope,ten_min,how='left',on='SYMBOL')
        ope['Time_Period']  = np.where(ope["Date"]<date +' 19:55','Pre5','Post5')

        #ope configs
        ope_pre5_pa_prepacket_threshold= 0.0030
        ope_pre5_low_pa_cross_pct= 0.0020
        ope_pre5_high_pa_pa_cross_pct= 0.60
        ope_pre5_max_cross_pct= 0.05
        ope_pre5_order_retry_count= 2
        ope_pre5_curr_pa_threshold= -0.0115
        ope_pre5_far_cross_pct= 0.003
        ope_post5_icp_max_cross_pct= 0.01
        min_pri = 0.0020
        min_pri_multiplier = 0.25

        a1 = (ope['PA_Prepacket'].notna()) & (ope['PA_Prepacket']>ope_pre5_pa_prepacket_threshold) & (ope['Time_Period']=='Pre5')
        a2 = (ope['PA_Prepacket'].notna()) & (ope['PA_Prepacket']<ope_pre5_pa_prepacket_threshold) & (ope['Time_Period']=='Pre5')
        a3 = (ope['Price_Action'].notna()) & (ope['Price_Action']<ope_pre5_curr_pa_threshold) & (ope['Time_Period']=='Pre5')
        a4 = (ope['Price_Action'].notna()) & (ope['Price_Action']>ope_pre5_curr_pa_threshold) & (ope['Time_Period']=='Pre5')
        a5 = (ope['Time_Period']=='Post5')
        vals = ['Packet_Agressive','Packet_Soft','Reprice_Agressive','Reprice_Soft','Post5']
        ope['OPE_Price_Type'] = np.select([a1,a2,a3,a4,a5],vals,np.nan)

        ope = ope.drop_duplicates()
        ope.to_csv(path+date+' ope.csv')
        ope.to_parquet(path+date+' ope.parquet')
        ten_min.to_csv(path+date+' ten_min.csv')


# In[1018]:


def write_orders(date,path,total_capital):
    orders = pd.read_parquet(path+date+' orders.parquet')
    tr = pd.read_parquet(path+date+' tr.parquet')
    exits = pd.read_parquet(path+date+' exits.parquet')
    profit = pd.read_parquet(path+date+' profit.parquet')
    ope = pd.read_parquet(path+date+' ope.parquet')
    passed = pd.read_parquet(path+date+' passed.parquet')
    #Read in Orders Report
    orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
    orders['EntryTime_ms'] = orders['EntryTime'].dt.microsecond
    orders['LastModTime'] = pd.to_datetime(orders['LastModTime'])
    orders = orders.groupby(['OrderId']).tail(1)
    orders.rename(columns={'Symbol':'SYMBOL'},inplace=True)
    orders['QTY_Abs'] = np.absolute(orders['Quantity'])
    orders['Desired_$'] = orders.QTY_Abs * orders.Price
    orders['FilledQTY_Abs'] = np.absolute(orders['FilledQty'])
    orders['Filled$_Abs'] = orders['FilledQTY_Abs']*orders['AvgFillPrice']
    orders['Filled$'] = orders['FilledQty']*orders['AvgFillPrice']            
    orders['Fill_Rate'] = orders['FilledQTY_Abs']/orders['QTY_Abs']
    orders['DATE'] = orders['EntryTime'].dt.date
    orders['Capital_Available'] = total_capital

    #Identifying Exit orders on Orders Report 
    exitid = exits[exits['Exit_Type'].notna()]
    exitid = exitid[['SYMBOL','Exit_Type','Date']]
    exitid = exitid.sort_values('Date')
    exitid['TIF'] = np.where(exitid['Exit_Type']=='LO','DAY','AT_THE_CLOSE')
    exitid.rename(columns={'Exit_Type':'EntryType','Date':'EntryTime'},inplace=True)
    orders = orders.sort_values('EntryTime')
    orders = pd.merge_asof(orders, exitid, by=['SYMBOL','TIF'],left_on=['EntryTime'], right_on=['EntryTime'],direction='nearest',tolerance=pd.Timedelta("100ms"))     

    #Identifying Profit-Taker orders on Orders Report
    if len(profit.index) == 0:
        print('profit file is empty')
    else:
        profit['Date'] = pd.to_datetime(profit['Date'])
        profit_merge = profit[['SYMBOL','Date','Price_Action_Multiple','Status','Price']]
        profit_merge.rename(columns={'Date':'EntryTime'},inplace=True)
        profit_merge['Price'] = profit_merge['Price'].round(2)
        profit_merge = profit_merge.sort_values('EntryTime')
        orders = pd.merge_asof(orders,profit_merge,left_on=['EntryTime'],right_on=['EntryTime'],by=['SYMBOL','Price'],direction='nearest',tolerance=pd.Timedelta("1000ms"))
        orders['EntryType'] = np.where(orders['Status']=='Profit-Taker',orders['Status'],orders['EntryType'])
        orders.pop('Status')

    #Identifying types of OPE orders
    if len(ope.index)==0:
        print('ope file empty')
    else:
        ope_merge = ope[['Date','SYMBOL','OPE_Price_Type','OPE_Price']]
        ope_merge.rename(columns={'Date':'EntryTime','OPE_Price':'Price'},inplace=True)
        ope_merge = ope_merge.sort_values('EntryTime')
        ope_merge['EntryTime'] = pd.to_datetime(ope_merge['EntryTime'])
        ope_merge['Price'] = ope_merge['Price'].round(2)
        orders['EntryTime'] = pd.to_datetime(orders['EntryTime'])
        orders = pd.merge_asof(orders,ope_merge,left_on=['EntryTime'],right_on=['EntryTime'],by=['SYMBOL','Price'],direction='nearest',tolerance=pd.Timedelta("1000ms"))

    #Identifying Exit orders on Orders Report 
    orders['EntryType'] = np.where(orders['TIF']=='IOC','OPE',orders['EntryType'])
    orders['EntryType'] = np.where(orders['EntryType'].isna(),'Entry',orders['EntryType'])
    passmerge = passed.reset_index()
    passmerge = passmerge[['Date','SYMBOL','Hurdle_Set']].sort_values('Date')
    orders = pd.merge_asof(orders,passmerge,by='SYMBOL',left_on=['EntryTime'],right_on=['Date'])
    orders['Hurdle_Set'] = np.where(orders['EntryType']=='Entry',orders['Hurdle_Set'],np.nan)
    orders['TIF_in_seconds'] = (orders['LastModTime'] - orders['EntryTime']).dt.total_seconds()
    orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
    orders['Time_Diff'] = (orders['EntryTime'] - orders.groupby(['SYMBOL','Long_Short','EntryType'])['EntryTime'].shift(1)).dt.total_seconds().fillna(100)
    orders['Time_Diff_Bool'] = orders['Time_Diff']>15
    orders['Unique'] = orders.groupby(['SYMBOL'])['Time_Diff_Bool'].cumsum()
    orders['QTY_Abs_Agg'] = orders.groupby(['SYMBOL','Long_Short','EntryTime'])[['QTY_Abs']].transform('sum')
    orders['QTY_Max_Desired'] = orders.groupby(['SYMBOL','Long_Short','Unique'])[['QTY_Abs_Agg']].transform('max')
    orders['$_Max_Desired'] = orders['QTY_Max_Desired'] * orders['Price']
    x = orders.groupby(['SYMBOL','Unique'])[['SYMBOL','Unique','Price','QTY_Max_Desired','$_Max_Desired']].head(1)
    x.rename(columns={'Price':'Start_Price'},inplace=True)
    orders.drop(columns=['QTY_Max_Desired','$_Max_Desired'],inplace=True)
    orders = pd.merge(orders,x,how='left')
    a1 = (orders['Start_Price']<5)
    a2 = (orders['Start_Price'].between(5,10,inclusive='left'))
    a3 = (orders['Start_Price'].between(10,15,inclusive='left'))
    a4 = (orders['Start_Price'].between(15,30,inclusive='left'))
    a5 = (orders['Start_Price'].between(30,60,inclusive='left'))
    a6 = (orders['Start_Price']>=60)
    vals = [0,.01,.02,.03,.04,.05]
    default = 0
    orders['Cross_Amount'] = np.select([a1,a2,a3,a4,a5,a6],vals,default)
    orders['Start_NBBO'] = np.where(orders['Quantity']>0,orders['Start_Price']-orders['Cross_Amount'],orders['Start_Price']+orders['Cross_Amount'])
    orders['Start_NBBO'] = np.where(orders['AvgFillPrice']==0,np.nan,orders['Start_NBBO'])
    orders['Day_Type'] = cf.get_daytype(date)

    #Confirm no lines with unassigned EntryType
    checkx = orders[orders['EntryType']=='X']
    print('Number of unassigned order EntryTypes is {0}'.format(len(checkx))) 


    ######################
    #Read in Trade Reports
    tr.rename(columns={'Symbol':'SYMBOL'},inplace=True)
    tr['EntryTime'] = pd.to_datetime(tr['EntryTime'])
    tr = tr.sort_values('EntryTime')
    tr = tr[tr['EntryTime']>date +' '+hr+':50:00']
    tr['DATE'] = tr['EntryTime'].dt.date
    tr['Day_of_Week'] = tr['EntryTime'].dt.dayofweek
    tr['Day_Type'] = cf.get_daytype(date)
    #Create slice of orders to only look at entries
    entries = orders[orders['EntryType']=='Entry']
    #deployed_by_symbol = entries[['SYMBOL','FilledQty','AvgFillPrice','LastModTime','EntryType','Filled$_Abs','FilledQTY_Abs','QTY_Abs','Capital_Available','Desired_$']].sort_values('LastModTime')
    #deployed_by_symbol.rename(columns={'FilledQty':'Quantity'},inplace=True)
    #tr = pd.merge_asof(tr,deployed_by_symbol,by=['SYMBOL','Quantity'],left_on=['EntryTime'],right_on=['LastModTime'],direction='nearest')
    tr['FilledQTY_Abs'] = np.absolute(tr['Quantity'])
    tr['Filled$_Abs'] = tr['FilledQTY_Abs'] * tr['EntryPrice']
    tr['Stuck'] = np.where(tr['ExitTime']=='1970-Jan-01 00:00:00','Stuck','Exited')
    cols = ['Quantity','RealizedPL','Filled$_Abs']
    tr[cols] = tr[cols].astype('float')
    trinst = tr.copy()
    x = tr.groupby(['SYMBOL'])[['Quantity','RealizedPL','Filled$_Abs']].sum().reset_index()
    tr.drop(columns=['Quantity','RealizedPL','Filled$_Abs'],inplace=True)
    tr = pd.merge(tr,x,how='left')
    tr.drop_duplicates(['SYMBOL'],inplace=True)
    tr['Basis_Return'] = tr['RealizedPL']/tr['Filled$_Abs']
    #Creating Trade Reports by 
    trinst['Basis_Return'] = trinst['RealizedPL']/trinst['Filled$_Abs']
    fortrinst = passed[['SYMBOL','MQ','ARTI','IADV','MQAS','MQAS_Score','IADV_Score','Per_Return_Exp','PRI_Score','Total_Score','ADV','Flips','direction_from_10','Notional_Auction_Size','matched_quantity','min-pri','Hurdle_Set']].reset_index()
    fortrinst['Date'] = fortrinst['Date'].dt.tz_localize(None)
    fortrinst = fortrinst.sort_values(['SYMBOL','Date'])
    fortrinst['Total_Score'] = fortrinst['Total_Score'].replace(0,np.nan)
    fortrinst = fortrinst.sort_values('Date')
    fortrinst['SYMBOLx'] = fortrinst['SYMBOL']
    fortrinst = fortrinst.groupby(['SYMBOLx']).fillna(method='bfill')
    trinst = pd.merge_asof(trinst,fortrinst,by=['SYMBOL'],left_on='EntryTime',right_on='Date',direction='nearest',tolerance=pd.Timedelta("1000ms"))
    #trinst['ADV'] = trinst['ARTI'] / trinst['IADV']
    a1 = (trinst['Total_Score']>100)
    a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
    a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
    a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
    a5 = (trinst['Total_Score'].between(0,20))
    vals = ['A','B','C','D','E']
    default = 'D'
    trinst['Score_Bracket'] = np.select([a1,a2,a3,a4,a5],vals,default=default)

    cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
    trinst['ADV_Even_Brackets'] = pd.cut(trinst['ADV'], bins=cuts,duplicates='drop')
    trinst['MQ_Brackets'] = pd.cut(trinst['MQ'], bins=cuts,duplicates='drop')
    trinst['Imb_Brackets'] = pd.cut(trinst['ARTI'], bins=cuts,duplicates='drop')
    cuts = [-np.inf,0.00001,.1,.2,.3,.4,.5,.6,.7,.8,.9,1,2,5,np.inf]
    trinst['IADV_Even_Extended_Brackets'] = pd.cut(trinst['IADV'],bins=cuts)
    cuts = [-np.inf,0,.1,.2,.3,.4,.5,.6,.7,.8,.9,1]
    trinst['MQAS_Brackets'] = pd.cut(trinst['MQAS'], bins=cuts,duplicates='drop')
    cuts = [-np.inf,0,5,10,15,30,60,np.inf]
    trinst['Price_Brackets'] = pd.cut(trinst['EntryPrice'], bins=cuts)
    a1 = (trinst['EntryTime']>=(date + ' ' +hr + ':00')) & (trinst['EntryTime']<(date + ' ' +hr + ':55'))
    a2 = (trinst['EntryTime']>=(date + ' ' +hr + ':55')) & (trinst['EntryTime']<(date + ' ' +hr + ':58'))
    a3 = (trinst['EntryTime']>=(date + ' ' +hr + ':58'))
    vals = ['Entry50', 'Entry55', 'Entry58']
    trinst['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')

    fortrinst.pop('Hurdle_Set')
    orders = pd.merge_asof(orders,fortrinst,by=['SYMBOL'], left_on='EntryTime', right_on='Date',direction='nearest',tolerance=pd.Timedelta("1000ms"))
    a1 = (orders['Total_Score']>100)
    a2 = (orders['Total_Score'].between(75,100,inclusive='left'))
    a3 = (orders['Total_Score'].between(50,75,inclusive='left'))
    a4 = (orders['Total_Score'].between(20,50,inclusive='left'))
    a5 = (orders['Total_Score'].between(0,20))
    vals = ['A','B','C','D','E']
    default = 'D'
    orders['Score_Bracket'] = np.select([a1,a2,a3,a4,a5],vals,default=default)
    orders['ADV'] = orders['ARTI'] / orders['IADV']
    cuts = [0,100000,200000,500000,1000000,2000000,5000000,np.inf]
    orders['ADV_Even_Brackets'] = pd.cut(orders['ADV'], bins=cuts,duplicates='drop')
    #Adding EntryTiming
    a1 = (orders['EntryTime']>=(date + ' ' +hr + ':00')) & (orders['EntryTime']<(date + ' ' +hr + ':55'))
    a2 = (orders['EntryTime']>=(date + ' ' +hr + ':55')) & (orders['EntryTime']<(date + ' ' +hr + ':58'))
    a3 = (orders['EntryTime']>=(date + ' ' +hr + ':58'))
    vals = ['Entry50', 'Entry55', 'Entry58']
    orders['EntryTiming'] = np.select([a1,a2,a3],vals,default='X')
    orders['Long_Short'] = np.where(orders['Quantity']>0,'Long','Short')
    #Adding columns for price slippage

    orders['Slippage_$'] = np.where(orders['Quantity']>0,orders['Start_NBBO']-orders['AvgFillPrice'],orders['AvgFillPrice']-orders['Start_NBBO'])
    orders['Slipage_%'] = orders['Slippage_$']/orders['Start_NBBO']
    orders['Weighted_Price'] = orders['$_Max_Desired']*orders['Start_NBBO']
    orders['Slippage_$$, $Weighted'] = orders['$_Max_Desired']*orders['Slippage_$']
    orders['Weighted_Shares'] = orders['QTY_Max_Desired']*orders['Start_NBBO']
    orders['Slippage_$,Shares Weighted'] = orders['QTY_Max_Desired']*orders['Slippage_$']
    orders['Percent_of_Imbalance'] = orders['FilledQTY_Abs']/orders['ARTI']
    orders['Percent_of_Capital'] = orders['Filled$_Abs']/orders['Capital_Available']
    orders['on_packet'] = orders['EntryTime_ms']!=500000


    stuckpnlfile = date + ' suckpnl.csv'
    if stuckpnlfile in os.listdir('.'):
        stuckpnl = pd.read_csv(date +' stuckpnl.csv')
        stuckpnl.rename(columns={'Symbol':'SYMBOL'},inplace=True)
        trinst['Percent_of_Total'] = trinst.groupby('SYMBOL')['Quantity'].apply(lambda column: column/column.sum())
        trinst = pd.merge(trinst,stuckpnl,how='left')
        trinst['Net'] = trinst['Net'].fillna(0)
        trinst['StuckPNL'] = trinst['Percent_of_Total']*trinst['Net']
        trinst['Adj_RealizedPL'] = trinst['RealizedPL'] + trinst['StuckPNL']
    else:
        print('no stuckpnl file')

#     #Combining attemps-df and orderlog-df
#     combo = pd.merge(attempts, orderlog,how='outer')
#     combo = combo.sort_values('Date')
#     alllogs = pd.concat([orderlog, attempts, exits,ope,closeout,passed.reset_index()],keys=['orderlog', 'attempts', 'exits','ope','closeout','passed']).reset_index()


    scores = passed[passed['Total_Score'].notna()].reset_index()
    scores = scores[['Date','SYMBOL','MQAS_Score','IADV_Score','PRI_Score','Boost','Total_Score']]
    scores['Date'] = pd.to_datetime(scores['Date'])
    trinst['EntryTime'] = pd.to_datetime(trinst['EntryTime'])
    trinst.drop(columns=['MQAS_Score','IADV_Score','PRI_Score','Total_Score'],inplace=True)
    trinst = pd.merge_asof(trinst,scores,left_on=['EntryTime'],right_on=['Date'],by='SYMBOL',direction='nearest')
    a1 = (trinst['Total_Score']>100)
    a2 = (trinst['Total_Score'].between(75,100,inclusive='left'))
    a3 = (trinst['Total_Score'].between(50,75,inclusive='left'))
    a4 = (trinst['Total_Score'].between(20,50,inclusive='left'))
    a5 = (trinst['Total_Score'].between(0,20))
    vals = ['A','B','C','D','E']
    default = 'D'
    trinst['Score_Bracket'] = np.select([a1,a2,a3,a4,a5],vals,default=default)
    
    tr.to_csv(path + date + ' tradereports.csv')
    trinst.to_csv(path + date + ' tr_byinstance.csv')
    orders.to_csv(path + date +' orders.csv')


# In[1019]:


def check_liquid_6M_55m_pri_hurdle_updated_outdated(df):
    rd = pd.read_csv('read_hurdles_nqc_6M_55m_updated_wquadandruss_nointersecondsmoothing_20220728_15bps.csv')
    rd.columns = rd.columns.str.replace(' ','')
    rd[['fpa_low','IADV_Clean_BracketsLow']] = rd[['fpa_low','IADV_Clean_BracketsLow']].replace(-1,'-np.inf')
    rd[['fpa_low','IADV_Clean_BracketsLow']] = rd[['fpa_low','IADV_Clean_BracketsLow']].replace('#NAME?','-np.inf')

    cols = ['is_Flip', 'fpa_Brackets', 'fpa_low', 'fpa_high',
           'IADV_Clean_Brackets', 'IADV_Clean_BracketsLow',
           'IADV_Clean_BracketsHigh', 'hurdle']
    rd[cols] = rd[cols].astype(str)
    rd[['fpa_high','IADV_Clean_BracketsHigh']] = rd[['fpa_high','IADV_Clean_BracketsHigh']].replace('inf','np.inf')
    rd['Time2'] = rd.groupby('Features')['Time'].shift(-1).fillna('20:00:00')
    stat1 = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index))"
    stat2 = "(df['is_Flip']=='"+rd['is_Flip']+"')"
    stat3 = "(df['direction_from_10'].between("+rd['fpa_low']+","+rd['fpa_high']+"))"
    stat4 = "(df['IADV'].between("+rd['IADV_Clean_BracketsLow']+","+rd['IADV_Clean_BracketsHigh']+"))"
    stat5 = "(df['Per_Return_Exp']>="+rd['hurdle']+")"
    rd['stat'] = stat1 + "&" + stat2 + "&" + stat3 + "&" + stat4
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    rd['value'] = True
    vals = rd['hurdle'].astype('float').to_list()
    a0=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a2=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a3=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a4=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a5=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a6=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a7=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a8=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a9=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a10=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a11=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a12=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a13=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a14=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a15=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a16=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a17=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a18=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a19=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a20=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a21=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a22=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a23=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a24=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a25=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a26=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a27=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a28=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a29=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a30=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a31=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a32=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a33=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a34=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a35=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a36=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a37=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a38=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a39=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a40=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a41=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a42=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a43=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a44=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a45=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a46=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a47=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a48=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a49=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a50=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a51=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a52=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a53=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a54=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a55=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a56=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a57=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a58=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a59=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a60=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a61=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a62=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a63=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a64=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a65=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a66=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a67=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a68=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a69=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a70=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a71=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a72=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a73=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a74=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a75=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a76=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a77=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a78=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a79=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a80=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a81=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a82=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a83=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a84=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a85=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a86=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a87=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a88=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a89=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a90=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a91=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a92=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a93=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a94=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a95=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a96=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a97=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a98=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a99=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a100=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a101=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a102=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a103=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a104=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a105=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a106=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a107=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a108=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a109=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a110=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a111=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a112=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a113=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a114=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a115=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a116=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a117=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a118=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a119=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a120=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a121=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a122=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a123=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a124=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a125=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a126=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a127=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a128=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a129=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a130=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a131=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a132=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a133=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a134=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a135=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a136=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a137=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a138=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a139=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a140=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a141=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a142=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a143=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a144=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a145=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a146=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a147=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a148=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a149=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a150=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a151=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a152=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a153=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a154=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a155=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a156=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a157=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a158=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a159=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a160=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a161=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a162=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a163=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a164=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a165=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a166=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a167=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a168=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a169=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a170=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a171=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a172=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a173=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a174=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a175=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a176=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a177=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a178=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a179=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a180=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a181=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a182=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a183=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a184=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a185=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a186=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a187=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a188=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a189=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a190=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a191=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a192=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a193=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a194=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a195=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a196=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a197=(df.index.isin(df.between_time('19:55:00','19:55:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a198=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a199=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a200=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a201=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a202=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a203=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a204=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a205=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a206=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a207=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a208=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a209=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a210=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a211=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a212=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a213=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a214=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a215=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a216=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a217=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a218=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a219=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a220=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a221=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a222=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a223=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a224=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a225=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a226=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a227=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a228=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a229=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a230=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a231=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a232=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a233=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a234=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a235=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a236=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a237=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a238=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a239=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a240=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a241=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a242=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a243=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a244=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a245=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a246=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a247=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a248=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a249=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a250=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a251=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a252=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a253=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a254=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a255=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a256=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a257=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a258=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a259=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a260=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a261=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a262=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a263=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a264=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a265=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a266=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a267=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a268=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a269=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a270=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a271=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a272=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a273=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a274=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a275=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a276=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a277=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a278=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a279=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a280=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a281=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a282=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a283=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a284=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a285=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a286=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a287=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a288=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a289=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a290=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a291=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a292=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a293=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a294=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a295=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a296=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a297=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a298=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a299=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a300=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a301=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a302=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a303=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a304=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a305=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a306=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a307=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a308=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a309=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a310=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a311=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a312=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a313=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a314=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a315=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a316=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a317=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a318=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a319=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a320=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a321=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a322=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a323=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a324=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a325=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a326=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a327=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a328=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a329=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a330=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a331=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a332=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a333=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a334=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a335=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a336=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a337=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a338=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a339=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a340=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a341=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a342=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a343=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a344=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a345=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a346=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a347=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a348=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a349=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a350=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a351=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a352=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a353=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a354=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a355=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a356=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a357=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a358=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a359=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a360=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a361=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a362=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a363=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a364=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a365=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a366=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a367=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a368=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a369=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a370=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a371=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a372=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a373=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a374=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a375=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a376=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a377=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a378=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a379=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a380=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a381=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a382=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a383=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a384=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a385=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a386=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a387=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a388=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a389=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a390=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a391=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a392=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a393=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a394=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a395=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a396=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a397=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a398=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a399=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a400=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a401=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a402=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a403=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a404=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a405=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a406=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a407=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a408=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a409=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a410=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a411=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a412=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a413=(df.index.isin(df.between_time('19:56:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a414=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a415=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a416=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a417=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a418=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a419=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a420=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a421=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a422=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a423=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a424=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a425=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a426=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a427=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a428=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a429=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a430=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a431=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a432=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a433=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a434=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a435=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a436=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a437=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a438=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a439=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a440=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a441=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a442=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a443=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a444=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a445=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a446=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a447=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a448=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a449=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a450=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a451=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a452=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a453=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a454=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a455=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a456=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a457=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a458=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a459=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a460=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a461=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a462=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a463=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a464=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a465=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a466=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a467=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a468=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a469=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a470=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a471=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a472=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a473=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a474=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a475=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a476=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a477=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a478=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a479=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a480=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a481=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a482=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a483=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a484=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a485=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a486=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a487=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a488=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a489=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a490=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a491=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a492=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a493=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a494=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a495=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a496=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a497=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a498=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a499=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a500=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a501=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a502=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a503=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a504=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a505=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a506=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a507=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a508=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a509=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a510=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a511=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a512=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a513=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a514=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a515=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a516=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a517=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a518=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a519=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a520=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a521=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a522=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a523=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a524=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a525=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a526=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a527=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a528=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a529=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a530=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a531=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a532=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a533=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a534=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a535=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a536=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a537=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a538=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a539=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a540=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a541=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a542=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a543=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a544=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a545=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a546=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a547=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a548=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a549=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a550=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a551=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a552=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a553=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a554=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a555=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a556=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a557=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a558=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a559=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a560=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a561=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a562=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a563=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a564=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a565=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a566=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a567=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a568=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a569=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a570=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a571=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a572=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a573=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a574=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a575=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a576=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a577=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a578=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a579=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a580=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a581=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a582=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a583=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a584=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a585=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a586=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a587=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a588=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a589=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a590=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a591=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a592=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a593=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a594=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a595=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a596=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a597=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a598=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a599=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a600=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a601=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a602=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a603=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a604=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a605=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a606=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a607=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a608=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a609=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a610=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a611=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a612=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a613=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a614=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a615=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a616=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a617=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a618=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a619=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a620=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a621=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a622=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a623=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a624=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a625=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a626=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a627=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a628=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a629=(df.index.isin(df.between_time('19:57:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a630=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a631=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a632=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a633=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a634=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a635=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a636=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a637=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a638=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a639=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a640=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a641=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a642=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a643=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a644=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a645=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a646=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a647=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a648=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a649=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a650=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a651=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a652=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a653=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a654=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a655=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a656=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a657=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a658=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a659=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a660=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a661=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a662=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a663=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a664=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a665=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a666=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a667=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a668=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a669=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a670=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a671=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a672=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a673=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a674=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a675=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a676=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a677=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a678=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a679=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a680=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a681=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a682=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a683=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a684=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a685=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a686=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a687=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a688=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a689=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a690=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a691=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a692=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a693=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a694=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a695=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a696=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a697=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a698=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a699=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a700=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a701=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a702=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a703=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a704=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a705=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a706=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a707=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a708=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a709=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a710=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a711=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a712=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a713=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a714=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a715=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a716=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a717=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a718=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a719=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a720=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a721=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a722=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a723=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a724=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a725=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a726=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a727=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a728=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a729=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a730=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a731=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a732=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a733=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a734=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a735=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a736=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a737=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a738=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a739=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a740=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a741=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a742=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a743=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a744=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a745=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a746=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a747=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a748=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a749=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a750=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a751=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a752=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a753=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a754=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a755=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a756=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a757=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a758=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a759=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a760=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a761=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a762=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a763=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a764=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a765=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a766=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a767=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a768=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a769=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a770=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a771=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a772=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a773=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a774=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a775=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a776=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a777=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a778=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a779=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a780=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a781=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a782=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a783=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a784=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a785=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a786=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a787=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a788=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a789=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a790=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a791=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a792=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a793=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a794=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a795=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a796=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a797=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a798=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a799=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a800=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a801=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a802=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a803=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a804=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a805=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a806=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a807=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a808=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a809=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a810=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a811=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a812=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a813=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a814=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a815=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a816=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a817=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a818=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a819=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a820=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a821=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a822=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a823=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a824=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a825=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a826=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a827=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a828=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a829=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a830=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a831=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a832=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a833=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a834=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a835=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a836=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a837=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a838=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a839=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a840=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a841=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a842=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a843=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a844=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a845=(df.index.isin(df.between_time('19:58:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a846=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a847=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a848=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a849=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a850=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a851=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a852=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a853=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a854=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a855=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a856=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a857=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a858=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a859=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a860=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a861=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a862=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a863=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a864=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a865=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a866=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a867=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a868=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a869=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a870=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a871=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a872=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a873=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a874=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a875=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a876=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a877=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a878=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a879=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a880=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a881=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a882=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a883=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a884=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a885=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a886=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a887=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a888=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a889=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a890=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a891=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a892=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a893=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a894=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a895=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a896=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a897=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a898=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a899=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a900=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a901=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a902=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a903=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a904=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a905=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a906=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a907=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a908=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a909=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a910=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a911=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a912=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a913=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a914=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a915=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a916=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a917=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a918=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a919=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a920=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a921=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a922=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a923=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a924=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a925=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a926=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a927=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a928=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a929=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a930=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a931=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a932=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a933=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a934=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a935=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a936=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a937=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a938=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a939=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a940=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a941=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a942=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a943=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a944=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a945=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a946=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a947=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a948=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a949=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a950=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a951=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a952=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a953=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a954=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a955=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a956=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a957=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a958=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a959=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a960=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a961=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a962=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a963=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a964=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a965=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a966=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a967=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a968=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a969=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a970=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a971=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a972=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a973=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a974=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a975=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a976=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a977=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a978=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a979=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a980=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a981=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a982=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a983=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a984=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a985=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a986=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a987=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a988=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a989=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a990=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a991=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a992=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a993=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a994=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a995=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a996=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a997=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a998=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a999=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1000=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1001=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1002=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1003=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1004=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1005=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1006=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1007=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1008=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1009=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1010=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1011=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1012=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1013=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1014=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1015=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1016=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1017=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1018=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1019=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1020=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1021=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1022=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1023=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1024=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1025=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1026=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1027=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1028=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1029=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1030=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1031=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1032=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1033=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1034=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1035=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1036=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1037=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1038=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1039=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1040=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1041=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1042=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1043=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1044=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1045=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1046=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1047=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1048=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1049=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1050=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1051=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1052=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1053=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1054=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1055=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1056=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1057=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1058=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1059=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1060=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1061=(df.index.isin(df.between_time('19:59:00','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1062=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1063=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1064=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1065=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1066=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1067=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1068=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1069=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1070=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a1071=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1072=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a1073=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1074=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a1075=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a1076=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a1077=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a1078=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a1079=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,
    a31,a32,a33,a34,
a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,
a62,a63,a64,a65,a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87,a88,a89,a90,a91,a92,a93,a94,a95,a96,a97,a98,a99,a100,
a101,a102,a103,a104,a105,a106,a107,a108,a109,a110,a111,a112,a113,a114,a115,a116,a117,a118,a119,a120,a121,a122,a123,a124,a125,a126,a127,a128,a129,a130,a131,a132,a133,a134,a135,a136,a137,a138,a139,a140,a141,a142,a143,a144,a145,a146,a147,a148,a149,a150,a151,a152,a153,a154,a155,a156,a157,a158,a159,a160,a161,a162,a163,a164,a165,a166,a167,a168,a169,a170,a171,a172,a173,a174,a175,a176,a177,a178,a179,a180,a181,a182,a183,a184,a185,a186,a187,a188,a189,a190,a191,a192,a193,a194,a195,a196,a197,a198,a199,a200,
a201,a202,a203,a204,a205,a206,a207,a208,a209,a210,a211,a212,a213,a214,a215,a216,a217,a218,a219,a220,a221,a222,a223,a224,a225,a226,a227,a228,a229,a230,a231,a232,a233,a234,a235,a236,a237,a238,a239,a240,a241,a242,a243,a244,a245,a246,a247,a248,a249,a250,a251,a252,a253,a254,a255,a256,
a257,a258,a259,a260,a261,a262,a263,a264,a265,a266,a267,a268,a269,a270,a271,a272,a273,a274,a275,a276,a277,a278,a279,a280,a281,a282,a283,a284,a285,a286,a287,a288,a289,a290,a291,a292,a293,a294,a295,a296,a297,a298,
a299,a300,a301,a302,a303,a304,a305,a306,a307,a308,a309,a310,a311,a312,a313,a314,a315,a316,a317,a318,a319,a320,a321,a322,a323,a324,a325,a326,a327,a328,a329,a330,a331,a332,a333,a334,a335,a336,a337,a338,a339,a340,a341,a342,a343,a344,a345,a346,a347,a348,a349,a350,
a351,a352,a353,a354,a355,a356,a357,a358,a359,a360,a361,a362,a363,a364,a365,a366,a367,a368,a369,a370,a371,a372,a373,a374,a375,a376,a377,a378,a379,a380,a381,a382,a383,a384,a385,a386,a387,a388,a389,a390,a391,a392,a393,a394,a395,a396,a397,a398,a399,a400,a401,
a402,a403,a404,a405,a406,a407,a408,a409,a410,a411,a412,a413,a414,a415,a416,a417,a418,a419,a420,a421,a422,a423,a424,a425,a426,a427,a428,a429,a430,a431,a432,a433,a434,a435,a436,a437,a438,a439,a440,a441,a442,a443,a444,a445,a446,a447,a448,a449,a450,a451,
a452,a453,a454,a455,a456,a457,a458,a459,a460,a461,a462,a463,a464,a465,a466,a467,a468,a469,a470,a471,a472,a473,a474,a475,a476,a477,a478,a479,a480,a481,a482,a483,a484,a485,a486,a487,a488,a489,a490,a491,a492,a493,a494,a495,a496,a497,a498,a499,a500,a501,
a502,a503,a504,a505,a506,a507,a508,a509,a510,a511,a512,a513,a514,a515,a516,a517,a518,a519,a520,a521,a522,a523,a524,a525,a526,a527,a528,a529,a530,a531,a532,a533,a534,a535,a536,a537,a538,a539,a540,a541,a542,a543,a544,a545,a546,a547,a548,a549,a550,a551,
a552,a553,a554,a555,a556,a557,a558,a559,a560,a561,a562,a563,a564,a565,a566,a567,a568,a569,a570,a571,a572,a573,a574,a575,a576,a577,a578,a579,a580,a581,a582,a583,a584,a585,a586,a587,a588,a589,a590,a591,a592,a593,a594,a595,a596,a597,a598,a599,a600,a601,
a602,
a603,
a604,
a605,
a606,
a607,
a608,
a609,
a610,
a611,
a612,
a613,
a614,
a615,
a616,
a617,
a618,
a619,
a620,
a621,
a622,
a623,
a624,
a625,
a626,
a627,
a628,
a629,
a630,
a631,
a632,
a633,
a634,
a635,
a636,
a637,
a638,
a639,
a640,
a641,
a642,
a643,
a644,
a645,
a646,
a647,
a648,
a649,
a650,
a651,
a652,
a653,
a654,
a655,
a656,
a657,
a658,
a659,
a660,
a661,
a662,
a663,
a664,
a665,
a666,
a667,
a668,
a669,
a670,
a671,
a672,
a673,
a674,
a675,
a676,
a677,
a678,
a679,
a680,
a681,
a682,
a683,
a684,
a685,
a686,
a687,
a688,
a689,
a690,
a691,
a692,
a693,
a694,
a695,
a696,
a697,
a698,
a699,
a700,
a701,
a702,
a703,
a704,
a705,
a706,
a707,
a708,
a709,
a710,
a711,
a712,
a713,
a714,
a715,
a716,
a717,
a718,
a719,
a720,
a721,
a722,
a723,
a724,
a725,
a726,
a727,
a728,
a729,
a730,
a731,
a732,
a733,
a734,
a735,
a736,
a737,
a738,
a739,
a740,
a741,
a742,
a743,
a744,
a745,
a746,
a747,
a748,
a749,
a750,
a751,
a752,
a753,
a754,
a755,
a756,
a757,
a758,
a759,
a760,
a761,
a762,
a763,
a764,
a765,
a766,
a767,
a768,
a769,
a770,
a771,
a772,
a773,
a774,
a775,
a776,
a777,
a778,
a779,
a780,
a781,
a782,
a783,
a784,
a785,
a786,
a787,
a788,
a789,
a790,
a791,
a792,
a793,
a794,
a795,
a796,
a797,
a798,
a799,
a800,
a801,
a802,
a803,
a804,
a805,
a806,
a807,
a808,
a809,
a810,
a811,
a812,
a813,
a814,
a815,
a816,
a817,
a818,
a819,
a820,
a821,
a822,
a823,
a824,
a825,
a826,
a827,
a828,
a829,
a830,
a831,
a832,
a833,
a834,
a835,
a836,
a837,
a838,
a839,
a840,
a841,
a842,
a843,
a844,
a845,
a846,
a847,
a848,
a849,
a850,
a851,
a852,
a853,
a854,
a855,
a856,
a857,
a858,
a859,
a860,
a861,
a862,
a863,
a864,
a865,
a866,
a867,
a868,
a869,
a870,
a871,
a872,
a873,
a874,
a875,
a876,
a877,
a878,
a879,
a880,
a881,
a882,
a883,
a884,
a885,
a886,
a887,
a888,
a889,
a890,
a891,
a892,
a893,
a894,
a895,
a896,
a897,
a898,
a899,
a900,
a901,
a902,
a903,
a904,
a905,
a906,
a907,
a908,
a909,
a910,
a911,
a912,
a913,
a914,
a915,
a916,
a917,
a918,
a919,
a920,
a921,
a922,
a923,
a924,
a925,
a926,
a927,
a928,
a929,
a930,
a931,
a932,
a933,
a934,
a935,
a936,
a937,
a938,
a939,
a940,
a941,
a942,
a943,
a944,
a945,
a946,
a947,
a948,
a949,
a950,
a951,
a952,
a953,
a954,
a955,
a956,
a957,
a958,
a959,
a960,
a961,
a962,
a963,
a964,
a965,
a966,
a967,
a968,
a969,
a970,
a971,
a972,
a973,
a974,
a975,
a976,
a977,
a978,
a979,
a980,
a981,
a982,
a983,
a984,
a985,
a986,
a987,
a988,
a989,
a990,
a991,
a992,
a993,
a994,
a995,
a996,
a997,
a998,
a999,
a1000,
a1001,
a1002,
a1003,
a1004,
a1005,
a1006,
a1007,
a1008,
a1009,
a1010,
a1011,
a1012,
a1013,
a1014,
a1015,
a1016,
a1017,
a1018,
a1019,
a1020,
a1021,
a1022,
a1023,
a1024,
a1025,
a1026,
a1027,
a1028,
a1029,
a1030,
a1031,
a1032,
a1033,
a1034,
a1035,
a1036,
a1037,
a1038,
a1039,
a1040,
a1041,
a1042,
a1043,
a1044,
a1045,
a1046,
a1047,
a1048,
a1049,
a1050,
a1051,
a1052,
a1053,
a1054,
a1055,
a1056,
a1057,
a1058,
a1059,
a1060,
a1061,
a1062,
a1063,
a1064,
a1065,
a1066,
a1067,
a1068,
a1069,
a1070,
a1071,
a1072,
a1073,
a1074,
a1075,
a1076,
a1077,
a1078,
a1079
]
        
    df['Liquid_6M_Hurdles_Updated'] = np.select(statements,vals,15)
    df['Over_Liquid_6M_Hurdles'] = (df['Per_Return_Exp'] > df['Liquid_6M_Hurdles_Updated']) & (df['ADV'].between(6000000,25000000))


# In[1020]:


def check_liquid_6M_55m_pri_hurdle_updated(df):
    truncated = True
    rd = pd.read_csv('read_hurdles_nqc_6M_55m_updated_20220819_20bps.csv')
    rd.columns = rd.columns.str.replace(' ','')
    rd[['fpa_low','IADV_Clean_BracketsLow']] = rd[['fpa_low','IADV_Clean_BracketsLow']].replace(-1,'-np.inf')
    rd[['fpa_low','IADV_Clean_BracketsLow']] = rd[['fpa_low','IADV_Clean_BracketsLow']].replace('#NAME?','-np.inf')

    cols = ['is_Flip', 'fpa_Brackets', 'fpa_low', 'fpa_high',
           'IADV_Clean_Brackets', 'IADV_Clean_BracketsLow',
           'IADV_Clean_BracketsHigh', 'hurdle']
    rd[cols] = rd[cols].astype(str)
    rd[['fpa_high','IADV_Clean_BracketsHigh']] = rd[['fpa_high','IADV_Clean_BracketsHigh']].replace('inf','np.inf')
    rd['Previous'] = rd.groupby(cols[:-1])['hurdle'].shift(1)
    rd['Same'] = rd['hurdle'] == rd['Previous']
    if truncated == True:
        rd = rd[rd['Same']==False]
    rd['Time2'] = rd.groupby('Features')['Time'].shift(-1).fillna('20:00:00')
    stat1 = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index))"
    stat2 = "(df['is_Flip']=='"+rd['is_Flip']+"')"
    stat3 = "(df['direction_from_10'].between("+rd['fpa_low']+","+rd['fpa_high']+"))"
    stat4 = "(df['IADV'].between("+rd['IADV_Clean_BracketsLow']+","+rd['IADV_Clean_BracketsHigh']+"))"
    stat5 = "(df['Per_Return_Exp']>="+rd['hurdle']+")"
    rd['stat'] = stat1 + "&" + stat2 + "&" + stat3 + "&" + stat4
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    rd['value'] = True
    vals = rd['hurdle'].astype('float').to_list()
    a0=(df.index.isin(df.between_time('19:55:00','19:55:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a1=(df.index.isin(df.between_time('19:55:00','19:56:40', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a2=(df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a3=(df.index.isin(df.between_time('19:55:00','19:56:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a4=(df.index.isin(df.between_time('19:55:00','19:57:10', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a5=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a6=(df.index.isin(df.between_time('19:55:00','19:56:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a7=(df.index.isin(df.between_time('19:55:00','19:55:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a8=(df.index.isin(df.between_time('19:55:00','19:55:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a9=(df.index.isin(df.between_time('19:55:00','19:57:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a10=(df.index.isin(df.between_time('19:55:00','19:55:05', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a11=(df.index.isin(df.between_time('19:55:00','19:57:10', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a12=(df.index.isin(df.between_time('19:55:00','19:55:10', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a13=(df.index.isin(df.between_time('19:55:00','19:57:20', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a14=(df.index.isin(df.between_time('19:55:00','19:58:05', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a15=(df.index.isin(df.between_time('19:55:00','19:56:10', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a16=(df.index.isin(df.between_time('19:55:00','19:56:05', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a17=(df.index.isin(df.between_time('19:55:00','19:56:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a18=(df.index.isin(df.between_time('19:55:05','19:57:50', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a19=(df.index.isin(df.between_time('19:55:05','19:57:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a20=(df.index.isin(df.between_time('19:55:10','19:57:20', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a21=(df.index.isin(df.between_time('19:55:55','19:57:10', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a22=(df.index.isin(df.between_time('19:55:55','19:57:10', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a23=(df.index.isin(df.between_time('19:56:05','19:57:45', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a24=(df.index.isin(df.between_time('19:56:05','19:57:10', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a25=(df.index.isin(df.between_time('19:56:05','19:57:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a26=(df.index.isin(df.between_time('19:56:10','19:57:40', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a27=(df.index.isin(df.between_time('19:56:35','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a28=(df.index.isin(df.between_time('19:56:40','19:56:45', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a29=(df.index.isin(df.between_time('19:56:45','19:57:50', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a30=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a31=(df.index.isin(df.between_time('19:57:00','19:57:15', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a32=(df.index.isin(df.between_time('19:57:10','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a33=(df.index.isin(df.between_time('19:57:10','19:57:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a34=(df.index.isin(df.between_time('19:57:10','19:57:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a35=(df.index.isin(df.between_time('19:57:10','19:57:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a36=(df.index.isin(df.between_time('19:57:10','19:57:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a37=(df.index.isin(df.between_time('19:57:15','19:57:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a38=(df.index.isin(df.between_time('19:57:20','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a39=(df.index.isin(df.between_time('19:57:20','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a40=(df.index.isin(df.between_time('19:57:30','19:57:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a41=(df.index.isin(df.between_time('19:57:30','19:57:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a42=(df.index.isin(df.between_time('19:57:30','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a43=(df.index.isin(df.between_time('19:57:35','19:57:45', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a44=(df.index.isin(df.between_time('19:57:35','19:57:45', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a45=(df.index.isin(df.between_time('19:57:35','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a46=(df.index.isin(df.between_time('19:57:35','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a47=(df.index.isin(df.between_time('19:57:35','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a48=(df.index.isin(df.between_time('19:57:35','19:57:40', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a49=(df.index.isin(df.between_time('19:57:40','19:57:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a50=(df.index.isin(df.between_time('19:57:40','19:57:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a51=(df.index.isin(df.between_time('19:57:45','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a52=(df.index.isin(df.between_time('19:57:45','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a53=(df.index.isin(df.between_time('19:57:45','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a54=(df.index.isin(df.between_time('19:57:45','19:57:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a55=(df.index.isin(df.between_time('19:57:45','19:57:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a56=(df.index.isin(df.between_time('19:57:45','19:57:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a57=(df.index.isin(df.between_time('19:57:45','19:57:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a58=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a59=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a60=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a61=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a62=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a63=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a64=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a65=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a66=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a67=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a68=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a69=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a70=(df.index.isin(df.between_time('19:57:55','19:58:40', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a71=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a72=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a73=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a74=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a75=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a76=(df.index.isin(df.between_time('19:57:55','19:59:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a77=(df.index.isin(df.between_time('19:57:55','19:59:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a78=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a79=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a80=(df.index.isin(df.between_time('19:57:55','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a81=(df.index.isin(df.between_time('19:58:00','19:58:10', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a82=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a83=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a84=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a85=(df.index.isin(df.between_time('19:58:00','19:58:15', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a86=(df.index.isin(df.between_time('19:58:00','19:59:50', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a87=(df.index.isin(df.between_time('19:58:00','19:59:50', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a88=(df.index.isin(df.between_time('19:58:00','19:59:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a89=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a90=(df.index.isin(df.between_time('19:58:00','19:58:25', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a91=(df.index.isin(df.between_time('19:58:00','19:58:25', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a92=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a93=(df.index.isin(df.between_time('19:58:00','19:58:25', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a94=(df.index.isin(df.between_time('19:58:00','19:58:10', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a95=(df.index.isin(df.between_time('19:58:05','19:59:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a96=(df.index.isin(df.between_time('19:58:05','19:59:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a97=(df.index.isin(df.between_time('19:58:05','19:58:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a98=(df.index.isin(df.between_time('19:58:05','19:59:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a99=(df.index.isin(df.between_time('19:58:05','19:59:15', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a100=(df.index.isin(df.between_time('19:58:10','19:58:15', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a101=(df.index.isin(df.between_time('19:58:10','19:58:15', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a102=(df.index.isin(df.between_time('19:58:15','19:58:20', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a103=(df.index.isin(df.between_time('19:58:15','19:58:20', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a104=(df.index.isin(df.between_time('19:58:15','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a105=(df.index.isin(df.between_time('19:58:20','19:58:25', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a106=(df.index.isin(df.between_time('19:58:20','19:58:25', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a107=(df.index.isin(df.between_time('19:58:25','19:59:40', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a108=(df.index.isin(df.between_time('19:58:25','19:59:50', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a109=(df.index.isin(df.between_time('19:58:25','19:59:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a110=(df.index.isin(df.between_time('19:58:25','19:59:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a111=(df.index.isin(df.between_time('19:58:25','19:58:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a112=(df.index.isin(df.between_time('19:58:35','19:58:45', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a113=(df.index.isin(df.between_time('19:58:35','19:59:15', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a114=(df.index.isin(df.between_time('19:58:40','19:59:05', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a115=(df.index.isin(df.between_time('19:58:45','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a116=(df.index.isin(df.between_time('19:59:00','19:59:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a117=(df.index.isin(df.between_time('19:59:05','19:59:35', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a118=(df.index.isin(df.between_time('19:59:15','19:59:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a119=(df.index.isin(df.between_time('19:59:15','19:59:35', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a120=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a121=(df.index.isin(df.between_time('19:59:35','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a122=(df.index.isin(df.between_time('19:59:35','19:59:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a123=(df.index.isin(df.between_time('19:59:35','19:59:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a124=(df.index.isin(df.between_time('19:59:35','19:59:40', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a125=(df.index.isin(df.between_time('19:59:35','19:59:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a126=(df.index.isin(df.between_time('19:59:35','19:59:45', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a127=(df.index.isin(df.between_time('19:59:40','19:59:55', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a128=(df.index.isin(df.between_time('19:59:40','19:59:50', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a129=(df.index.isin(df.between_time('19:59:45','19:59:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a130=(df.index.isin(df.between_time('19:59:45','19:59:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a131=(df.index.isin(df.between_time('19:59:45','19:59:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a132=(df.index.isin(df.between_time('19:59:45','19:59:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a133=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a134=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a135=(df.index.isin(df.between_time('19:59:50','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a136=(df.index.isin(df.between_time('19:59:50','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a137=(df.index.isin(df.between_time('19:59:50','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a138=(df.index.isin(df.between_time('19:59:50','19:59:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a139=(df.index.isin(df.between_time('19:59:50','19:59:55', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    a140=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,np.inf))
    a141=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a142=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a143=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a144=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a145=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.05,np.inf))
    a146=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(0.02,0.05))
    a147=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a148=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,np.inf))
    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,
    a31,a32,a33,a34,
a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,
a62,a63,a64,a65,a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87,a88,a89,a90,a91,a92,a93,a94,a95,a96,a97,a98,a99,a100,
a101,a102,a103,a104,a105,a106,a107,a108,a109,a110,a111,a112,a113,a114,a115,a116,a117,a118,a119,a120,a121,a122,a123,a124,a125,a126,a127,a128,a129,a130,a131,a132,a133,a134,a135,a136,a137,a138,a139,a140,a141,a142,a143,a144,a145,a146,a147,a148]
    
    df['Liquid_6M_Hurdles_Updated'] = np.select(statements,vals,15)
    df['Over_Liquid_6M_Hurdles'] = (df['Per_Return_Exp'] > df['Liquid_6M_Hurdles_Updated']) & (df['ADV'].between(6000000,25000000))


# In[1021]:


def check_liquid_25M_55m_pri_hurdle_updated(df):    
    truncated = True
    rd = pd.read_csv('read_hurdles_nqc_25M_updated_20220822_15bps.csv')
    rd.columns = rd.columns.str.replace(' ','')
    rd['fpa_low'] = rd['fpa_low'].replace(-1,"-np.inf")
    rd['fpa_low'] = rd['fpa_low'].replace('#NAME?',"-np.inf")
    rd[['nas_high','matched_quantity_BracketsHigh']] = rd[['nas_high','matched_quantity_BracketsHigh']].replace('inf','np.inf')
    cols = ['fpa_low', 'fpa_high', 'nas_low','nas_high', 'matched_quantity_BracketsLow', 'matched_quantity_BracketsHigh', 'hurdle']
    rd[cols] = rd[cols].astype(str)
    rd['Previous'] = rd.groupby(cols[:-1])['hurdle'].shift(1)
    rd['Same'] = rd['hurdle'] == rd['Previous']
    if truncated == True:
        rd = rd[rd['Same']==False]
    rd['Time2'] = rd.groupby('Features')['Time'].shift(-1).fillna('20:00:00')
    rd['stat'] = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index)) & (df['direction_from_10'].between("+rd['fpa_low']+","+rd['fpa_high']+")) & (df['Notional_Auction_Size'].between("+rd['nas_low']+","+rd['nas_high']+")) & (df['matched_quantity'].between("+rd['matched_quantity_BracketsLow']+","+rd['matched_quantity_BracketsHigh']+"))" 
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    rd['value'] = True
    vals = rd['hurdle'].astype('float').to_list()
    
    a0=(df.index.isin(df.between_time('19:55:00','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a1=(df.index.isin(df.between_time('19:57:20','19:57:55', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a2=(df.index.isin(df.between_time('19:57:55','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a3=(df.index.isin(df.between_time('19:58:05','19:58:45', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a4=(df.index.isin(df.between_time('19:58:45','19:59:25', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a5=(df.index.isin(df.between_time('19:59:25','19:59:40', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a6=(df.index.isin(df.between_time('19:59:40','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a7=(df.index.isin(df.between_time('19:55:00','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a8=(df.index.isin(df.between_time('19:57:20','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a9=(df.index.isin(df.between_time('19:57:30','19:57:35', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a10=(df.index.isin(df.between_time('19:57:35','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a11=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a12=(df.index.isin(df.between_time('19:55:00','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a13=(df.index.isin(df.between_time('19:57:20','19:57:55', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a14=(df.index.isin(df.between_time('19:57:55','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a15=(df.index.isin(df.between_time('19:58:05','19:58:25', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a16=(df.index.isin(df.between_time('19:58:25','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a17=(df.index.isin(df.between_time('19:58:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a18=(df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000.0))
    a19=(df.index.isin(df.between_time('19:55:00','19:56:40', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a20=(df.index.isin(df.between_time('19:56:40','19:57:10', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a21=(df.index.isin(df.between_time('19:57:10','19:59:15', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a22=(df.index.isin(df.between_time('19:59:15','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a23=(df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a24=(df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a25=(df.index.isin(df.between_time('19:55:00','19:55:05', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a26=(df.index.isin(df.between_time('19:55:05','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a27=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a28=(df.index.isin(df.between_time('19:58:05','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a29=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a30=(df.index.isin(df.between_time('19:55:00','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a31=(df.index.isin(df.between_time('19:57:20','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a32=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a33=(df.index.isin(df.between_time('19:58:05','19:58:10', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a34=(df.index.isin(df.between_time('19:58:10','19:58:55', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a35=(df.index.isin(df.between_time('19:58:55','19:59:05', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a36=(df.index.isin(df.between_time('19:59:05','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a37=(df.index.isin(df.between_time('19:55:00','19:57:35', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a38=(df.index.isin(df.between_time('19:57:35','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a39=(df.index.isin(df.between_time('19:58:00','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a40=(df.index.isin(df.between_time('19:58:05','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a41=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a42=(df.index.isin(df.between_time('19:55:00','19:56:20', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000.0))
    a43=(df.index.isin(df.between_time('19:56:20','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000.0))
    a44=(df.index.isin(df.between_time('19:55:00','19:55:55', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a45=(df.index.isin(df.between_time('19:55:55','19:57:15', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a46=(df.index.isin(df.between_time('19:57:15','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a47=(df.index.isin(df.between_time('19:57:20','19:57:35', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a48=(df.index.isin(df.between_time('19:57:35','19:57:45', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a49=(df.index.isin(df.between_time('19:57:45','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a50=(df.index.isin(df.between_time('19:58:00','19:59:35', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a51=(df.index.isin(df.between_time('19:59:35','19:59:55', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a52=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a53=(df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a54=(df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a55=(df.index.isin(df.between_time('19:55:00','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a56=(df.index.isin(df.between_time('19:58:05','19:59:40', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a57=(df.index.isin(df.between_time('19:59:40','19:59:45', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a58=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a59=(df.index.isin(df.between_time('19:55:00','19:57:10', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a60=(df.index.isin(df.between_time('19:57:10','19:57:45', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a61=(df.index.isin(df.between_time('19:57:45','19:57:50', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a62=(df.index.isin(df.between_time('19:57:50','19:57:55', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a63=(df.index.isin(df.between_time('19:57:55','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a64=(df.index.isin(df.between_time('19:55:00','19:58:05', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a65=(df.index.isin(df.between_time('19:58:05','19:59:20', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a66=(df.index.isin(df.between_time('19:59:20','19:59:40', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a67=(df.index.isin(df.between_time('19:59:40','19:59:45', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a68=(df.index.isin(df.between_time('19:59:45','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a69=(df.index.isin(df.between_time('19:55:00','19:59:55', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000.0))
    a70=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000.0))
    a71=(df.index.isin(df.between_time('19:55:00','19:57:45', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a72=(df.index.isin(df.between_time('19:57:45','19:59:55', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a73=(df.index.isin(df.between_time('19:59:55','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a74=(df.index.isin(df.between_time('19:55:00','19:56:35', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a75=(df.index.isin(df.between_time('19:56:35','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a76=(df.index.isin(df.between_time('19:57:20','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a77=(df.index.isin(df.between_time('19:57:30','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a78=(df.index.isin(df.between_time('19:58:00','19:59:15', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a79=(df.index.isin(df.between_time('19:59:15','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(0,1000000.0))
    a80=(df.index.isin(df.between_time('19:55:00','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a81=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(1000000,3000000.0))
    a82=(df.index.isin(df.between_time('19:55:00','19:56:35', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a83=(df.index.isin(df.between_time('19:56:35','19:57:20', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a84=(df.index.isin(df.between_time('19:57:20','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a85=(df.index.isin(df.between_time('19:57:30','19:57:35', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))
    a86=(df.index.isin(df.between_time('19:57:35','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000.0)) & (df['matched_quantity'].between(3000000,np.inf))

    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,a62,a63,a64,a65,a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86]
    
    df['Liquid_25M_Hurdles_Updated'] = np.select(statements,vals,15)
    df['Over_Liquid_25M_Hurdles'] = (df['Per_Return_Exp'] > df['Liquid_25M_Hurdles_Updated']) & (df['ADV']>25000000)


# In[1022]:


def check_liquid_6M_55m_pri_hurdle(df):
    truncated = True
    rd = pd.read_csv('read_hurdles_nqc_6M_55m_forapproval.csv')
    rd.columns = rd.columns.str.replace(' ','')
    rd[['fpa_low','IADV_Clean_BracketsLow']] = rd[['fpa_low','IADV_Clean_BracketsLow']].replace(-1,'-np.inf')
    cols = ['is_Flip', 'fpa_Brackets', 'fpa_low', 'fpa_high',
           'IADV_Clean_Brackets', 'IADV_Clean_BracketsLow',
           'IADV_Clean_BracketsHigh', 'hurdle']
    rd[cols] = rd[cols].astype(str)
    rd[cols] = rd[cols].astype(str)
    rd['Previous'] = rd.groupby(cols[:-1])['hurdle'].shift(1)
    rd['Same'] = rd['hurdle'] == rd['Previous']
    if truncated == True:
        rd = rd[rd['Same']==False]
    rd[['fpa_high','IADV_Clean_BracketsHigh']] = rd[['fpa_high','IADV_Clean_BracketsHigh']].replace('inf','np.inf')
    rd['Time2'] = rd.groupby('Factor')['Time'].shift(-1).fillna('20:00:00')
    stat1 = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index))"
    stat2 = "(df['is_Flip']=='"+rd['is_Flip']+"')"
    stat3 = "(df['direction_from_10'].between("+rd['fpa_low']+","+rd['fpa_high']+"))"
    stat4 = "(df['IADV'].between("+rd['IADV_Clean_BracketsLow']+","+rd['IADV_Clean_BracketsHigh']+"))"
    stat5 = "(df['Per_Return_Exp']>="+rd['hurdle']+")"
    rd['stat'] = stat1 + "&" + stat2 + "&" + stat3 + "&" + stat4
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    rd['value'] = True
    vals = rd['hurdle'].astype('float').to_list()
    
    a0=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a1=(df.index.isin(df.between_time('19:57:30','19:58:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a2=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a3=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a4=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a5=(df.index.isin(df.between_time('19:55:30','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a6=(df.index.isin(df.between_time('19:56:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a7=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a8=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a9=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,0.1))
    a10=(df.index.isin(df.between_time('19:56:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,0.1))
    a11=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,0.1))
    a12=(df.index.isin(df.between_time('19:55:00','19:58:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.1,np.inf))
    a13=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.1,np.inf))
    a14=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.1,np.inf))
    a15=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a16=(df.index.isin(df.between_time('19:56:00','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a17=(df.index.isin(df.between_time('19:57:00','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a18=(df.index.isin(df.between_time('19:57:30','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a19=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a20=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a21=(df.index.isin(df.between_time('19:55:30','19:56:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a22=(df.index.isin(df.between_time('19:56:30','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a23=(df.index.isin(df.between_time('19:57:30','19:58:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a24=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a25=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a26=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a27=(df.index.isin(df.between_time('19:55:30','19:59:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a28=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a29=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a30=(df.index.isin(df.between_time('19:55:30','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a31=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a32=(df.index.isin(df.between_time('19:55:00','19:56:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a33=(df.index.isin(df.between_time('19:56:30','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a34=(df.index.isin(df.between_time('19:57:30','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a35=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a36=(df.index.isin(df.between_time('19:55:00','19:56:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a37=(df.index.isin(df.between_time('19:56:30','19:57:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a38=(df.index.isin(df.between_time('19:57:00','19:58:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a39=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a40=(df.index.isin(df.between_time('19:59:00','19:59:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a41=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a42=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a43=(df.index.isin(df.between_time('19:56:00','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a44=(df.index.isin(df.between_time('19:57:30','19:59:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a45=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a46=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.1,np.inf))
    a47=(df.index.isin(df.between_time('19:56:00','19:57:30', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.1,np.inf))
    a48=(df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index))&(df['is_Flip']=='Flip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.1,np.inf))
    a49=(df.index.isin(df.between_time('19:55:00','19:56:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a50=(df.index.isin(df.between_time('19:56:30','19:57:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a51=(df.index.isin(df.between_time('19:57:30','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a52=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(-np.inf,0.02))
    a53=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a54=(df.index.isin(df.between_time('19:55:30','19:56:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a55=(df.index.isin(df.between_time('19:56:30','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a56=(df.index.isin(df.between_time('19:59:00','19:59:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a57=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.02,0.05))
    a58=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,0.1))
    a59=(df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.05,0.1))
    a60=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.1,np.inf))
    a61=(df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(-np.inf,0.0))&(df['IADV'].between(0.1,np.inf))
    a62=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a63=(df.index.isin(df.between_time('19:55:30','19:56:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a64=(df.index.isin(df.between_time('19:56:30','19:57:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a65=(df.index.isin(df.between_time('19:57:30','19:59:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a66=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(-np.inf,0.02))
    a67=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a68=(df.index.isin(df.between_time('19:55:30','19:58:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a69=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a70=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.02,0.05))
    a71=(df.index.isin(df.between_time('19:55:00','19:58:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a72=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a73=(df.index.isin(df.between_time('19:59:00','19:59:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a74=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.05,0.1))
    a75=(df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a76=(df.index.isin(df.between_time('19:57:00','19:58:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a77=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a78=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.0,0.005))&(df['IADV'].between(0.1,np.inf))
    a79=(df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a80=(df.index.isin(df.between_time('19:58:00','19:58:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a81=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a82=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(-np.inf,0.02))
    a83=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a84=(df.index.isin(df.between_time('19:56:00','19:57:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a85=(df.index.isin(df.between_time('19:57:30','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a86=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.02,0.05))
    a87=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a88=(df.index.isin(df.between_time('19:55:30','19:56:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a89=(df.index.isin(df.between_time('19:56:30','19:58:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a90=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.05,0.1))
    a91=(df.index.isin(df.between_time('19:55:00','19:59:30', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.1,np.inf))
    a92=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index))&(df['is_Flip']=='NoFlip')&(df['direction_from_10'].between(0.005,0.015))&(df['IADV'].between(0.1,np.inf))

    statements = [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,
a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,a62,a63,a64,a65,a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,
a82,a83,a84,a85,a86,a87,a88,a89,a90,a91,a92]
    
    df['Liquid_6M_Hurdles'] = np.select(statements,vals,15)
    df['Over_Liquid_6M_Hurdles'] = (df['Per_Return_Exp'] > df['Liquid_6M_Hurdles']) & (df['ADV'].between(6000000,25000000))


# In[1023]:


def check_liquid_25M_55m_pri_hurdle(df):
    truncated = True
    rd = pd.read_csv('read_hurdles_nqc_25M_55m_liquid_forapproval.csv')
    rd.columns = rd.columns.str.replace(' ','')
    rd['fpa_low'] = rd['fpa_low'].replace(-1,"-np.inf")
    rd[['NAS_high','mq_high']] = rd[['NAS_high','mq_high']].replace('inf',np.inf)
    cols = ['fpa_low', 'fpa_high', 'NAS_low','NAS_high', 'mq_low', 'mq_high', 'hurdle']
    rd[cols] = rd[cols].astype(str)
    rd['Previous'] = rd.groupby(cols[:-1])['hurdle'].shift(1)
    rd['Same'] = rd['hurdle'] == rd['Previous']
    if truncated == True:
        rd = rd[rd['Same']==False]
    rd['Time2'] = rd.groupby('Factor')['Time'].shift(-1).fillna('20:00:00')
    rd['stat'] = "(df.index.isin(df.between_time('"+rd['Time']+"','"+rd['Time2']+"', include_end=False).index)) & (df['direction_from_10'].between("+rd['fpa_low']+","+rd['fpa_high']+")) & (df['Notional_Auction_Size'].between("+rd['NAS_low']+","+rd['NAS_high']+")) & (df['matched_quantity'].between("+rd['mq_low']+","+rd['mq_high']+"))" 
    rd['ind'] = np.arange(len(rd)).astype(str)
    rd['ind'] = 'a'+ rd['ind']
    rd['stat'] = rd['ind'] + "=" + rd['stat']
    rd['value'] = True
    vals = rd['hurdle'].astype('float').to_list()
    
    a0=(df.index.isin(df.between_time('19:55:00','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a1=(df.index.isin(df.between_time('19:58:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a2=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a3=(df.index.isin(df.between_time('19:56:00','19:56:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a4=(df.index.isin(df.between_time('19:56:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a5=(df.index.isin(df.between_time('19:59:00','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a6=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a7=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a8=(df.index.isin(df.between_time('19:55:30','19:56:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a9=(df.index.isin(df.between_time('19:56:30','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a10=(df.index.isin(df.between_time('19:57:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a11=(df.index.isin(df.between_time('19:59:00','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a12=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a13=(df.index.isin(df.between_time('19:55:00','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(0,1000000))
    a14=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(0,1000000))
    a15=(df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a16=(df.index.isin(df.between_time('19:58:00','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a17=(df.index.isin(df.between_time('19:58:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a18=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a19=(df.index.isin(df.between_time('19:56:00','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a20=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a21=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a22=(df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a23=(df.index.isin(df.between_time('19:57:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a24=(df.index.isin(df.between_time('19:57:30','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a25=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a26=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a27=(df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a28=(df.index.isin(df.between_time('19:57:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a29=(df.index.isin(df.between_time('19:57:30','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a30=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a31=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a32=(df.index.isin(df.between_time('19:55:00','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a33=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a34=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(-np.inf,0.0)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a35=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a36=(df.index.isin(df.between_time('19:57:30','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a37=(df.index.isin(df.between_time('19:58:30','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a38=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a39=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a40=(df.index.isin(df.between_time('19:57:30','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a41=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a42=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a43=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a44=(df.index.isin(df.between_time('19:57:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a45=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a46=(df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(0,1000000))
    a47=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(0,1000000))
    a48=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a49=(df.index.isin(df.between_time('19:55:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a50=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a51=(df.index.isin(df.between_time('19:55:30','19:56:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a52=(df.index.isin(df.between_time('19:56:00','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a53=(df.index.isin(df.between_time('19:58:00','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a54=(df.index.isin(df.between_time('19:59:00','19:59:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a55=(df.index.isin(df.between_time('19:59:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a56=(df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a57=(df.index.isin(df.between_time('19:58:00','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a58=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a59=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(0,1000000))
    a60=(df.index.isin(df.between_time('19:55:00','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a61=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a62=(df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a63=(df.index.isin(df.between_time('19:58:00','19:58:30', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a64=(df.index.isin(df.between_time('19:58:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a65=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.0,0.005)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a66=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a67=(df.index.isin(df.between_time('19:55:30','19:59:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a68=(df.index.isin(df.between_time('19:59:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(0,1000000))
    a69=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a70=(df.index.isin(df.between_time('19:55:30','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a71=(df.index.isin(df.between_time('19:57:30','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a72=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(1000000,3000000))
    a73=(df.index.isin(df.between_time('19:55:00','19:55:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a74=(df.index.isin(df.between_time('19:55:30','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a75=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(0,50000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a76=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a77=(df.index.isin(df.between_time('19:56:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a78=(df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(1000000,3000000))
    a79=(df.index.isin(df.between_time('19:55:00','19:56:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a80=(df.index.isin(df.between_time('19:56:00','19:56:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a81=(df.index.isin(df.between_time('19:56:30','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a82=(df.index.isin(df.between_time('19:57:30','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a83=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(450000000,np.inf)) & (df['matched_quantity'].between(3000000,np.inf))
    a84=(df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(1000000,3000000))
    a85=(df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a86=(df.index.isin(df.between_time('19:57:30','19:58:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))
    a87=(df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index)) & (df['direction_from_10'].between(0.005,0.015)) & (df['Notional_Auction_Size'].between(50000000,450000000)) & (df['matched_quantity'].between(3000000,np.inf))

    statements= [a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,
a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46,a47,a48,a49,a50,a51,a52,a53,a54,a55,a56,a57,a58,a59,a60,a61,
a62,a63,a64,a65,a66,a67,a68,a69,a70,a71,a72,a73,a74,a75,a76,a77,a78,a79,a80,a81,a82,a83,a84,a85,a86,a87]
    df['Liquid_25M_Hurdles'] = np.select(statements,vals,15)
    df['Over_Liquid_25M_Hurdles'] = (df['Per_Return_Exp'] > df['Liquid_25M_Hurdles']) & (df['ADV']>25000000)


# In[1024]:


def MQAS_Score(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['MQAS']<.20)   
    a2a = (df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['MQAS']<.20) & (df['Indicated_Clearing_Price']==0)
    a2b = (df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['MQAS']<.20) & (df['Indicated_Clearing_Price']!=0)
    a3 = (df.index.isin(df.between_time('19:57:00','20:00:00', include_end=False).index)) & (df['MQAS']<.20)   
    a4 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left'))  
    a5a = (df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left')) & (df['Indicated_Clearing_Price']==0)    
    a5b = (df.index.isin(df.between_time('19:55:00','19:57:00', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left')) & (df['Indicated_Clearing_Price']!=0)
    a6 = (df.index.isin(df.between_time('19:57:00','20:00:00', include_end=False).index)) & (df['MQAS'].between(.2,.5,inclusive='left'))
    a7 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['MQAS']>=.5) 
    a8a = (df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index)) & (df['MQAS']>=.5) & (df['Indicated_Clearing_Price']==0)    
    a8b = (df.index.isin(df.between_time('19:55:00','19:58:00', include_end=False).index)) & (df['MQAS']>=.5) & (df['Indicated_Clearing_Price']!=0)
    a9 = (df.index.isin(df.between_time('19:58:00','20:00:00', include_end=False).index)) & (df['MQAS']>=.5)
    a10 = (df['MQAS'].isna())
    
    vals = [.0095,.0095,.015,.0095,.002,.002,.0125,.015,.003,.003,.004,.0025,0]
    default = ['x']
    
    df['MQAS_Score'] = np.select([a1,a2a,a2b,a3,a4,a5a,a5b,a6,a7,a8a,a8b,a9,a10],vals,default=default)
    df['MQAS_Score'] = df['MQAS_Score'].astype(float)


# In[1025]:


def IADV_Score(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['IADV']<2)
    a2a = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df['IADV']<2) & (df['Indicated_Clearing_Price']==0) 
    a2b = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df['IADV']<2) & (df['Indicated_Clearing_Price']!=0) 
    a3 = (df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df['IADV']<2)
    a4 = (df.index.isin(df.between_time('19:50:00','19:58:30', include_end=False).index)) & (df['IADV']>=2)   
    a5 = (df.index.isin(df.between_time('19:58:30','20:00:00', include_end=False).index)) & (df['IADV']>=2)
    a6 = (df['IADV'].isna())
    
    vals = [.003,.003,.005,.004,.0115,.005,0]
    default = ['x']
    
    df['IADV_Score'] = np.select([a1,a2a,a2b,a3,a4,a5,a6],vals,default=default)
    df['IADV_Score'] = df['IADV_Score'].astype(float)


# In[1026]:


def PRI_Score(df,pri_col='Per_Return_Exp'):
    a12 = (df['Indicated_Clearing_Price']==0)
    a0 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index))
    a1 = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df[pri_col]<.003)
    a2 = (df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df[pri_col]<.003)
    a3 = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df[pri_col].between(.003,.005))
    a4 = (df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df[pri_col].between(.003,.005))
    a5 = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df[pri_col].between(.005,.0075))
    a6 = (df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df[pri_col].between(.005,.0075))
    a7 = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df[pri_col].between(.0075,.01))
    a8 = (df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df[pri_col].between(.0075,.01))
    a9 = (df.index.isin(df.between_time('19:55:00','19:57:30', include_end=False).index)) & (df[pri_col]>.01)
    a10 = (df.index.isin(df.between_time('19:57:30','20:00:00', include_end=False).index)) & (df[pri_col]>.01)
    a11 = (df[pri_col].isna())
    
    vals = [0,0,.0005,.0015,.003,.002,.0045,.003,.0055,.0045,.007,.0115,0]
    default = ['x']
    
    df['PRI_Score'] = np.select([a12,a0,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11],vals, default=default)
    df['PRI_Score'] = df['PRI_Score'].astype(float)


# In[1027]:


def pri_hurdles(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index))
    a2 = (df.index.isin(df.between_time('19:55:00','20:00:00', include_end=False).index)) & (df['Per_Return_Exp']<.002)

    vals = [True,False]
    default = True

    df['Over_PRI_Hurdle'] = np.select([a1,a2], vals, default=default)


# In[1028]:


def iadv_hurdles(df):
    a1 = (df['IADV']<.1)
    a2 = (df.index.isin(df.between_time('19:50:00','19:58:00', include_end=False).index)) & (df['IADV'].between(.10,.30,inclusive='left'))
    #a3 = (df.index.isin(df.between_time('19:53:30','19:55:00', include_end=False).index)) & (df['IADV'].between(.30,.40,inclusive='left'))
    #a4 = (df.index.isin(df.between_time('19:52:00','19:56:00', include_end=False).index)) & (df['IADV'].between(.9,1,inclusive='left'))


    vals = [False,False]
    default = True

    df['Over_IADV_Hurdle'] = np.select([a1,a2], vals, default=default)


# In[1029]:


def MQAS_hurdles(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:58:15', include_end=False).index)) & (df['MQAS']>.8)   
    a2 = (df.index.isin(df.between_time('19:50:00','19:55:30', include_end=False).index)) & (df['MQAS'].between(.7,.8,inclusive='left'))   

    vals = [False,False]
    default = True

    df['Over_MQAS_Hurdle'] = np.select([a1,a2], vals, default=default)


# In[1030]:


def Flip_hurdles(df):
    a1 = (df.index.isin(df.between_time('19:50:00','19:55:00', include_end=False).index)) & (df['Flip_Count_Cumsum']>0)
    
    vals = [False]
    default = True
    
    df['Over_Flip_Hurdle'] = np.select([a1],vals,default=default)


# In[1031]:


def Price_Action_hurdles(df):
    a1 = (df.index.isin(df.between_time('19:50:00','20:00:00', include_end=False).index)) & (df['direction_from_10']>0.015)
    
    vals = [False]
    default = True
    
    df['Over_PriceAction_Hurdle'] = np.select([a1],vals,default=default)


def create_results_table(df,factor):
#To create summary table for one factor
    merge = False
    factor=factor
    u_factor = factor.copy()
    x_factor = factor.copy()
    if 'DATE' in u_factor:
        pass
    else:
        u_factor.append('DATE')
    if 'SYMBOL' in u_factor:
        pass
    else:
        u_factor.append('SYMBOL')
    df['DATE'] = df['DATE'].astype(str)
    res = df.groupby(factor)[['Filled$_Abs']].sum().reset_index()
    res.rename(columns={'Filled$_Abs':'Capital_Deployed'},inplace=True)
    try:
        total_capital
    except NameError:
        total_capital = 0
    res['Total_Capital'] = total_capital
    #tb = df.groupby(factor)[['Capital_Available']].mean().reset_index()
    #res = pd.merge(res,tb,how='outer')
    #RealizedPL and Basis Return
    tb = df.groupby(factor)[['RealizedPL']].sum().reset_index()
    res = pd.merge(res,tb,how='outer')
    res['Basis_Return'] = res['RealizedPL']/res['Capital_Deployed']
    #Symbols Traded
    tb = df.groupby(u_factor)[['SYMBOL']].nunique()
    tb.rename(columns={'SYMBOL':'SYMBOLS_Traded'},inplace=True)
    tb = tb.reset_index()
    tb = tb.groupby(factor)[['SYMBOLS_Traded']].sum().reset_index()
    res = pd.merge(res,tb,how='outer')
    #Fill Rates
    #tb = (df.groupby(factor)[['FilledQTY_Abs']].sum()*2).reset_index()
    #res = pd.merge(res,tb,how='outer')
    #tb = df.groupby(factor)[['FilledQTY_Abs','QTY_Abs']].sum()
    #tb['Fill_Rate_Shares'] = tb['FilledQTY_Abs']/tb['QTY_Abs']
    #tb[['Filled$_Abs','Desired_$']] = df.groupby(factor)[['Filled$_Abs','Desired_$']].sum()
    #tb['Fill_Rate_Dollars'] = tb['Filled$_Abs']/tb['Desired_$']
    #tb = tb.reset_index()
    ##tb = tb[[factor,'Fill_Rate_Shares','Fill_Rate_Dollars']]
    #res = pd.merge(res,tb,how='outer')
    #Win Rates
    winners = df[df['RealizedPL']>0].reset_index()
    win_summary = winners.groupby(u_factor)[['SYMBOL']].nunique()
    win_summary.rename(columns={'SYMBOL':'Win_Count'},inplace=True)
    win_summary = win_summary.reset_index()
    win_summary = win_summary.groupby(factor)[['Win_Count']].sum().reset_index()
    win_dollars = winners.groupby(factor)[['RealizedPL']].sum().reset_index()
    win_summary = win_summary.merge(win_dollars)
    win_summary.rename(columns={'RealizedPL':'Winner_Dollars'},inplace=True)
    winmean = winners.groupby(factor)[['RealizedPL']].mean().reset_index()
    winmean.rename(columns={'RealizedPL':'WinMean'},inplace=True)
    losers = df[df['RealizedPL']<=0].reset_index()
    lose_dollars = losers.groupby(factor)[['RealizedPL']].sum().reset_index()
    lose_dollars.rename(columns={'RealizedPL':'Loser_Dollars'},inplace=True)
    losemean = losers.groupby(factor)[['RealizedPL']].mean().reset_index()
    losemean.rename(columns={'RealizedPL':'LoseMean'},inplace=True)
    win_summary = pd.merge(win_summary,lose_dollars)
    res = pd.merge(res,win_summary,how='outer')
    res['Win_Rate'] = res['Win_Count']/res['SYMBOLS_Traded']
    res['Win_Ratio'] = winmean['WinMean']/np.absolute(losemean['LoseMean'])
    #Stuck Symbols
    #tb = tb.groupby(factor)[['Stuck']].apply(lambda x: (x=='Stuck').sum()).reset_index()
    #tb.rename(columns={'Stuck':'Stuck_Count'},inplace=True)
    tb = df[df['Stuck']=='Stuck'].reset_index()
    tbsummary = tb.groupby(u_factor)[['SYMBOL']].nunique()
    tbsummary.rename(columns={'SYMBOL':'Stuck_Count'},inplace=True)
    tbsummary = tbsummary.reset_index().groupby(factor)[['Stuck_Count']].sum().reset_index()
    res = pd.merge(res,tbsummary,how='outer')
    res['Stuck_Percent_Count'] = res['Stuck_Count']/res['SYMBOLS_Traded']
    x = df[df['Stuck']=='Stuck']
    tb = x.groupby(factor)[['Filled$_Abs']].sum().reset_index()
    tb.rename(columns={'Filled$_Abs':'Stuck_Filled$_Abs'},inplace=True)
    res = pd.merge(res,tb,how='outer')
    res['Stuck_Percent_$'] = res['Stuck_Filled$_Abs']/res['Capital_Deployed']
    #if 'Score_Bracket' not in factor:
    #    x_factor.append('Score_Bracket')
    #    #Grouping TR_by_Instance by score bracket
    #    factorslice = trinst
    #    group = factorslice.groupby(x_factor)
    #    tb = group[['RealizedPL','Filled$_Abs']].sum()
    #    tb['Basis_Return'] = tb['RealizedPL'] / tb['Filled$_Abs']
    #    tb['Count_by_Symbol'] = group['SYMBOL'].nunique()
    #    x = factorslice[factorslice['RealizedPL']>0]
    #    xwin_count = x.groupby(x_factor)['SYMBOL'].nunique()
    #    tb['Win_Count_by_Symbol'] = xwin_count
    #    tb['Win_Rate'] = tb['Win_Count_by_Symbol'] / tb['Count_by_Symbol']
    #    #Merging Score Bracket groupby into res dataframe
    #    tb = tb.reset_index()
    #    tb = tb.set_index(x_factor[0])
    #    tbscore = tb.pivot(columns='Score_Bracket',values='Count_by_Symbol').add_suffix('_Symbol_Count').reset_index()
    #    res = pd.merge(res,tbscore,how='left')
    #    tbscore = tb.pivot(columns='Score_Bracket',values='Filled$_Abs').add_suffix('_Filled$_Abs').reset_index()
    #    res = pd.merge(res,tbscore,how='left',suffixes=(None,'_Filled$_Abs'))
    #    tbscore = tb.pivot(columns='Score_Bracket',values='RealizedPL').add_suffix('_RealizedPL').reset_index()
    #    res = pd.merge(res,tbscore,how='left')
    try:
        res_imb
        merge = True
    except NameError:
        pass
    if 'DATE' not in res.columns:
        pass
    elif merge == False:
        pass
    elif res['DATE'][0]!=res_imb['DATE'][0]:
        print(res['DATE'][0])
        print(res_imb['DATE'][0])
    else:
        res = pd.merge(res, res_imb)
        print('yes')
    print(factor)
    #res['DATE'] = date
    #res.to_excel(writer, sheet_name=factor)
    #else:
    #    print(factor)
    #    res['DATE'] = date
    #res.to_excel(writer, sheet_name=factor)

    #writer.close()
    #writer.handles = None
    return res


