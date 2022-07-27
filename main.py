import requests
from pprint import pprint as pp
import schedule
from datetime import datetime
import pyodbc as odbc
from operator import itemgetter
import pandas as pd
from sqlalchemy import create_engine
import urllib
import smtplib , ssl
#SQL Information
Driver = "ODBC Driver 17 for SQL Server"
Server_name = 'LAPTOP-36NUUO53\SQLEXPRESS'
Database_name = 'test'
Data_table = 'Data_table'
#getting correct URL
now = datetime.now()
date_time = now.strftime("%Y%#m%d%#H")
currency = 'LBP'
_ver = date_time
base_url = "https://lirarate.org/wp-json/lirarate/v2/rates"
url = f'{base_url}?currency={currency}&_ver=t{_ver}'
#Declaring Previous buyrate for comparison
PrevBuyRate = None

#EMAIL SENDER
def SendMail(v,w):
    print(f'Rate Has {w} by {v}')
    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login("kevinelkik3@gmail.com", "ovlgwvajuyfzkdxe")
    server.sendmail("kevinelkik3@gmail.com",
                    "kevinelkik2@gmail.com",
                    f'"Lira Price Rate has {w} by {v}"')
    server.quit()

#API BYT RATE CAll
def LiraRateApiCallPrice():
    global PrevBuyRate
    R = requests.get(url)
    data = R.json()['buy'][0:]
    LastBuyRate = R.json()['buy'][-1][1]
    buyRate = list(map(itemgetter(1), data))
    if PrevBuyRate == None:
        PrevBuyRate = LastBuyRate
    if PrevBuyRate != None and abs(int(PrevBuyRate) - int(LastBuyRate)) >= 200:
        if PrevBuyRate > LastBuyRate:
            v = PrevBuyRate - LastBuyRate
            w = "gone down"
            SendMail(v,w)
        else:
            v1 = LastBuyRate - PrevBuyRate
            w1 = "gone up"
            SendMail(v1,w1)

    PrevBuyRate = LastBuyRate
    return(buyRate)

#API DATE CALL
def LiraRateApiCallDate():
    R = requests.get(url)
    data = R.json()['buy'][0:]
    timestamp = list(map(itemgetter(0),data))
    timelist =[]
    for i in timestamp:
        i = i/1000
        format_date = '%d/%m/%Y'
        date = datetime.fromtimestamp(i)
        timelist.append(date.strftime(format_date))

    return (timelist)

#PUTS DATA INTO SQL
def InstertData():
    Data_Rate = LiraRateApiCallPrice()
    Data_Date = LiraRateApiCallDate()
    df = pd.DataFrame()
    df['Date'] = Data_Date
    df['BuyPrice'] = Data_Rate
    quoted = urllib.parse.quote_plus(f'''
                              Driver={{{Driver}}};
                              Server={Server_name};
                              Database={Database_name};
                              Trusted_connection=yes;
                              ''')
    engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
    df.to_sql('Data_table', con=engine, if_exists='replace')
    print(df)
    return(df)

#Repeating every 30 minutes
schedule.every(3).seconds.do(InstertData())
while 1:
    schedule.run_pending()