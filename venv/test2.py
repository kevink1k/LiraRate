import requests
from pprint import pprint as pp
import schedule
from datetime import datetime
import pyodbc as odbc
from operator import itemgetter
import pandas as pd
import urllib
from sqlalchemy import create_engine
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

#API CALL
def LiraRateApiCallPrice():
    R = requests.get(url)
    data = R.json()['buy'][0:]
    buyRate = list(map(itemgetter(1), data))
    return(buyRate)
def LiraRateApiCallDate():
    R = requests.get(url)
    data = R.json()['buy'][0:]
    timestamp = list(map(itemgetter(0),data))
    timelist =[]
    for i in timestamp:
        i = i/1000
        format_date = '%d/%m/%y'
        date = datetime.fromtimestamp(i)
        timelist.append(date.strftime(format_date))

    return (timelist)

Data_Rate = LiraRateApiCallPrice()
Data_Date = LiraRateApiCallDate()
df = pd.DataFrame()
df['Date'] = Data_Date
df['Buy Price'] = Data_Rate
print(df)



def SQL_Add():
    quoted = urllib.parse.quote_plus(f'''
                        Driver={{{Driver}}};
                        Server={Server_name};
                        Database={Database_name};
                        Trusted_connection=yes;
                        ''')
    engine= create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
    df.to_sql('Data_table', con=engine ,if_exists='replace')
#Repeating every 30 minutes
schedule.every(10).seconds.do(LiraRateApiCallPrice)
schedule.every(10).seconds.do(LiraRateApiCallDate)
schedule.every(2).seconds.do(SQL_Add)
while 1:
    schedule.run_pending()