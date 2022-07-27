import requests
from pprint import pprint as pp
from bs4 import BeautifulSoup
import schedule
from datetime import datetime
import pyodbc as odbc
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

#API CALL
def LiraRateApiCall():
    R = requests.get(url)
    timestamp = R.json()['buy'][-1][0]/1000
    format_date = '%d/%m/%y'
    date = datetime.fromtimestamp(timestamp)
    buyRate = R.json()['buy'][-1][1]
    print(date.strftime(format_date))
    print(buyRate)
    return (date.strftime(format_date),buyRate)

data = LiraRateApiCall()
#ADDDING TO SQL SERVER
def SQL_Add():
    conn = odbc.connect(f'''
                        Driver={{{Driver}}};
                        Server={Server_name};
                        Database={Database_name};
                        Trusted_connection=yes;
                        ''')
    cursor = conn.cursor()
    cursor.execute ('''
                    INSERT INTO Data_table (Time1,Price)
                    VALUES ( ? , ?)
                    ''',data)
    conn.commit()
    cursor.execute(f'SELECT * FROM {Data_table}')

    for i in cursor:
        print(i)


#Repeating every 30 minutes
schedule.every(1).seconds.do(LiraRateApiCall)
schedule.every(1).seconds.do(SQL_Add)
while 1:
    schedule.run_pending()