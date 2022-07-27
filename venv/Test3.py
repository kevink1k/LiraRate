import pyodbc
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
Driver = "ODBC Driver 17 for SQL Server"
Server_name = 'LAPTOP-36NUUO53\SQLEXPRESS'
Database_name = 'test'
Data_table = 'Data_table'
quoted = urllib.parse.quote_plus(f'''
                              Driver={{{Driver}}};
                              Server={Server_name};
                              Database={Database_name};
                              Trusted_connection=yes;
                              ''')
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={quoted}')
query = ("SELECT * from Email_table")
data = pd.read_sql(query,engine)
RowsCount = len(data)
for i in range (RowsCount):
    print(data.iloc[i][0])
    mail = data.iloc[i][0]
    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login("kevinelkik3@gmail.com", "ovlgwvajuyfzkdxe")
    server.sendmail("kevinelkik3@gmail.com",
                    f'"{mail}"',
                    "Lira price rate CHANGEDDDDDDDDDD"
                    )
    server.quit()




