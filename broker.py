# %%
import pandas as pd
from datetime import datetime, timedelta
from requests import get
import math
import random
import re
#Write records to sqlite
#import sqlite3
import concurrent.futures


# %%
def is_number(string):
    try:
        f=float(string)
        if math.isnan(f):
            return False
        return True
    except ValueError:
        return False

# %%
def fetch(SERVER, warrant_row, ds, headers,try_counter = 3):
    while True:
        random_server = random.choice(SERVER)
        warrant_vol_URI = f"{random_server}z/zc/zco/zco.djhtm?a={warrant_row['權證代號']}&e={ds}&f={ds}"
        records = pd.DataFrame()
        try:
                raw = get(warrant_vol_URI, headers=headers)
                raw = pd.read_html(raw.content, encoding  = "cp950")
                df = raw[2]
                #parsing and writing data
                start_row =999
                end_flag = False
                try:
                        buy_price = float(df[df[0]=='平均買超成本'][1].values[0])
                        sell_price = float(df[df[0]=='平均買超成本'][6].values[0])
                except:
                        print("無平均成本", warrant_vol_URI)
                        break
                data_date = re.search('最後更新日：([0-9]{4}/[0-9]{2}/[0-9]{2})', df.loc[2,2]).group(1)
                for index, row in df.iterrows():
                        if (row[0]=='買超券商'):
                                start_row=index+1
                        if(row[0]=='合計買超張數'):
                                end_flag = True
                        if (index>= start_row and end_flag == False):
                                if (is_number(row[3])):
                                        data_row = {"券商":row[0],"種類":"買超", "張數":row[3], "成本":buy_price, "日期":data_date, "權證代號":warrant_row["權證代號"], "標的代號":warrant_row["標的代號"], "權證簡稱":warrant_row["權證簡稱"], "標的名稱":warrant_row["標的名稱"]}
                                        records = pd.concat([records, pd.DataFrame(data_row, index=[0])])
                                        # insert_record ([data_date, row[0], a, row [3], buy_price],mycursor)
                                if (is_number(row[8])):  
                                        data_row = {"券商":row[5],"種類":"賣超", "張數":row[8], "成本":sell_price, "日期":data_date, "權證代號":warrant_row["權證代號"], "標的代號":warrant_row["標的代號"], "權證簡稱":warrant_row["權證簡稱"], "標的名稱":warrant_row["標的名稱"]}
                                        records = pd.concat([records, pd.DataFrame(data_row, index=[0])])
                print (warrant_vol_URI, '已載入')
                return records
        except Exception as e:
                print(warrant_vol_URI, e)
                try_counter -= 1
                if try_counter == 0:
                        return pd.DataFrame

def update_broker(d=datetime.now()):
    SERVER=[
            'https://fubon-ebrokerdj.fbs.com.tw/',
            'http://jsjustweb.jihsun.com.tw/',
            'http://5850web.moneydj.com/',
            #'https://stock.capital.com.tw/', #只能抓當日資料
            'http://just2.entrust.com.tw/',
            'http://newjust.masterlink.com.tw/',
            'https://moneydj.emega.com.tw/',
            'http://jdata.yuanta.com.tw/',
            #'https://dj.mybank.com.tw/',
            #'https://sjmain.esunsec.com.tw/',
            ]
    records = pd.DataFrame()
    headers ={
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36",
    }
    ###########Main###########
    ds = d.strftime('%Y%m%d')
    warrants_URI = 'https://www.twse.com.tw/rwd/zh/stock/warrantStock?response=csv&date=' + ds
    #download csv 
    with open(f'./warrants/warrants_{ds}.csv', 'wb') as f:
        f.write(get(warrants_URI).content)
    warrants = pd.read_csv(f'./warrants/warrants_{ds}.csv', encoding="cp950", skiprows=2)

    #remove the = symbol in 權證代號 and 標的代號
    warrants['權證代號'] = warrants['權證代號'].str.replace('=','')
    warrants['標的代號'] = warrants['標的代號'].str.replace('=','')
    #remove the " symbol in 權證代號
    warrants['權證代號'] = warrants['權證代號'].str.replace('"','')
    warrants['標的代號'] = warrants['標的代號'].str.replace('"','')
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for index, warrant_row in warrants[:].iterrows():
                # 2023-5-5 without preceding 0
                ds = f"{d.year}-{d.month}-{d.day}"
                future = executor.submit(fetch, SERVER, warrant_row, ds, headers)
                futures.append(future)
                #Collect the results every 10 requests
                if index % 10 == 0:
                        for future in futures:
                                records = pd.concat([records, future.result()])
                        futures = []
        #Last batch
        for future in futures:
                records = pd.concat([records, future.result()])
                #records = pd.concat([records, fetch(SERVER, warrant_row, ds, headers)])
    records.to_csv(f'./data/warrant_records_{ds}.csv', index=False, encoding="utf-8-sig")

# %%
from dateutil.parser import parse
update_broker(parse('2023-5-10'))
#update_broker(datetime.today().strftime("%Y%m%d"))



# %%
