#----------PROBLEM STATEMENT-------
# Your goal is to get per-hour stock price data for a time range for the ten stocks specified in the doc. 
# Further, you should call the static info api for the stocks to get their current 52WeekHigh and 52WeekLow values.
# You should craft individual data records with information about the stockid, price, price timestamp, 52WeekHigh and 52WeekLow values and push them individually on the Kinesis stream


#----------SOLUTION-----------------
import decimal
import json
import boto3
import sys
import yfinance as yf
from decimal import Decimal

import time
import random
import datetime
import pandas as pd

#Create kinesis client
kinesis_client = boto3.client('kinesis', region_name = "us-east-1")


#Function to push data records to Kinesis stream.
def putdatatokinesis(stockid, price, price_timestamp, wHigh, wLow, c):
    temp_responselist = []

    payload = {
                'stockid': stockid,
                'price': price,
                'price timestamp': price_timestamp,
                '52WeekHigh': wHigh,
                '52WeekLow': wLow,
                'Ctr':c
              }

    print(payload)

    put_response = kinesis_client.put_record(
                        StreamName='LoadStockPrice',
                        Data=json.dumps(payload),
                        PartitionKey=stockid)

    #return response code
    temp_responselist = list(put_response.items())
    temp_responselist1 = list (temp_responselist[2][1].items())
    responsecode = temp_responselist1[1][1]

    return responsecode



today = datetime.date(2022,5,11)
yesterday = datetime.date(2022,5,10)

# today = datetime.date.today()
# yesterday = datetime.date.today() - datetime.timedelta(1)

list_stock = ['MSFT','MVIS', 'GOOG', 'SPOT', 'INO', 'OCGN', 'ABML', 'RLLCF', 'JNJ', 'PSFE' ]
#stocklist = ['MSFT','MVIS']

# Pulling the data between 2 dates from yfinance API code for the stocks specified in the doc
for i in list_stock:
    data = yf.download(i, start= yesterday, end= today, interval = '1h' )

    #Code to call 'info' API to get 52WeekHigh and 52WeekLow refering this link - https://pypi.org/project/yfinance/
    ticker = yf.Ticker(i)
    wHigh = ticker.info['fiftyTwoWeekHigh']
    wLow = ticker.info['fiftyTwoWeekLow']

    #gets the closing price for the stock
    pricedata = dict(data['Close'])
    pricedatalist = list(pricedata.items())

    ctr = 0

    # Push data records to Kinesis stream.
    for j in pricedatalist:
        ctr = ctr + 1
        temp_price = str(j[1])
        price = str(decimal.Decimal(temp_price))
        temp_high = str(wHigh)
        High = str(decimal.Decimal(temp_high))
        temp_low = str(wLow)
        Low = str(decimal.Decimal(temp_low))
        n = str(ctr)

        datapostresponse = putdatatokinesis(i, price, str(j[0]), High, Low, n)

        # wait for 2 second
        time.sleep(2)


        if datapostresponse == 200:
            print(f"HTTPStatusCode:{datapostresponse}, Success posting data to stream!")
        else:
            print((f"HTTPStatusCode:{datapostresponse}, Failure posting data to stream, check status code!"))









