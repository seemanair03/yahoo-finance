# ---------POI Detector Lambda Function-----------
from pprint import pprint
import boto3
import json
import csv
import datetime
import os
import random
import base64
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from boto3.dynamodb.conditions import Key, And

# DynamoDB table to store alerts
dynamodb_res = boto3.resource('dynamodb', region_name="us-east-1")
alerts_table = dynamodb_res.Table("POIAlert")

# SNS Topic to send alerts
sns_client = boto3.client('sns', region_name='us-east-1')
sns_topic_arn = "arn:aws:sns:us-east-1:862085834875:POIAlert"

# Create blank dict
def getnewresult():
    return {}

# Return price to compare for High POI
def getPOIHigh(h):
    return 0.8 * float(h)

# Return price to compare for Low POI
def getPOILow(l):
    return 1.2 * float(l)


def lambda_handler(event, context):
    for record in event["Records"]:
        payload = base64.b64decode(record["kinesis"]["data"])
        payload = str(payload, 'utf-8')
        result = json.loads(payload, parse_float=Decimal)

        POIHigh = getPOIHigh(result['52WeekHigh'])
        POILow = getPOILow(result['52WeekLow'])
        price = float(result['price'])
        s = str(result['stockid'])
        d = result['price timestamp']
        dt = d[0:10]
        c = str(result['Ctr'])

        print(f'Stock:{s}, Counter:{c}, Date:{dt}, Price:{price}') #For logs

        if str(result['Ctr']) != 1:
            response = alerts_table.scan(FilterExpression=Attr('stockid').eq(s) & Attr('price timestamp').contains(dt))
            if response['Count'] == 0:
                flag = 1
            else:
                flag = 0
        else:
            flag = 1

        if price >= POIHigh and flag == 1:
            print("\nPrice is >=80% of 52WeekHigh") #For logs

            new_result = getnewresult()
            new_result['stockid'] = result['stockid']
            new_result['price timestamp'] = result['price timestamp']
            new_result['price'] = result['price']
            new_result['alert'] = 'High Price'
            new_result['52WeekHigh'] = result['52WeekHigh']
            new_result['52WeekLow'] = result['52WeekLow']

            # Write to database
            alerts_table.put_item(Item=new_result)

            # Send email
            # sns_client.publish(TopicArn=sns_topic_arn, Message="POI detected, price is >= 80% of 52WeekHigh", Subject="High POI")
            sns_client.publish(TopicArn=sns_topic_arn, Message=json.dumps(new_result),
                               Subject="Alert: High Price Detected on Yahoo Finance")

            print("Alert written to Alerts Table and Email sent for POI High") #For logs



        elif price <= POILow and flag == 1:

            print("\nPrice is <=120% of 52WeekLow") #For logs

            new_result = getnewresult()
            new_result['stockid'] = result['stockid']
            new_result['price timestamp'] = result['price timestamp']
            new_result['price'] = result['price']
            new_result['alert'] = 'Low Price'
            new_result['52WeekHigh'] = result['52WeekHigh']
            new_result['52WeekLow'] = result['52WeekLow']

            # Write to database
            alerts_table.put_item(Item=new_result)

            # Send email
            # sns_client.publish(TopicArn=sns_topic_arn, Message="POI detected, price is <= 120% of 52WeekLow", Subject="Low POI")
            sns_client.publish(TopicArn=sns_topic_arn, Message=json.dumps(new_result),
                               Subject="Alert: Low Price Detected on Yahoo Finance")

            print("Alert written to Alerts Table and Email sent for POI Low") #For logs


        else:
            print('\nNo Alert generated or stored!') #For logs

