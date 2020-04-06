import json
import boto3
import requests
import os

def handle(event, context):

    url = 'https://api.chucknorris.io/jokes/random'
    r = requests.get(url)
    data = r.json()
    print(data)

    response = {
        "statusCode": 200,
        "body":'hello'
    }

    return response
