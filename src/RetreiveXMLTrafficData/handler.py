import json
import boto3
import requests

def handle(event, context):

    response = {
        "statusCode": 200,
        "body":'hello'
    }

    return response
