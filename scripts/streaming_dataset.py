from datetime import datetime
import requests
import json

# copy "Push URL" from "API Info" in Power BI
url = "https://api.powerbi.com/beta/49c3d703-3579-47bf-a888-7c913fbdced9/datasets/a52e28e9-93f2-4c07-934f-52818e43536d/rows?noSignUpCheck=1&key=aYKSacZqpKWp7w1kUNI6KQJNejVi95yZkv%2FHJQDtLuZFrI3%2FhQkjYdoHsT9%2BoYSShdMZWQevrf5i77sdQHgYUA%3D%3D"

# timestamps should be formatted like this
now = datetime.strftime(
    datetime.now(),
    "%Y-%m-%dT%H:%M:%S"
)

datetime.datetime.fromtimestamp(1597855140000/1000.0)

# data dict must be contained in a list
data = [
    {
        "uniqueId": "1065",
        "outputType": "SPEED_DIFFERENTIAL",
        "currentSpeed": 120,
        "loc": "DitIsDeLocatie",
        "outputType_recordTimestamp": "SPEED_DIFFERENTIAL_1597801262",
        "recordTimestamp": "2020-08-18T14:23:58.300Z",
        "avgSpeed10Minutes": 101,
        "avgSpeed2Minutes": 89,
        "bezettingsgraad": 20,
        "previousSpeed": 96,
        "speedDiffIndicator": 0,
        "trafficJamIndicator": 0
    }
]

# post/push data to the streaming API
headers = {
    "Content-Type": "application/json"
}
response = requests.request(
    method="POST",
    url=url,
    headers=headers,
    data=json.dumps(data)
)

print(response)
print(response.content)
