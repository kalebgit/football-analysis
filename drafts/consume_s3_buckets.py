import boto3
from rich import print_json
from botocore.config import Config
from botocore.exceptions import ClientError

#recordamos que sin los parametros toma las
#variables de entorno AWS_PROFILE Y AWS_REGION
session = boto3.Session(
    profile_name="dev", 
    region_name="us-east-1"
)

cfg = Config(retries={
    "max_attempts": 10, 
    "mode": "adaptive"
}, read_timeout=120)

s3 = session.client("s3", config=cfg)

try: 
    resp = s3.list_buckets()
    print_json(data=resp, default=str)
    print([b.get("Name") for b in resp["Buckets"]])
except ClientError as e:
    err = e.response.get("Error", {})
    print(err.get("Code"), err.get("Message"))
