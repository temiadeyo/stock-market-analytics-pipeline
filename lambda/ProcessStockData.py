import json
import boto3
import base64
from decimal import Decimal

# Initialize AWS Clients
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# Resource Names
DYNAMO_TABLE = "stock-market-data"
S3_BUCKET = "stock-market-data-bucket-temi-v1"

# Table reference
table = dynamodb.Table(DYNAMO_TABLE)


def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            # Decode base64 Kinesis data
            raw_data = base64.b64decode(
                record["kinesis"]["data"]
            ).decode("utf-8")

            payload = json.loads(raw_data)
            print("Processing:", payload)

            # Store raw data in S3
            s3_key = f"raw-data/{payload['symbol']}/{payload['timestamp'].replace(':', '-')}.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=json.dumps(payload),
                ContentType="application/json"
            )

            # Compute stock metrics
            price_change = round(payload["price"] - payload["previous_close"], 2)
            price_change_percent = round(
                (price_change / payload["previous_close"]) * 100, 2
            )

            is_anomaly = "Yes" if abs(price_change_percent) > 5 else "No"

            moving_average = (
                payload["open"]
                + payload["high"]
                + payload["low"]
                + payload["price"]
            ) / 4

            # Structured data for DynamoDB
            item = {
                "symbol": payload["symbol"],
                "timestamp": payload["timestamp"],
                "open": Decimal(str(payload["open"])),
                "high": Decimal(str(payload["high"])),
                "low": Decimal(str(payload["low"])),
                "price": Decimal(str(payload["price"])),
                "previous_close": Decimal(str(payload["previous_close"])),
                "change": Decimal(str(price_change)),
                "change_percent": Decimal(str(price_change_percent)),
                "volume": int(payload["volume"]),
                "moving_average": Decimal(str(moving_average)),
                "anomaly": is_anomaly
            }

            # Store in DynamoDB
            table.put_item(Item=item)

            print("Stored:", item)

        except Exception as e:
            print("Error:", str(e))

    return {
        "statusCode": 200,
        "body": "Processing complete"
    }
