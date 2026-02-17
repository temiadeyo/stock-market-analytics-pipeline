import boto3
import json
import decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key

# AWS Clients
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")

# DynamoDB Table Name and SNS ARN
TABLE_NAME = "stock-market-data"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:019891040273:Stock_Trend_Alerts"

table = dynamodb.Table(TABLE_NAME)


def get_recent_stock_data(symbol, minutes=5):
    # Fetch stock data for the last minutes from DynamoDB using 'Query' instead of 'Scan'

    now = datetime.utcnow()
    past_time = now - timedelta(minutes=minutes)

    try:
        response = table.query(
            KeyConditionExpression=
                Key("symbol").eq(symbol) & Key("timestamp").gte(
                    past_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                ),
            ScanIndexForward=True
        )

        return response.get("Items", [])

    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return []


def calculate_moving_average(data, period):
    # Calculate moving average for given period, avoid None issues

    if len(data) < period:
        return decimal.Decimal("0")

    return sum(
        decimal.Decimal(str(d["price"])) for d in data[-period:]
    ) / decimal.Decimal(period)


def lambda_handler(event, context):

    # All your symbols
    symbols = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL"]

    for symbol in symbols:

        stock_data = get_recent_stock_data(symbol)

        if len(stock_data) < 20: # Ensure we have enough data
            continue

        # Compute Short-Term Moving Averages
        sma_5 = calculate_moving_average(stock_data, 5)
        sma_20 = calculate_moving_average(stock_data, 20)

        # Get previous Short-Term Moving Averages
        sma_5_prev = calculate_moving_average(stock_data[:-1], 5)
        sma_20_prev = calculate_moving_average(stock_data[:-1], 20)

        message = None

        # Detect Trend Change
        if sma_5_prev < sma_20_prev and sma_5 > sma_20:
            message = f"{symbol} is in an **UPTREND**! — Potential buy opportunity."

        elif sma_5_prev > sma_20_prev and sma_5 < sma_20:
            message = f"{symbol} is in a **DOWNTREND** — Consider selling."

        # Publish SNS Alert if trend changed
        if message:
            try:
                sns.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Message=message,
                    Subject=f"Stock Alert: {symbol}"
                )
                print("Alert sent:", message)

            except Exception as e:
                print(f"SNS error for {symbol}: {e}")

    return {
        "statusCode": 200,
        "body": json.dumps("Trend analysis complete")
    }
