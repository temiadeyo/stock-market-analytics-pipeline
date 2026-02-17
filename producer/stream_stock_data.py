import boto3
import json
import time
import yfinance as yf

# AWS Kinesis Configuration
kinesis_client = boto3.client('kinesis', region_name='us-east-1')
STREAM_NAME = "stock-market-stream" #  Your kinesis stream name
STOCK_SYMBOLS = ["AAPL", "MSFT", "AMZN", "NVDA", "GOOGL"] # Multiple stock symbols
DELAY_TIME = 30  #Time delay in seconds

# Function to fetch stock data
def get_stock_data(symbol):
    try:
        stock = yf.Ticker(symbol)
        data = stock.history(period="2d") # Fetch last 2 days to get previous close

        if len(data) < 2:
            raise ValueError("Not enough data")

        latest = data.iloc[-1]
        previous = data.iloc[-2]

        return {
            "symbol": symbol,
            "open": round(latest["Open"], 2),
            "high": round(latest["High"], 2),
            "low": round(latest["Low"], 2),
            "price": round(latest["Close"], 2),
            "previous_close": round(previous["Close"], 2),
            "change": round(latest["Close"] - previous["Close"], 2),
            "change_percent": round(((latest["Close"] - previous["Close"]) / previous["Close"]) * 100, 2),
            "volume": int(latest["Volume"]),
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }

    except Exception as e:
        print(f"{symbol} error: {e}")
        return None

# Function to stream data into Kinesis
def send_to_kinesis():
    while True:
        for symbol in STOCK_SYMBOLS:
            stock_data = get_stock_data(symbol)

            if stock_data is None:
                continue

            print(f"Sending {symbol}: {stock_data}")
            
            # Send to Kinesis
            try:
                response = kinesis_client.put_record(
                    StreamName=STREAM_NAME,
                    Data=json.dumps(stock_data),
                    PartitionKey=symbol   # Keeps each stock ordered
                )
                
                # Debugging Response
                if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                    print(f"{symbol} sent successfully")
                else:
                    print(f"{symbol} failed: {response}")

            except Exception as e:
                print(f"Kinesis error for {symbol}: {e}")

        print("---- cycle complete ----\n")
        time.sleep(DELAY_TIME)

# Run the streaming function
send_to_kinesis()
