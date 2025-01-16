import json
import requests
import boto3
import os
from datetime import datetime

def lambda_handler(event, context):
    # Alpha Vantage API parameters
    api_key = os.environ['ALPHA_VANTAGE_API_KEY']  # API key stored as an environment variable
    symbol = event.get('symbol', 'IBM')           # Default stock symbol is IBM
    interval = event.get('interval', '5min')      # Default interval is 5 minutes
    
    # Construct the Alpha Vantage API URL
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}'
    print(f"Generated URL: {url}")
    
    try:
        # Call the Alpha Vantage API
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        # print(f"API Response: {json.dumps(data)}")
        
        # Check if time series data is included in the response
        # better to manual check the url
        time_series_key = f"Time Series ({interval})"
        if time_series_key not in data:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'No time series data found', 'response': data})
            }
        
        # Extract the time series data
        time_series_data = data[time_series_key]
        formatted_data = []
        for timestamp, metrics in time_series_data.items():
            record = {
                "timestamp": timestamp,
                "open": metrics["1. open"],
                "high": metrics["2. high"],
                "low": metrics["3. low"],
                "close": metrics["4. close"],
                "volume": metrics["5. volume"]
            }
            formatted_data.append(record)
        
        # Store the extracted data in S3
        s3 = boto3.client('s3')
        bucket_name = os.environ['S3_BUCKET_NAME']  # S3 bucket name stored as an environment variable
        file_name = f"alpha_vantage_data_{symbol}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        print(f"Bucket Name: {bucket_name}")
        print(f"File Name: {file_name}")
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(formatted_data),
            ContentType='application/json'
        )
        print(f"Data successfully uploaded to S3: {bucket_name}/{file_name}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Data successfully retrieved and stored in S3', 'file_name': file_name})
        }
    
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'API request failed', 'message': str(e)})
        }
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Unexpected error', 'message': str(e)})
        }
