import pandas as pd
from google_play_scraper import app
import boto3
import io
from datetime import datetime, timedelta

def read_app_ids(s3_client, bucket_name, file_path):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')
    return df['App Id'].tolist()

def check_file_exists(s3_client, bucket_name, file_path):
    try:
        s3_client.head_object(Bucket=bucket_name, Key=file_path)
        return True
    except:
        return False

def download_from_s3(s3_client, bucket_name, file_path):
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
    return pd.read_csv(io.BytesIO(obj['Body'].read()), encoding='utf8')

def generate_csv(app_ids, existing_ids, processing_time_limit, start_time):
    columns = [
        "App Name", "App Id", "Category", "Rating", "Rating Count",
        "Reviews", "Installs", "Free",
        "Price", "Developer",
        "Released", "Last Updated", "Content Rating",
        "Ad Supported", "In App Purchases"
    ]
    app_details_list = []

    for app_id in app_ids:
        if datetime.now() - start_time >= processing_time_limit:
            print("Processing time limit reached, stopping data collection.")
            break

        if app_id not in existing_ids:
            try:
                details = app(app_id)

                # Skip apps with a rating of 0
                if details['score'] == 0:
                    continue

                # Formatting 'released' date
                released = details.get('released', 'N/A')
                if isinstance(released, str) and ',' in released:
                    released = datetime.strptime(released, '%b %d, %Y').strftime('%m-%d-%Y')
                else:
                    released = 'N/A'

                # Formatting 'last updated' date
                last_updated = details.get('updated', 'N/A')
                if isinstance(last_updated, int):
                    last_updated = datetime.utcfromtimestamp(last_updated).strftime('%m-%d-%Y')
                else:
                    last_updated = 'N/A'

                app_details_list.append({
                    "App Name": details['title'].replace(',',''),
                    "App Id": app_id,
                    "Category": details['genreId'],
                    "Rating": details['score'],
                    "Rating Count": details['ratings'],
                    "Reviews": details['reviews'],
                    "Installs": details['realInstalls'],
                    "Free": details['free'],
                    "Price": details['price'],
                    "Developer": details['developer'].replace(',',''),
                    "Released": released,
                    "Last Updated": last_updated,
                    "Content Rating": details['contentRating'],
                    "Ad Supported": details['adSupported'],
                    "In App Purchases": details['inAppProductPrice']
                })
            except ValueError as ve:
                print(ve)
            except Exception as e:
                if 'not found' in str(e).lower():
                    print(f"App ID {app_id} not found, skipping.")
                else:
                    print(f"Error fetching details for app ID {app_id}: {e}")

    return pd.DataFrame(app_details_list, columns=columns)

def upload_to_s3(s3_client, bucket_name, file_content, file_path, upload_time_limit, start_time):
    if datetime.now() - start_time >= upload_time_limit:
        print("Overall time limit reached, not uploading CSV.")
        return

    if check_file_exists(s3_client, bucket_name, file_path):
        existing_data = download_from_s3(s3_client, bucket_name, file_path)
        combined_data = pd.concat([existing_data, file_content]).drop_duplicates().reset_index(drop=True)
    else:
        combined_data = file_content

    csv_buffer = io.StringIO()
    combined_data.to_csv(csv_buffer, index=False)
    s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_path)

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    utc_now = datetime.utcnow()  # Current time in UTC
    pacific_now = utc_now - timedelta(hours=8)  # Subtract 8 hours for approximate Pacific Time

    overall_time_limit = timedelta(minutes=14, seconds=55)
    processing_time_limit = timedelta(minutes=14, seconds=25)

    output_filename = f'output.csv'

    input_file_path = 'input/final_realtime_dataset.csv'
    output_file_path = f'output/{output_filename}'

    app_ids = read_app_ids(s3_client, 'googleplaystoresjsu', input_file_path)

    existing_ids = set()
    if check_file_exists(s3_client, 'googleplaystoresjsu', output_file_path):
        existing_data = download_from_s3(s3_client, 'googleplaystoresjsu', output_file_path)
        existing_ids = set(existing_data['App Id'])

    new_data = generate_csv(app_ids, existing_ids, processing_time_limit, utc_now)
    upload_to_s3(s3_client, 'googleplaystoresjsu', new_data, output_file_path, overall_time_limit, utc_now)

    return {
        'statusCode': 200,
        'body': f'CSV file {output_filename} generated and uploaded successfully to the output folder.'
    }