import json
from datetime import datetime

import boto3
import pytz

bucket_name = 'hubspot-deals-info-prod'
file_key = 'deals-sync-info.json'

def get_deals_last_sync_info():
    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        sync_info = json.loads(file_content)

        return sync_info

    except Exception as e:
        print(f"Error accessing S3: {e}")
        return None


def update_deals_last_sync_time(event_name, status):
    s3 = boto3.client('s3')

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        sync_info = json.loads(file_content)
        sync_info['update_event'] = event_name
        sync_info['last_sync_status'] = status
        sync_info['sync_status'] = "COMPLETED"
        sync_info['last_updated_on'] = datetime.now(pytz.timezone('America/New_York')).isoformat()

        updated_json_content = json.dumps(sync_info)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=updated_json_content.encode('utf-8'))

        print("Updated Sync Status to S3")
        return "success"

    except Exception as e:
        print(f"Error updating S3: {e}")
        return None


def set_deal_sync_status(sync_status, sync_info=None):
    s3 = boto3.client('s3')

    try:
        if not sync_info:
            response = s3.get_object(Bucket=bucket_name, Key=file_key)
            file_content = response['Body'].read().decode('utf-8')
            sync_info = json.loads(file_content)
        sync_info['sync_status'] = sync_status

        updated_json_content = json.dumps(sync_info)
        s3.put_object(Bucket=bucket_name, Key=file_key, Body=updated_json_content.encode('utf-8'))

        print(f"Updated Sync Status - {sync_status} - to S3")
        return "success"

    except Exception as e:
        print(f"Error updating S3: {e}")
        return None