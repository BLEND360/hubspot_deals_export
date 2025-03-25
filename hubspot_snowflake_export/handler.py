import json
import traceback

import boto3

from .bulk_events import sync_deals
from .events import single_deal_fetch, bulk_deals_fetch, back_fill_deals, schedule_fetch, handle_sync_status
from .handle_deal import handle_deal
from .hubspot_events import handle_webhook_from_hubspot
from .utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE, API_AUTH_KEY, \
    AWS_ACCOUNT_ID
from .utils.hubspot_api import get_deal
from .utils.s3 import update_deals_last_sync_time
from .utils.snowflake_db import create_sf_connection, close_sf_connection

sqs = boto3.client('sqs')


def handle_api_request(event):
    print("===>", event)
    headers = event.get('headers', {})
    isHubspotEvent = '/hubspot/deals/sync' in event.get('path', "")
    authorization_header = headers.get('Auth-Key')
    if not isHubspotEvent and authorization_header != API_AUTH_KEY:
        return {
            "statusCode": 401,
            "body": json.dumps({"message": f"Unauthorised"})
        }

    path_params = event.get('pathParameters')
    if path_params and 'dealId' in path_params:
        sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
        sf_cursor = sf_conn.cursor()
        try:
            deal_id = path_params['dealId']
            print(f"[API] Deal sync for - {deal_id}")
            deal_details = get_deal(deal_id)
            handle_deal(deal_details, sf_cursor)

            close_sf_connection(sf_conn)
            return {
                "statusCode": 201,
                "body": json.dumps({"message": f"Completed Sync for Deal: {deal_id}"})
            }
        except Exception:
            close_sf_connection(sf_conn)
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Failed to Sync Deal"})
            }
    elif event['path'] and '/hubspot/deals/sync' in event['path']:
        return handle_webhook_from_hubspot(event, sqs)
    else:
        print("[API] Invoking Async Function - To Sync Deals")
        last_status = handle_sync_status()

        if last_status == "PROCESSING":
            return {
                "statusCode": 201,
                "body": json.dumps({"message": f"Already Sync In Progress"})
            }

        event_body = event.get('body', None)
        sync_from_ = json.loads(event_body).get('sync_from', None)
        if not sync_from_:
            sync_from_ = "2024-01-01T00:00:00+00:00"

        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName=f"arn:aws:lambda:us-east-1:{AWS_ACCOUNT_ID}:function:hubspot-snowflake-export",
            InvocationType="Event",
            Payload=json.dumps(
                {'event': 'MANUAL_SYNC', 'sync_from': sync_from_})
        )

        print("Invoked Lambda with Event", {'event': 'MANUAL_SYNC', 'sync_from': sync_from_})
        return {
            "statusCode": 202,
            "body": json.dumps({"message": f"Accepted - Sync for all Deal"})
        }


def handle_event(event):
    event_job = event['event']
    print(f"Received Event: {event_job}")

    try:
        if event_job == 'SCHEDULE_FETCH':
            schedule_fetch(event_job)

        elif event_job == 'SINGLE_DEAL_UPDATE':
            single_deal_fetch(event)

        elif event_job == 'MANUAL_SYNC':
            sync_deals(event)

        elif event_job == 'BACK_FILL_FETCH':
            back_fill_deals(event)

        elif event_job == 'BULK_DEALS_UPDATE':
            bulk_deals_fetch(event)

        else:
            print(f"Invalid event: {event_job}")

        update_deals_last_sync_time(event_job.upper(), 'SUCCESS')

        return "success"

    except Exception as ex:
        traceback.print_exc()
        update_deals_last_sync_time(event_job.upper(), 'FAILED')
        return "failed"


def lambda_handler(event, context):
    if 'httpMethod' in event:
        return handle_api_request(event)
    else:
        return handle_event(event)
