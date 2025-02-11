import json
from datetime import datetime, timedelta

import boto3
import traceback

import pytz

from .events import single_deal_fetch, bulk_deals_fetch, back_fill_deals, sync_deals, schedule_fetch, handle_sync_status
from .handle_deal import handle_deal
from .utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE, SF_SYNC_INFO_TABLE, API_AUTH_KEY
from .utils.hubspot_api import get_deal
from .utils.snowflake_db import create_sf_connection, close_sf_connection


def lambda_handler(event, context):
    sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
    sf_cursor = sf_conn.cursor()

    if 'httpMethod' in event:

        headers = event.get('headers', {})
        authorization_header = headers.get('Auth-Key')
        if authorization_header != API_AUTH_KEY:
            return {
                "statusCode": 401,
                "body": json.dumps({"message": f"Unauthorised"})
            }

        path_params = event.get('pathParameters')
        if path_params and 'dealId' in path_params:
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
        else:
            print("[API] Invoking Async Function - To Sync Deals")
            last_status = handle_sync_status(sf_cursor)

            if last_status == "PROCESSING":
                return {
                    "statusCode": 201,
                    "body": json.dumps({"message": f"Already Sync In Progress"})
                }

            timestamp = datetime.fromisoformat(str(last_status))
            utc_timestamp = timestamp.astimezone(pytz.UTC)
            lambda_client = boto3.client('lambda')
            lambda_client.invoke(
                FunctionName="arn:aws:lambda:us-east-1:739817132544:function:hubspot-snowflake-export",
                InvocationType="Event",
                Payload=json.dumps(
                    {'event': 'MANUAL_SYNC', 'sync_from': str(utc_timestamp - timedelta(minutes=2))})
            )

            print("Invoked Lambda with Event", {'event': 'MANUAL_SYNC', 'sync_from': str(utc_timestamp - timedelta(minutes=2))})
            return {
                "statusCode": 202,
                "body": json.dumps({"message": f"Accepted - Sync for all Deal"})
            }

    event_job = event['event']
    print(f"Received Event: {event_job}")

    try:

        if event_job == 'SCHEDULE_FETCH':
            schedule_fetch(sf_cursor)

        elif event_job == 'SINGLE_DEAL_UPDATE':
            single_deal_fetch(sf_cursor, event)

        elif event_job == 'MANUAL_SYNC':
            sync_deals(sf_cursor, event)

        elif event_job == 'BACK_FILL_FETCH':
            back_fill_deals(sf_cursor, event)

        elif event_job == 'BULK_DEALS_UPDATE':
            bulk_deals_fetch(sf_cursor, event)

        else:
            print(f"Invalid event: {event_job}")

        sync_update_sql = f"""
            MERGE INTO {SF_SYNC_INFO_TABLE} AS target
            USING (VALUES 
                ('DEALS', CURRENT_TIMESTAMP(), 'System', '{event_job.upper()}', 'SUCCESS', NULL, 'COMPLETED')
            ) AS source (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON, SYNC_STATUS)
            ON target.ENTITY_NAME = source.ENTITY_NAME
            WHEN MATCHED THEN
                UPDATE SET target.LAST_UPDATED_ON = source.LAST_UPDATED_ON, 
                target.UPDATED_BY = source.UPDATED_BY,
                target.UPDATE_EVENT = source.UPDATE_EVENT,
                target.LAST_SYNC_STATUS = source.LAST_SYNC_STATUS,
                target.SYNC_STATUS = source.SYNC_STATUS
            WHEN NOT MATCHED THEN
                INSERT (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON)
                VALUES (source.ENTITY_NAME, source.LAST_UPDATED_ON, source.UPDATED_BY, source.UPDATE_EVENT, source.LAST_SYNC_STATUS, source.LAST_FAILED_ON);
            """
        sf_cursor.execute(sync_update_sql)

        close_sf_connection(sf_conn)

    except Exception as ex:
        traceback.print_exc()
        sync_failed_sql = f"""
            MERGE INTO {SF_SYNC_INFO_TABLE} AS target
            USING (VALUES 
                ('DEALS', CURRENT_TIMESTAMP(), 'System', '{event_job.upper()}', 'FAILED', CURRENT_TIMESTAMP(), 'COMPLETED')
            ) AS source (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON, SYNC_STATUS)
            ON target.ENTITY_NAME = source.ENTITY_NAME
            WHEN MATCHED THEN
                UPDATE SET target.LAST_UPDATED_ON = source.LAST_UPDATED_ON, 
                target.UPDATED_BY = source.UPDATED_BY,
                target.UPDATE_EVENT = source.UPDATE_EVENT,
                target.LAST_SYNC_STATUS = source.LAST_SYNC_STATUS,
                target.LAST_FAILED_ON = source.LAST_FAILED_ON,
                target.SYNC_STATUS = source.SYNC_STATUS
            WHEN NOT MATCHED THEN
                INSERT (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON)
                VALUES (source.ENTITY_NAME, source.LAST_UPDATED_ON, source.UPDATED_BY, source.UPDATE_EVENT, source.LAST_SYNC_STATUS, source.LAST_FAILED_ON);
            """
        sf_cursor.execute(sync_failed_sql)
        close_sf_connection(sf_conn)

    return "success"
