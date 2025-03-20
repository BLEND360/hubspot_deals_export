import json
import boto3

from hubspot_snowflake_export.bulk_events import sync_deals
from hubspot_snowflake_export.handle_deal import handle_deal
from hubspot_snowflake_export.utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
from hubspot_snowflake_export.utils.hubspot_api import get_deal
from hubspot_snowflake_export.utils.s3 import update_deals_last_sync_time
from hubspot_snowflake_export.utils.snowflake_db import create_sf_connection, close_sf_connection


def lambda_handler(event, context):
    print("Received SQS Batch Size of", len(event['Records']))

    deal_ids = []
    for record in event['Records']:
        message_body = record['body']
        deal_to_update = json.loads(message_body).get('hs_object_id', None)
        if deal_to_update:
            deal_ids.append(deal_to_update)

    if deal_ids and len(deal_ids) < 20:
        sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
        sf_cursor = sf_conn.cursor()
        for deal_id in deal_ids:
            try:
                deal_details = get_deal(deal_id)
                handle_deal(deal_details, sf_cursor)
            except Exception as e:
                print(f"[Webhook] Deal sync failed for - {deal_id}")
        close_sf_connection(sf_conn)
    else:
        sync_deals({"deal_ids": deal_ids})

    update_deals_last_sync_time("HUBSPOT_WEBHOOK", 'SUCCESS')
    return "Success"
