import json

from hubspot_snowflake_export.handle_deal import handle_deal
from hubspot_snowflake_export.utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
from hubspot_snowflake_export.utils.hubspot_api import get_deal
from hubspot_snowflake_export.utils.s3 import update_deals_last_sync_time
from hubspot_snowflake_export.utils.snowflake_db import create_sf_connection, close_sf_connection


def handle_webhook_from_hubspot(event):
    print("======== Start: Received Webhook from HubSpot ========")
    event_body = event.get('body', None)
    deal_to_update = json.loads(event_body).get('hs_object_id', None)
    if deal_to_update:
        sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
        sf_cursor = sf_conn.cursor()
        try:
            print(f"[Webhook] Deal sync for - {deal_to_update}")
            deal_details = get_deal(deal_to_update)
            handle_deal(deal_details, sf_cursor)
            close_sf_connection(sf_conn)
            update_deals_last_sync_time("HUBSPOT_WEBHOOK", 'SUCCESS')
            return {
                "statusCode": 202,
                "body": json.dumps({"message": f"Success"})
            }
        except Exception:
            close_sf_connection(sf_conn)
            update_deals_last_sync_time("HUBSPOT_WEBHOOK", 'FAILED')
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Failed"})
            }
    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Ok"})
    }
