import json
import traceback

from hubspot_snowflake_export.bulk_events import sync_deals
from hubspot_snowflake_export.handle_deal import handle_deal
from hubspot_snowflake_export.utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
from hubspot_snowflake_export.utils.hubspot_api import get_deal
from hubspot_snowflake_export.utils.s3 import update_deals_last_sync_time
from hubspot_snowflake_export.utils.send_mail import send_email
from hubspot_snowflake_export.utils.snowflake_db import create_sf_connection, close_sf_connection


def lambda_handler(event, context):
    print("Received SQS Batch Size of", len(event['Records']))

    deal_ids = []
    for record in event['Records']:
        message_body = record['body']
        deal_to_update = json.loads(message_body).get('hs_object_id', None)
        if deal_to_update:
            deal_ids.append(deal_to_update)

    deal_ids = list(set(deal_ids))

    if deal_ids and len(deal_ids) < 20:
        sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
        sf_cursor = sf_conn.cursor()
        for deal_id in deal_ids:
            try:
                deal_details = get_deal(deal_id)
                handle_deal(deal_details, sf_cursor)
            except Exception as e:
                error_log = traceback.format_exc()
                html_content = f'''
                <h1>Hubspot Sync Failed for Deal - {deal_id}</h1><br>
                <h2>Error:</h2><br>
                <b>{str(e)}</b><br>
                <pre>{error_log}</pre>
                '''
                send_email(["Ramakrishna.Pinni@blend360.com"], subject="Hubspot Sync Failed error logs",
                           content=html_content, content_type="html",
                           email_cc_list=[], importance=True)
                print(f"[Webhook] Deal sync failed for - {deal_id}")
        close_sf_connection(sf_conn)
    else:
        try:
            sync_deals({"deal_ids": deal_ids})
        except Exception as e:
            error_log = traceback.format_exc()
            html_content = f'''
            <h1>Hubspot Sync Failed for Deals</h1><br>
            <b>Deals: {deal_ids}</b><br>
            <h2>Error:</h2><br>
            <b>{str(e)}</b><br>
            <pre>{error_log}</pre>
            '''
            send_email(["Ramakrishna.Pinni@blend360.com"], subject="Hubspot Sync Failed error logs",
                       content=html_content, content_type="html",
                       email_cc_list=[], importance=True)

    update_deals_last_sync_time("HUBSPOT_WEBHOOK", 'SUCCESS')
    return "Success"
