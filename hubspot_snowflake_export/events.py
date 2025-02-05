from datetime import datetime, timezone, timedelta

from .handle_deal import handle_deal, handle_deal_upsert
from .utils.config import SF_SYNC_INFO_TABLE
from .utils.hubspot_api import fetch_updated_or_created_deals, get_deal
from .utils.snowflake_db import close_sf_connection


def schedule_fetch(sf_cursor):
    curr_time = datetime.now(timezone.utc)
    formatted_datetime = curr_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"Checking Created/Updated Deals at {formatted_datetime}")

    time_31_minutes_ago = curr_time - timedelta(minutes=31)
    search_start_time = time_31_minutes_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

    deals = fetch_updated_or_created_deals(search_start_time)

    if len(deals) > 0:
        print(f"Found {len(deals)} - Created/Updated Deal(s)")
        for deal in deals:
            handle_deal_upsert(deal, sf_cursor)
    else:
        print("No Created/Updated Deals Found. Exiting.")

    return "success"


def single_deal_fetch(sf_cursor, event):
    deal_id = event['deal_id']

    if not deal_id and len(deal_id.trim()) < 0:
        print("Missing DealId in the request. Exiting.")
        return
    deal_details = get_deal(deal_id)
    handle_deal(deal_details, sf_cursor)
    return "success"


def is_valid_datetime(date_str, date_format):
    try:
        datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False


def back_fill_deals(sf_cursor, event):
    sync_from = event.get('sync_from', None)

    if not sync_from or not is_valid_datetime(sync_from, "%Y-%m-%dT%H:%M:%SZ"):
        print("Missing sync_from in the request. Exiting.")
        return

    updated_deals_since = fetch_updated_or_created_deals(sync_from)
    if len(updated_deals_since) > 0:
        print(f"Deals Updated/Created Since: {sync_from} - {len(updated_deals_since)}")
        for deal in updated_deals_since:
            handle_deal_upsert(deal, sf_cursor)
        print(f"Done - Deals Updated/Created Since: {sync_from}")
    else:
        print(f"No Deals Updated/Created Since: {sync_from}")
    return "success"


def sync_all_deals(sf_cursor,sf_conn):
    sync_from = "2024-01-01T01:01:01Z"

    updated_deals_since = fetch_updated_or_created_deals(sync_from)
    if len(updated_deals_since) > 0:
        print(f"Deals Updated/Created Since: {sync_from} - {len(updated_deals_since)}")
        for deal in updated_deals_since:
            handle_deal_upsert(deal, sf_cursor)
        print(f"Done - Deals Updated/Created Since: {sync_from}")
        sync_update_sql = f"""
            MERGE INTO {SF_SYNC_INFO_TABLE} AS target
            USING (VALUES 
                ('DEALS', CURRENT_TIMESTAMP(), 'System', 'SYNC_ALL_FROM_API', 'SUCCESS', NULL)
            ) AS source (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON)
            ON target.ENTITY_NAME = source.ENTITY_NAME
            WHEN MATCHED THEN
                UPDATE SET target.LAST_UPDATED_ON = source.LAST_UPDATED_ON, 
                target.UPDATED_BY = source.UPDATED_BY,
                target.UPDATE_EVENT = source.UPDATE_EVENT,
                target.LAST_SYNC_STATUS = source.LAST_SYNC_STATUS
            WHEN NOT MATCHED THEN
                INSERT (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON)
                VALUES (source.ENTITY_NAME, source.LAST_UPDATED_ON, source.UPDATED_BY, source.UPDATE_EVENT, source.LAST_SYNC_STATUS, source.LAST_FAILED_ON);
            """
        sf_cursor.execute(sync_update_sql)
        close_sf_connection(sf_conn)
    else:
        print(f"No Deals Updated/Created Since: {sync_from}")
    return "success"


def bulk_deals_fetch(sf_cursor, event):
    deal_ids = event['deal_ids']

    if not deal_ids and len(deal_ids) < 1:
        print("Missing DealId(s) in the request. Exiting.")
        return

    for deal_id in deal_ids:
        deal_details = get_deal(deal_id)
        handle_deal(deal_details, sf_cursor)

    return "success"

def handle_sync_status(sf_cursor):
    get_sync_status_sql = f"""
        SELECT SYNC_STATUS, SYNC_START_ON, LAST_UPDATED_ON FROM {SF_SYNC_INFO_TABLE} WHERE ENTITY_NAME='DEALS'
    """
    sf_cursor.execute(get_sync_status_sql)
    sync_data = sf_cursor.fetchone()

    if sync_data[0] == 'PROCESSING':
        return 'PROCESSING'
    else:
        update_sync_status_sql = f"""
            UPDATE {SF_SYNC_INFO_TABLE} SET SYNC_STATUS='PROCESSING', SYNC_START_ON=CURRENT_TIMESTAMP() WHERE ENTITY_NAME='DEALS'
        """
        sf_cursor.execute(update_sync_status_sql)
        return "OK"
