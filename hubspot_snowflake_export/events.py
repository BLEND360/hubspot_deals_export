from datetime import datetime

from .handle_deal import handle_deal, handle_deal_upsert
from .utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
from .utils.hubspot_api import fetch_updated_or_created_deals, get_deal
from .utils.s3 import get_deals_last_sync_info, update_deals_last_sync_time, set_deal_sync_status
from .utils.snowflake_db import close_sf_connection, create_sf_connection


def schedule_fetch(event_job):

    last_sync_info = get_deals_last_sync_info()
    parsed_datetime = datetime.strptime(last_sync_info['last_updated_on'], "%Y-%m-%dT%H:%M:%S.%f%z")
    last_updated_on = parsed_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        deals = fetch_updated_or_created_deals(last_updated_on)

        if len(deals) > 0:
            print(f"Found {len(deals)} - Created/Updated Deal(s) since {last_updated_on}")
            sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
            sf_cursor = sf_conn.cursor()
            try:
                for deal in deals:
                    handle_deal_upsert(deal, sf_cursor)
                close_sf_connection(sf_conn)
                update_deals_last_sync_time(event_job.upper(), "SUCCESS")
                print(f"Updated {len(deals)} - Created/Updated Deal(s) since {last_updated_on}")
            except Exception as ex:
                close_sf_connection(sf_conn)
                raise ex
        else:
            print("No Created/Updated Deals Found. Exiting.")
        return "success"

    except Exception as ex:
        print(f"Failed Sync - {ex}")
        return "failed"



def is_valid_datetime(date_str, date_format):
    try:
        datetime.strptime(date_str, date_format)
        return True
    except ValueError:
        return False


def back_fill_deals(event):
    sync_from = event.get('sync_from', None)

    if not sync_from or not is_valid_datetime(sync_from, "%Y-%m-%dT%H:%M:%SZ"):
        print("Missing sync_from in the request. Exiting.")
        return

    updated_deals_since = fetch_updated_or_created_deals(sync_from)
    if len(updated_deals_since) > 0:
        print(f"Deals Updated/Created Since: {sync_from} - {len(updated_deals_since)}")
        sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
        sf_cursor = sf_conn.cursor()
        try:
            for deal in updated_deals_since:
                handle_deal_upsert(deal, sf_cursor)
            close_sf_connection(sf_conn)
            print(f"Done - Deals Updated/Created Since: {sync_from}")
        except Exception as ex:
            close_sf_connection(sf_conn)
            raise ex
    else:
        print(f"No Deals Updated/Created Since: {sync_from}")
    return "success"

def sync_deals(event):
    sync_from = event.get('sync_from', None)

    if not sync_from:
        print("Missing sync_from in the request. Exiting.")
        return

    parsed_datetime = datetime.strptime(sync_from, "%Y-%m-%dT%H:%M:%S.%f%z")
    last_updated_on = parsed_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        updated_deals_since = fetch_updated_or_created_deals(last_updated_on)
        if len(updated_deals_since) > 0:
            print(f"Deals Updated/Created Since: {last_updated_on} - {len(updated_deals_since)}")
            sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
            sf_cursor = sf_conn.cursor()
            try:
                for deal in updated_deals_since:
                    handle_deal_upsert(deal, sf_cursor)
                print(f"Done - Deals Updated/Created Since: {sync_from}")
                close_sf_connection(sf_conn)
            except Exception as ex:
                close_sf_connection(sf_conn)
                raise ex
        else:
            print(f"No Deals Updated/Created Since: {sync_from}")
        return "success"
    except Exception as ex:
        set_deal_sync_status("FAILED")
        print(f"Failed Sync - {ex}")
        return "failed"


###____SINGLE/MULTIPLE DEALS____###

def single_deal_fetch(event):
    deal_id = event['deal_id']

    if not deal_id and len(deal_id.trim()) < 0:
        print("Missing DealId in the request. Exiting.")
        return
    sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
    sf_cursor = sf_conn.cursor()
    try:
        deal_details = get_deal(deal_id)
        handle_deal(deal_details, sf_cursor)
        close_sf_connection(sf_conn)
        return "success"
    except Exception as ex:
        close_sf_connection(sf_conn)
        print(f"Failed Sync - {ex}")
        return "failed"


def bulk_deals_fetch(event):
    deal_ids = event['deal_ids']

    if not deal_ids and len(deal_ids) < 1:
        print("Missing DealId(s) in the request. Exiting.")
        return
    sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
    sf_cursor = sf_conn.cursor()
    try:
        for deal_id in deal_ids:
            deal_details = get_deal(deal_id)
            handle_deal(deal_details, sf_cursor)
        close_sf_connection(sf_conn)
    except Exception as ex:
        close_sf_connection(sf_conn)
        print(f"Failed Sync - {ex}")
        return "failed"

    return "success"


def handle_sync_status():
    last_sync_info = get_deals_last_sync_info()

    if last_sync_info['sync_status'] == 'PROCESSING':
        return 'PROCESSING'
    else:
        set_deal_sync_status("PROCESSING", last_sync_info)
        return last_sync_info['last_updated_on']
