from datetime import datetime, timezone, timedelta

from .handle_deal import handle_deal, handle_deal_upsert
from .utils.hubspot_api import fetch_updated_or_created_deals, get_deal


def schedule_fetch(sf_cursor):
    curr_time = datetime.now(timezone.utc)
    formatted_datetime = curr_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f"Checking Created/Updated Deals at {formatted_datetime}")

    time_6_minutes_ago = curr_time - timedelta(minutes=6)
    search_start_time = time_6_minutes_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

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
    deal_details = get_deal(deal_id)
    handle_deal(deal_details, sf_cursor)
    return "success"

def back_fill_deals(sf_cursor, event):
    return "success"

def bulk_deals_fetch(sf_cursor, event):
    deal_ids = event['deal_ids']

    if not deal_ids and len(deal_ids) < 1:
        print("Missing DealId(s) in the request. Exiting.")

    for deal_id in deal_ids:
        deal_details = get_deal(deal_id)
        handle_deal(deal_details, sf_cursor)

    return "success"