import traceback

from .events import single_deal_fetch, bulk_deals_fetch, back_fill_deals, schedule_fetch
from .utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE
from .utils.snowflake_db import create_sf_connection, close_sf_connection


def lambda_handler(event, context):
    event_job = event['event']
    print(f"Received Event: {event_job}")
    sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
    sf_cursor = sf_conn.cursor()

    try:

        if event_job == 'SCHEDULE_FETCH':
            schedule_fetch(sf_cursor)

        elif event_job == 'SINGLE_DEAL_UPDATE':
            single_deal_fetch(sf_cursor, event)

        elif event_job == 'BACK_FILL_FETCH':
            back_fill_deals(sf_cursor, event)

        elif event_job == 'BULK_DEALS_UPDATE':
            bulk_deals_fetch(sf_cursor, event)

        else:
            print(f"Invalid event: {event_job}")

        close_sf_connection(sf_conn)

    except Exception as ex:
        traceback.print_exc()
        close_sf_connection(sf_conn)

    return "success"
