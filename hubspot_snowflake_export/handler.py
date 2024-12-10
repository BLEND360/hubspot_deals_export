import traceback
from datetime import datetime, timezone

from .events import single_deal_fetch, bulk_deals_fetch, back_fill_deals, schedule_fetch
from .utils.config import SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE, SF_SYNC_INFO_TABLE
from .utils.snowflake_db import create_sf_connection, close_sf_connection


def lambda_handler(event, context):
    event_job = event['event']
    print(f"Received Event: {event_job}")
    sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
    sf_cursor = sf_conn.cursor()

    curr_time = datetime.now(timezone.utc)
    formatted_datetime = curr_time.strftime('%Y-%m-%dT%H:%M:%SZ')

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

        sync_update_sql = f"""
            MERGE INTO {SF_SYNC_INFO_TABLE} AS target
            USING (VALUES 
                ('DEALS', '{formatted_datetime}', 'System', '{event_job.upper()}', 'SUCCESS', NULL)
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

    except Exception as ex:
        traceback.print_exc()
        sync_failed_sql = f"""
            MERGE INTO {SF_SYNC_INFO_TABLE} AS target
            USING (VALUES 
                ('DEALS', '{formatted_datetime}', 'System', '{event_job.upper()}', 'FAILED', '{formatted_datetime}')
            ) AS source (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON)
            ON target.ENTITY_NAME = source.ENTITY_NAME
            WHEN MATCHED THEN
                UPDATE SET target.LAST_UPDATED_ON = source.LAST_UPDATED_ON, 
                target.UPDATED_BY = source.UPDATED_BY,
                target.UPDATE_EVENT = source.UPDATE_EVENT,
                target.LAST_SYNC_STATUS = source.LAST_SYNC_STATUS,
                target.LAST_FAILED_ON = source.LAST_FAILED_ON
            WHEN NOT MATCHED THEN
                INSERT (ENTITY_NAME, LAST_UPDATED_ON, UPDATED_BY, UPDATE_EVENT, LAST_SYNC_STATUS, LAST_FAILED_ON)
                VALUES (source.ENTITY_NAME, source.LAST_UPDATED_ON, source.UPDATED_BY, source.UPDATE_EVENT, source.LAST_SYNC_STATUS, source.LAST_FAILED_ON);
            """
        sf_cursor.execute(sync_failed_sql)
        close_sf_connection(sf_conn)

    return "success"
