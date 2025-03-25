import json
import traceback
from datetime import datetime, timezone, timedelta

import pytz

from .utils.config import SF_DEALS_TABLE, SF_LINE_ITEMS_TABLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, \
    SF_ROLE
from .utils.hubspot_api import fetch_updated_or_created_deals, get_all_companies, get_all_stages, get_all_owners, \
    get_all_line_items
from .utils.snowflake_db import close_sf_connection, create_sf_connection


def sync_deals(event):
    sync_from = event.get('sync_from', None)
    deal_ids = event.get('deal_ids', [])

    if not sync_from and not deal_ids:
        print("Missing sync_from / deal_ids in the request. Exiting.")
        return

    if sync_from:
        parsed_datetime = datetime.strptime(sync_from, "%Y-%m-%dT%H:%M:%S%z")
        desired_timezone = pytz.timezone('UTC')
        converted_datetime = parsed_datetime.astimezone(desired_timezone)
        formatted_datetime = converted_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        formatted_datetime = None

    if deal_ids:
        deal_ids = list(set(deal_ids))

    updated_deals_since = fetch_updated_or_created_deals(start_date_time=formatted_datetime, deal_ids=deal_ids)
    if len(updated_deals_since) <= 0:
        print(f"No Deals Updated/Created Since: {formatted_datetime}")
        return "success"

    print(f"Deals Updated/Created Since: {formatted_datetime} - {len(updated_deals_since)}")
    deals_with_companies = get_all_companies()
    print("done company details")
    pipeline_stages = get_all_stages()
    print("done pipeline stages")
    owner_details = get_all_owners()
    print("done owner details")
    deals_with_line_items = get_all_line_items()
    print("done line items")
    line_items_deals = [deal_id for deal_id in deals_with_line_items.keys()]
    line_items = []
    for line_items_of_deal in deals_with_line_items.values():
        line_items.extend(line_items_of_deal)
    print("done line items fetch")
    sf_conn = create_sf_connection(SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, SF_ROLE)
    try:
        for line_item in line_items:
            for key in ['price', 'quantity', 'amount']:
                if line_item[key] is not None and line_item[key].strip() == '':
                    print(f"Field {key} is missing for Line Item: {line_item['id']}, value is {line_item[key]}")
                    line_item[key] = None
        sf_cursor = sf_conn.cursor()

        raw_deals = []
        for deal in updated_deals_since:
            deal_id = deal['id']
            # handle_deal_upsert(deal, sf_cursor, deals_with_companies, deals_with_line_items, owner_details, pipeline_stages)
            deal_properties = deal['properties']
            stage_name = pipeline_stages.get(deal_properties["pipeline"], {}).get(deal_properties['dealstage'])

            curr_time = datetime.now(pytz.timezone('America/New_York'))

            if deal_properties['work_ahead'] in ['No', 'blank']:
                work_ahead = 'No'
            else:
                work_ahead = deal_properties['work_ahead']
            deal_owner_details = owner_details.get(deal_properties['hubspot_owner_id'], {})
            delivery_lead_details = owner_details.get(deal_properties['delivery_lead'], {})
            solution_lead_details = owner_details.get(deal_properties['solution_lead'], {})
            company_details = deals_with_companies.get(deal_id, {})
            deal_collaborators_str = deal_properties['hs_all_collaborator_owner_ids']
            deal_collaborators = []
            if deal_collaborators_str:
                deal_collaborators = [owner_details.get(collaborator_id)
                                      for collaborator_id in
                                      deal_collaborators_str.split(";")]

            deal_data_raw = {
                "DEAL_ID": deal_id,
                "DEAL_NAME": deal_properties['dealname'],
                "DEAL_OWNER": json.dumps(deal_owner_details),
                "DEAL_OWNER_ID": deal_properties['hubspot_owner_id'],
                "DEAL_OWNER_EMAIL": deal_owner_details.get('email'),
                "DEAL_OWNER_NAME": deal_owner_details.get('name'),
                "DELIVERY_LEAD_ID": deal_properties['delivery_lead'],
                "DELIVERY_LEAD_EMAIL": delivery_lead_details.get('email'),
                "DELIVERY_LEAD_NAME": delivery_lead_details.get('name'),
                "SOLUTION_LEAD_ID": deal_properties['solution_lead'],
                "SOLUTION_LEAD_EMAIL": solution_lead_details.get('email'),
                "SOLUTION_LEAD_NAME": solution_lead_details.get('name'),
                "DEAL_STAGE_ID": deal_properties['dealstage'],
                "DEAL_STAGE_NAME": stage_name,
                "COMPANY_ID": company_details.get('id'),
                "COMPANY_NAME": company_details.get('name', None),
                "DEAL_TO_COMPANY_ASSOCIATIONS": json.dumps(company_details),
                "PIPELINE_ID": deal_properties['pipeline'],
                "PROJECT_START_DATE": deal_properties['expected_project_start_date'],
                "PROJECT_CLOSE_DATE": deal_properties['closedate'],
                "ENGAGEMENT_TYPE": deal_properties['engagement_type__cloned_'],
                "DURATION_IN_MONTHS": deal_properties['expected_project_duration_in_months'],
                "DEAL_COLLABORATORS": json.dumps(deal_collaborators),
                "DEAL_CREATED_ON": deal_properties['hs_createdate'],
                "DEAL_UPDATED_ON": deal_properties['hs_lastmodifieddate'],
                "IS_ARCHIVED": False,
                "COMPANY_DOMAIN": company_details.get('domain'),
                "NS_PROJECT_ID": deal_properties['ns_project_id__finance_only_'],
                "DEAL_AMOUNT_IN_COMPANY_CURRENCY": deal_properties['amount_in_home_currency'],
                "DEAL_TYPE": deal_properties['dealtype'],
                "SPECIAL_FIELDS_UPDATED_ON": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "WORK_AHEAD": work_ahead,
                "LAST_REFRESHED_ON": curr_time
            }

            timestamp_fields = [
                'PROJECT_START_DATE', 'PROJECT_CLOSE_DATE', 'DEAL_CREATED_ON',
                'DEAL_UPDATED_ON', 'SPECIAL_FIELDS_UPDATED_ON', 'LAST_REFRESHED_ON'
            ]
            for field in timestamp_fields:
                if deal_data_raw.get(field) is not None and str(deal_data_raw.get(field)).strip() == '':
                    print(f"Field {field} is missing for Deal: {deal_id}, value is {deal_data_raw.get(field)}")
                    deal_data_raw[field] = None

            number_fields = ['COMPANY_ID', 'DURATION_IN_MONTHS', 'DEAL_AMOUNT_IN_COMPANY_CURRENCY']
            for field in number_fields:
                if deal_data_raw.get(field) is not None and str(deal_data_raw.get(field)).strip() == '':
                    print(f"Field {field} is missing for Deal: {deal_id}, value is {deal_data_raw.get(field)}")
                    deal_data_raw[field] = None

            raw_deals.append(deal_data_raw)
        print("done raw deals")
        #     create temp table for upsert
        sf_cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE DEALS_TEMP LIKE {SF_DEALS_TABLE}")
        # insert this data into temp table
        print("Inserting data into temp table")
        sf_cursor.executemany("""INSERT INTO DEALS_TEMP (DEAL_ID, DEAL_NAME, DEAL_OWNER, DEAL_OWNER_ID,
            DEAL_OWNER_EMAIL, DEAL_OWNER_NAME, DEAL_STAGE_ID, DEAL_STAGE_NAME, COMPANY_ID, COMPANY_NAME,
            DEAL_TO_COMPANY_ASSOCIATIONS, PIPELINE_ID, PROJECT_START_DATE, PROJECT_CLOSE_DATE, ENGAGEMENT_TYPE,
            DURATION_IN_MONTHS, DEAL_COLLABORATORS, DEAL_CREATED_ON, DEAL_UPDATED_ON, IS_ARCHIVED, COMPANY_DOMAIN,
            NS_PROJECT_ID, DEAL_AMOUNT_IN_COMPANY_CURRENCY, DEAL_TYPE, SPECIAL_FIELDS_UPDATED_ON, WORK_AHEAD,
            LAST_REFRESHED_ON, DELIVERY_LEAD_ID, DELIVERY_LEAD_EMAIL, DELIVERY_LEAD_NAME, SOLUTION_LEAD_ID,
            SOLUTION_LEAD_EMAIL, SOLUTION_LEAD_NAME)
             VALUES 
            (%(DEAL_ID)s, %(DEAL_NAME)s, %(DEAL_OWNER)s, %(DEAL_OWNER_ID)s, %(DEAL_OWNER_EMAIL)s,
            %(DEAL_OWNER_NAME)s, %(DEAL_STAGE_ID)s, %(DEAL_STAGE_NAME)s, %(COMPANY_ID)s, %(COMPANY_NAME)s,
            %(DEAL_TO_COMPANY_ASSOCIATIONS)s, %(PIPELINE_ID)s, %(PROJECT_START_DATE)s, %(PROJECT_CLOSE_DATE)s,
            %(ENGAGEMENT_TYPE)s, %(DURATION_IN_MONTHS)s, %(DEAL_COLLABORATORS)s, %(DEAL_CREATED_ON)s,
            %(DEAL_UPDATED_ON)s, %(IS_ARCHIVED)s, %(COMPANY_DOMAIN)s, %(NS_PROJECT_ID)s,
            %(DEAL_AMOUNT_IN_COMPANY_CURRENCY)s, %(DEAL_TYPE)s, CURRENT_TIMESTAMP(), %(WORK_AHEAD)s,
            CURRENT_TIMESTAMP(), %(DELIVERY_LEAD_ID)s, %(DELIVERY_LEAD_EMAIL)s, %(DELIVERY_LEAD_NAME)s,
            %(SOLUTION_LEAD_ID)s, %(SOLUTION_LEAD_EMAIL)s, %(SOLUTION_LEAD_NAME)s)""",
                              raw_deals)
        # upsert from temp table to main table
        print("Upserting data into main table")
        sf_cursor.execute(f"""
            MERGE INTO {SF_DEALS_TABLE} AS target
            USING DEALS_TEMP AS source
            ON target.DEAL_ID = source.DEAL_ID
            WHEN MATCHED THEN
                UPDATE SET target.DEAL_NAME = source.DEAL_NAME,
                target.DEAL_OWNER = source.DEAL_OWNER,
                target.DEAL_OWNER_ID = source.DEAL_OWNER_ID,
                target.DEAL_OWNER_EMAIL = source.DEAL_OWNER_EMAIL,
                target.DEAL_OWNER_NAME = source.DEAL_OWNER_NAME,
                target.DEAL_STAGE_ID = source.DEAL_STAGE_ID,
                target.DEAL_STAGE_NAME = source.DEAL_STAGE_NAME,
                target.COMPANY_ID = source.COMPANY_ID,
                target.COMPANY_NAME = source.COMPANY_NAME,
                target.DEAL_TO_COMPANY_ASSOCIATIONS = source.DEAL_TO_COMPANY_ASSOCIATIONS,
                target.PIPELINE_ID = source.PIPELINE_ID,
                target.PROJECT_START_DATE = source.PROJECT_START_DATE,
                target.PROJECT_CLOSE_DATE = source.PROJECT_CLOSE_DATE,
                target.ENGAGEMENT_TYPE = source.ENGAGEMENT_TYPE,
                target.DURATION_IN_MONTHS = source.DURATION_IN_MONTHS,
                target.DEAL_COLLABORATORS = source.DEAL_COLLABORATORS,
                target.DEAL_CREATED_ON = source.DEAL_CREATED_ON,
                target.DEAL_UPDATED_ON = source.DEAL_UPDATED_ON,
                target.IS_ARCHIVED = source.IS_ARCHIVED,
                target.COMPANY_DOMAIN = source.COMPANY_DOMAIN,
                target.NS_PROJECT_ID = source.NS_PROJECT_ID,
                target.DEAL_AMOUNT_IN_COMPANY_CURRENCY = source.DEAL_AMOUNT_IN_COMPANY_CURRENCY,
                target.DEAL_TYPE = source.DEAL_TYPE,
                target.SPECIAL_FIELDS_UPDATED_ON = source.SPECIAL_FIELDS_UPDATED_ON,
                target.WORK_AHEAD = source.WORK_AHEAD,
                target.LAST_REFRESHED_ON = source.LAST_REFRESHED_ON,
                target.DELIVERY_LEAD_ID = source.DELIVERY_LEAD_ID,
                target.DELIVERY_LEAD_EMAIL = source.DELIVERY_LEAD_EMAIL,
                target.DELIVERY_LEAD_NAME = source.DELIVERY_LEAD_NAME,
                target.SOLUTION_LEAD_ID = source.SOLUTION_LEAD_ID,
                target.SOLUTION_LEAD_EMAIL = source.SOLUTION_LEAD_EMAIL,
                target.SOLUTION_LEAD_NAME = source.SOLUTION_LEAD_NAME
            WHEN NOT MATCHED THEN
                INSERT (DEAL_ID, DEAL_NAME, DEAL_OWNER, DEAL_OWNER_ID, DEAL_OWNER_EMAIL, DEAL_OWNER_NAME,
                DEAL_STAGE_ID, DEAL_STAGE_NAME, COMPANY_ID, COMPANY_NAME, DEAL_TO_COMPANY_ASSOCIATIONS,
                PIPELINE_ID, PROJECT_START_DATE, PROJECT_CLOSE_DATE, ENGAGEMENT_TYPE, DURATION_IN_MONTHS,
                DEAL_COLLABORATORS, DEAL_CREATED_ON, DEAL_UPDATED_ON, IS_ARCHIVED, COMPANY_DOMAIN, NS_PROJECT_ID,
                DEAL_AMOUNT_IN_COMPANY_CURRENCY, DEAL_TYPE, SPECIAL_FIELDS_UPDATED_ON, WORK_AHEAD, LAST_REFRESHED_ON,
                DELIVERY_LEAD_ID, DELIVERY_LEAD_EMAIL, DELIVERY_LEAD_NAME, SOLUTION_LEAD_ID, SOLUTION_LEAD_EMAIL,
                SOLUTION_LEAD_NAME)
                VALUES (source.DEAL_ID, source.DEAL_NAME, source.DEAL_OWNER, source.DEAL_OWNER_ID,
                source.DEAL_OWNER_EMAIL, source.DEAL_OWNER_NAME, source.DEAL_STAGE_ID, source.DEAL_STAGE_NAME,
                source.COMPANY_ID, source.COMPANY_NAME, source.DEAL_TO_COMPANY_ASSOCIATIONS, source.PIPELINE_ID,
                source.PROJECT_START_DATE, source.PROJECT_CLOSE_DATE, source.ENGAGEMENT_TYPE, source.DURATION_IN_MONTHS,
                source.DEAL_COLLABORATORS, source.DEAL_CREATED_ON, source.DEAL_UPDATED_ON, source.IS_ARCHIVED,
                source.COMPANY_DOMAIN, source.NS_PROJECT_ID, source.DEAL_AMOUNT_IN_COMPANY_CURRENCY, source.DEAL_TYPE,
                source.SPECIAL_FIELDS_UPDATED_ON, source.WORK_AHEAD, source.LAST_REFRESHED_ON, source.DELIVERY_LEAD_ID,
                source.DELIVERY_LEAD_EMAIL, source.DELIVERY_LEAD_NAME, source.SOLUTION_LEAD_ID,
                source.SOLUTION_LEAD_EMAIL, source.SOLUTION_LEAD_NAME)
        """
                          )
        print(f"Done - Deals Updated/Created Since: {sync_from}")
        # #####################################################################
        sf_cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE LINE_ITEMS_TEMP LIKE {SF_LINE_ITEMS_TABLE}")
        sf_cursor.executemany("""INSERT INTO LINE_ITEMS_TEMP (LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID)
            VALUES (%(id)s, %(name)s, %(price)s, %(quantity)s, %(amount)s, %(created_at)s, %(updated_at)s, %(deal_id)s)""",
                              line_items)
        sf_cursor.execute(f"DELETE FROM {SF_LINE_ITEMS_TABLE} WHERE DEAL_ID IN (%(line_items_deals)s)",
                          {'line_items_deals': line_items_deals})

        sf_cursor.execute(f"""
            MERGE INTO {SF_LINE_ITEMS_TABLE} AS target
            USING LINE_ITEMS_TEMP AS source
            ON target.LINE_ITEM_ID = source.LINE_ITEM_ID
            WHEN MATCHED THEN
                UPDATE SET target.NAME = source.NAME,
                target.PRICE = source.PRICE,
                target.QUANTITY = source.QUANTITY,
                target.AMOUNT = source.AMOUNT,
                target.CREATED_ON = source.CREATED_ON,
                target.UPDATED_ON = source.UPDATED_ON,
                target.DEAL_ID = source.DEAL_ID
            WHEN NOT MATCHED THEN
                INSERT (LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID)
                VALUES (source.LINE_ITEM_ID, source.NAME, source.PRICE, source.QUANTITY, source.AMOUNT,
                source.CREATED_ON, source.UPDATED_ON, source.DEAL_ID)
        """
                          )
        print("done line items insert")

        # #####################################################################
    except Exception as ex:
        print(traceback.format_exc())
        sf_conn.rollback()
        print(f"Failed Sync - {ex}")
        raise

    finally:
        sf_conn.commit()
        close_sf_connection(sf_conn)
