import json
import traceback
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import pytz

from .bulk_events import get_2026_book_lead_email
from .utils.config import SF_DEALS_TABLE, SF_LINE_ITEMS_TABLE, SF_WAREHOUSE, SF_DATABASE, SF_SCHEMA, \
    SF_ROLE
from .utils.hubspot_api import fetch_updated_or_created_deals, get_all_stages, get_all_owners, \
    get_associated_companies_of_deals, \
    get_associated_line_items_of_deals, get_line_items_by_ids_batch, get_companies_by_ids_batch, \
    get_owners_by_ids_users_search, get_associated_contacts_of_deals, get_contacts_by_ids_batch, \
    get_files_by_ids_search, get_associated_msas_for_deals, get_msas_by_ids_batch, \
    build_msa_details_json, select_msa_for_deal
from .utils.snowflake_db import close_sf_connection, create_sf_connection

def get_list_of_owner_ids(deals):
    owner_ids = set()
    for deal in deals:
        if deal['properties']['hubspot_owner_id']:
            owner_ids.add(deal['properties']['hubspot_owner_id'])
        if deal['properties']['delivery_lead']:
            owner_ids.add(deal['properties']['delivery_lead'])
        if deal['properties']['solution_lead']:
            owner_ids.add(deal['properties']['solution_lead'])
        if deal['properties']['hs_all_collaborator_owner_ids']:
            owner_ids.update(deal['properties']['hs_all_collaborator_owner_ids'].split(';'))
    return list(owner_ids)


def get_list_of_sales_deck_file_ids(deals):
    file_ids = set()
    for deal in deals:
        sales_decks = deal['properties'].get('sales_decks__presentations')
        if sales_decks:
            file_ids.update(fid.strip() for fid in sales_decks.split(';') if fid.strip())
    return list(file_ids)


def get_sales_decks_names(file_ids_str, file_names_by_id):
    """Map semicolon-separated file ids to a JSON list of {id, name} dicts using a prefetched lookup (bulk style)."""
    if not file_ids_str:
        return None
    result = []
    for fid in file_ids_str.split(";"):
        fid = fid.strip()
        if not fid:
            continue
        result.append({"id": fid, "name": file_names_by_id.get(fid)})
    return json.dumps(result) if result else None


def sync_deals(event):
    sync_from = event.get('sync_from', None)
    deal_ids = event.get('deal_ids', [])

    if not sync_from and not deal_ids:
        print("Missing sync_from / deal_ids in the request. Exiting.")
        return
    formatted_datetime = None
    updated_deals_since = []
    if sync_from:
        parsed_datetime = datetime.strptime(sync_from, "%Y-%m-%dT%H:%M:%S%z")
        desired_timezone = pytz.timezone('UTC')
        converted_datetime = parsed_datetime.astimezone(desired_timezone)
        formatted_datetime = converted_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")
        updated_deals_since = fetch_updated_or_created_deals(start_date_time=formatted_datetime)

    elif deal_ids:
        deal_ids = list(set(deal_ids))
        deals_as_batch_of_100 =  [deal_ids[i:i + 100] for i in range(0, len(deal_ids), 100)]
        for deal_ids_batch in deals_as_batch_of_100:
            updated_deals_since.extend(fetch_updated_or_created_deals(start_date_time=formatted_datetime, deal_ids=deal_ids_batch))


    if len(updated_deals_since) <= 0:
        print(f"No Deals Updated/Created Since: {formatted_datetime}")
        return
    list_of_owner_ids = get_list_of_owner_ids(updated_deals_since)
    print(f"Deals Updated/Created Since: {formatted_datetime} - {len(updated_deals_since)}")
    deal_ids = [deal['id'] for deal in updated_deals_since]
    deals_to_associated_company_ids = get_associated_companies_of_deals(deal_ids)
    # company_id_to_deal_id_mapping = {v:k for k, v in deals_to_associated_company_ids.items()}
    company_ids = list(set(deals_to_associated_company_ids.values()))
    company_details = get_companies_by_ids_batch(company_ids)
    deals_with_companies = {deal_id: company_details.get(deals_to_associated_company_ids.get(deal_id), {}) for deal_id in deal_ids}
    # deals_with_companies = get_all_companies()
    print("done company details")
    deals_to_associated_contact_ids = get_associated_contacts_of_deals(deal_ids)
    all_contact_ids = list(set(cid for cids in deals_to_associated_contact_ids.values() for cid in cids))
    contact_details_by_id = get_contacts_by_ids_batch(all_contact_ids)
    deals_with_contacts = {
        deal_id: [{"firstname": contact_details_by_id[cid]["firstname"], "lastname": contact_details_by_id[cid]["lastname"]}
                  for cid in deals_to_associated_contact_ids.get(deal_id, []) if cid in contact_details_by_id]
        for deal_id in deal_ids
    }
    print("done contact details")
    deals_to_associated_msa_ids = get_associated_msas_for_deals(deal_ids, deals_to_associated_company_ids)
    all_msa_ids = list(set(msa_id for msa_ids in deals_to_associated_msa_ids.values() for msa_id in msa_ids))
    msa_details_by_id = get_msas_by_ids_batch(all_msa_ids)
    deals_with_msa_names = {
        deal_id: [
            msa_details_by_id[msa_id]["msa_name"]
            for msa_id in deals_to_associated_msa_ids.get(deal_id, [])
            if msa_id in msa_details_by_id and msa_details_by_id[msa_id]["msa_name"]
        ]
        for deal_id in deal_ids
    }
    deals_with_msa_details = {
        deal_id: build_msa_details_json(deals_to_associated_msa_ids.get(deal_id, []), msa_details_by_id)
        for deal_id in deal_ids
    }
    print("done msa details")
    pipeline_stages = get_all_stages()
    print("done pipeline stages")
    # owner_details = get_all_owners()
    owner_details = get_owners_by_ids_users_search(owner_ids=list_of_owner_ids)
    print("done owner details")
    sales_deck_file_ids = get_list_of_sales_deck_file_ids(updated_deals_since)
    sales_deck_file_names_by_id = get_files_by_ids_search(sales_deck_file_ids)
    print("done sales decks presentations file details")
    deals_to_associated_line_item_ids = get_associated_line_items_of_deals(deal_ids)
    line_item_id_to_deal_id_mapping = {}
    for deal_id, line_item_ids in deals_to_associated_line_item_ids.items():
        for line_item_id in line_item_ids:
            line_item_id_to_deal_id_mapping[line_item_id] = deal_id
    line_item_ids = list(set(line_item_id_to_deal_id_mapping.keys()))

    # deals_with_line_items = get_all_line_items()
    line_item_details = get_line_items_by_ids_batch(line_item_ids)
    deals_with_line_items = defaultdict(list)
    for line_item_id, details in line_item_details.items():
        deals_with_line_items[line_item_id_to_deal_id_mapping[line_item_id]].append({**details, 'deal_id': line_item_id_to_deal_id_mapping[line_item_id]})
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
            selected_msa = select_msa_for_deal(
                deals_to_associated_msa_ids.get(deal_id, []),
                msa_details_by_id,
                deal_properties.get('primary_associated_company_id'),
            ) or {}

            curr_time = datetime.now(pytz.timezone('America/New_York'))

            if deal_properties['work_ahead'] in ['No', 'blank']:
                work_ahead = 'No'
            else:
                work_ahead = deal_properties['work_ahead']
            deal_owner_details = owner_details.get(deal_properties['hubspot_owner_id'], {})
            delivery_lead_details = owner_details.get(deal_properties['delivery_lead'], {})
            solution_lead_details = owner_details.get(deal_properties['solution_lead'], {})
            company_details = deals_with_companies.get(deal_id, {})
            deal_contacts = deals_with_contacts.get(deal_id, [])
            deal_msa_names = deals_with_msa_names.get(deal_id, [])
            deal_msa_details = deals_with_msa_details.get(deal_id)
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
                "DEAL_AMOUNT_IN_COMPANY_CURRENCY": deal_properties['amount'],
                "DEAL_TYPE": deal_properties['dealtype'],
                "SPECIAL_FIELDS_UPDATED_ON": datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
                "WORK_AHEAD": work_ahead,
                "LAST_REFRESHED_ON": curr_time,
                "REVENUE_TYPE": deal_properties['revenue_type'],
                "CURRENCY": deal_properties.get('deal_currency_code') or 'USD',
                "BOOK_LEADS_2026": deal_properties.get('n2026_book'),
                "BOOK_2026_EMAIL": get_2026_book_lead_email(deal_properties.get('n2026_book')),
                "OFFERING": deal_properties.get('offering'),
                "DESCRIPTION": deal_properties.get('description'),
                "TECH_INVOLVED": deal_properties.get('tech_involved'),
                "PRIMARY_ENTITY": deal_properties.get('primary_entity'),
                "PROJECT_END_DATE": deal_properties.get('est__project_end_date__cloned_'),
                "SOW_END_DATE": deal_properties.get('sow_end_date'),
                "SALES_DECKS_PRESENTATIONS": get_sales_decks_names(deal_properties.get('sales_decks__presentations'), sales_deck_file_names_by_id),
                "MSA_PAYMENT_TERMS": selected_msa.get('payment_terms'),
                "DEAL_REGION": deal_properties.get('deal_region'),
                "MSA_PIPELINE_STAGE": selected_msa.get('msa_pipeline_stage'),
                "MSA_NAME": "; ".join(deal_msa_names) if deal_msa_names else None,
                "MSA_DETAILS": deal_msa_details,
                "PRIMARY_ASSOCIATED_COMPANY_ID": deal_properties.get('primary_associated_company_id'),
                "EMEA_CONTRACTING_ENTITY": deal_properties.get('emea_contracting_entity'),
                "COMPANY_PASSED_TESTER": deal_properties.get('company_passed_tester'),
                "DEAL_CONTACTS": json.dumps(deal_contacts) if deal_contacts else None
            }

            timestamp_fields = [
                'PROJECT_START_DATE', 'PROJECT_CLOSE_DATE', 'DEAL_CREATED_ON',
                'DEAL_UPDATED_ON', 'SPECIAL_FIELDS_UPDATED_ON', 'LAST_REFRESHED_ON',
                'PROJECT_END_DATE', 'SOW_END_DATE'
            ]
            for field in timestamp_fields:
                if deal_data_raw.get(field) is not None and str(deal_data_raw.get(field)).strip() == '':
                    print(f"Field {field} is missing for Deal: {deal_id}, value is {deal_data_raw.get(field)}")
                    deal_data_raw[field] = None

            # number_fields = ['COMPANY_ID', 'DURATION_IN_MONTHS', 'DEAL_AMOUNT_IN_COMPANY_CURRENCY']
            number_fields = [
                "DURATION_IN_MONTHS",
                "DEAL_AMOUNT_IN_COMPANY_CURRENCY",
                "DEAL_OWNER_ID",
                "COMPANY_ID",
                "PIPELINE_ID",
                "NS_PROJECT_ID",
                "DELIVERY_LEAD_ID",
                "SOLUTION_LEAD_ID"
                # Add more if you know they are numeric
            ]
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
            SOLUTION_LEAD_EMAIL, SOLUTION_LEAD_NAME, REVENUE_TYPE, CURRENCY, BOOK_LEADS_2026, BOOK_2026_EMAIL, OFFERING,
            DESCRIPTION, TECH_INVOLVED, PRIMARY_ENTITY, PROJECT_END_DATE, SOW_END_DATE, SALES_DECKS_PRESENTATIONS, MSA_PAYMENT_TERMS,
            DEAL_REGION, MSA_PIPELINE_STAGE, MSA_NAME, MSA_DETAILS, PRIMARY_ASSOCIATED_COMPANY_ID,
            EMEA_CONTRACTING_ENTITY, COMPANY_PASSED_TESTER, DEAL_CONTACTS)
             VALUES
            (%(DEAL_ID)s, %(DEAL_NAME)s, %(DEAL_OWNER)s, %(DEAL_OWNER_ID)s, %(DEAL_OWNER_EMAIL)s,
            %(DEAL_OWNER_NAME)s, %(DEAL_STAGE_ID)s, %(DEAL_STAGE_NAME)s, %(COMPANY_ID)s, %(COMPANY_NAME)s,
            %(DEAL_TO_COMPANY_ASSOCIATIONS)s, %(PIPELINE_ID)s, %(PROJECT_START_DATE)s, %(PROJECT_CLOSE_DATE)s,
            %(ENGAGEMENT_TYPE)s, %(DURATION_IN_MONTHS)s, %(DEAL_COLLABORATORS)s, %(DEAL_CREATED_ON)s,
            %(DEAL_UPDATED_ON)s, %(IS_ARCHIVED)s, %(COMPANY_DOMAIN)s, %(NS_PROJECT_ID)s,
            %(DEAL_AMOUNT_IN_COMPANY_CURRENCY)s, %(DEAL_TYPE)s, CURRENT_TIMESTAMP(), %(WORK_AHEAD)s,
            CURRENT_TIMESTAMP(), %(DELIVERY_LEAD_ID)s, %(DELIVERY_LEAD_EMAIL)s, %(DELIVERY_LEAD_NAME)s,
            %(SOLUTION_LEAD_ID)s, %(SOLUTION_LEAD_EMAIL)s, %(SOLUTION_LEAD_NAME)s, %(REVENUE_TYPE)s,
            %(CURRENCY)s, %(BOOK_LEADS_2026)s, %(BOOK_2026_EMAIL)s, %(OFFERING)s,
            %(DESCRIPTION)s, %(TECH_INVOLVED)s, %(PRIMARY_ENTITY)s, %(PROJECT_END_DATE)s, %(SOW_END_DATE)s, %(SALES_DECKS_PRESENTATIONS)s,
            %(MSA_PAYMENT_TERMS)s, %(DEAL_REGION)s, %(MSA_PIPELINE_STAGE)s, %(MSA_NAME)s, %(MSA_DETAILS)s,
            %(PRIMARY_ASSOCIATED_COMPANY_ID)s, %(EMEA_CONTRACTING_ENTITY)s, %(COMPANY_PASSED_TESTER)s,
            %(DEAL_CONTACTS)s)""",
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
                target.SOLUTION_LEAD_NAME = source.SOLUTION_LEAD_NAME,
                target.REVENUE_TYPE = source.REVENUE_TYPE,
                target.CURRENCY = source.CURRENCY,
                target.BOOK_LEADS_2026 = source.BOOK_LEADS_2026,
                target.BOOK_2026_EMAIL = source.BOOK_2026_EMAIL,
                target.OFFERING = source.OFFERING,
                target.DESCRIPTION = source.DESCRIPTION,
                target.TECH_INVOLVED = source.TECH_INVOLVED,
                target.PRIMARY_ENTITY = source.PRIMARY_ENTITY,
                target.PROJECT_END_DATE = source.PROJECT_END_DATE,
                target.SOW_END_DATE = source.SOW_END_DATE,
                target.SALES_DECKS_PRESENTATIONS = source.SALES_DECKS_PRESENTATIONS,
                target.MSA_PAYMENT_TERMS = source.MSA_PAYMENT_TERMS,
                target.DEAL_REGION = source.DEAL_REGION,
                target.MSA_PIPELINE_STAGE = source.MSA_PIPELINE_STAGE,
                target.MSA_NAME = source.MSA_NAME,
                target.MSA_DETAILS = source.MSA_DETAILS,
                target.PRIMARY_ASSOCIATED_COMPANY_ID = source.PRIMARY_ASSOCIATED_COMPANY_ID,
                target.EMEA_CONTRACTING_ENTITY = source.EMEA_CONTRACTING_ENTITY,
                target.COMPANY_PASSED_TESTER = source.COMPANY_PASSED_TESTER,
                target.DEAL_CONTACTS = source.DEAL_CONTACTS
            WHEN NOT MATCHED THEN
                INSERT (DEAL_ID, DEAL_NAME, DEAL_OWNER, DEAL_OWNER_ID, DEAL_OWNER_EMAIL, DEAL_OWNER_NAME,
                DEAL_STAGE_ID, DEAL_STAGE_NAME, COMPANY_ID, COMPANY_NAME, DEAL_TO_COMPANY_ASSOCIATIONS,
                PIPELINE_ID, PROJECT_START_DATE, PROJECT_CLOSE_DATE, ENGAGEMENT_TYPE, DURATION_IN_MONTHS,
                DEAL_COLLABORATORS, DEAL_CREATED_ON, DEAL_UPDATED_ON, IS_ARCHIVED, COMPANY_DOMAIN, NS_PROJECT_ID,
                DEAL_AMOUNT_IN_COMPANY_CURRENCY, DEAL_TYPE, SPECIAL_FIELDS_UPDATED_ON, WORK_AHEAD, LAST_REFRESHED_ON,
                DELIVERY_LEAD_ID, DELIVERY_LEAD_EMAIL, DELIVERY_LEAD_NAME, SOLUTION_LEAD_ID, SOLUTION_LEAD_EMAIL,
                SOLUTION_LEAD_NAME, REVENUE_TYPE, CURRENCY, BOOK_LEADS_2026, BOOK_2026_EMAIL, OFFERING,
                DESCRIPTION, TECH_INVOLVED, PRIMARY_ENTITY, PROJECT_END_DATE, SOW_END_DATE, SALES_DECKS_PRESENTATIONS,
                MSA_PAYMENT_TERMS, DEAL_REGION, MSA_PIPELINE_STAGE, MSA_NAME, MSA_DETAILS,
                PRIMARY_ASSOCIATED_COMPANY_ID, EMEA_CONTRACTING_ENTITY, COMPANY_PASSED_TESTER, DEAL_CONTACTS)
                VALUES (source.DEAL_ID, source.DEAL_NAME, source.DEAL_OWNER, source.DEAL_OWNER_ID,
                source.DEAL_OWNER_EMAIL, source.DEAL_OWNER_NAME, source.DEAL_STAGE_ID, source.DEAL_STAGE_NAME,
                source.COMPANY_ID, source.COMPANY_NAME, source.DEAL_TO_COMPANY_ASSOCIATIONS, source.PIPELINE_ID,
                source.PROJECT_START_DATE, source.PROJECT_CLOSE_DATE, source.ENGAGEMENT_TYPE, source.DURATION_IN_MONTHS,
                source.DEAL_COLLABORATORS, source.DEAL_CREATED_ON, source.DEAL_UPDATED_ON, source.IS_ARCHIVED,
                source.COMPANY_DOMAIN, source.NS_PROJECT_ID, source.DEAL_AMOUNT_IN_COMPANY_CURRENCY, source.DEAL_TYPE,
                source.SPECIAL_FIELDS_UPDATED_ON, source.WORK_AHEAD, source.LAST_REFRESHED_ON, source.DELIVERY_LEAD_ID,
                source.DELIVERY_LEAD_EMAIL, source.DELIVERY_LEAD_NAME, source.SOLUTION_LEAD_ID,
                source.SOLUTION_LEAD_EMAIL, source.SOLUTION_LEAD_NAME, source.REVENUE_TYPE, source.CURRENCY,
                source.BOOK_LEADS_2026, source.BOOK_2026_EMAIL, source.OFFERING,
                source.DESCRIPTION, source.TECH_INVOLVED, source.PRIMARY_ENTITY, source.PROJECT_END_DATE, source.SOW_END_DATE,
                source.SALES_DECKS_PRESENTATIONS, source.MSA_PAYMENT_TERMS, source.DEAL_REGION,
                source.MSA_PIPELINE_STAGE, source.MSA_NAME, source.MSA_DETAILS,
                source.PRIMARY_ASSOCIATED_COMPANY_ID, source.EMEA_CONTRACTING_ENTITY,
                source.COMPANY_PASSED_TESTER, source.DEAL_CONTACTS)
        """
                          )
        print(f"Done - Deals Updated/Created Since: {sync_from}")
        # #####################################################################
        sf_cursor.execute(f"CREATE OR REPLACE TEMPORARY TABLE LINE_ITEMS_TEMP LIKE {SF_LINE_ITEMS_TABLE}")
        sf_cursor.executemany("""INSERT INTO LINE_ITEMS_TEMP (LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID, CURRENCY)
            VALUES (%(id)s, %(name)s, %(price)s, %(quantity)s, %(amount)s, %(created_at)s, %(updated_at)s, %(deal_id)s, %(currency)s)""",
                              line_items)
        # an empty list renders as `IN ()`, which Snowflake rejects as a syntax error
        if line_items_deals:
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
                target.DEAL_ID = source.DEAL_ID,
                target.CURRENCY = source.CURRENCY
            WHEN NOT MATCHED THEN
                INSERT (LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID, CURRENCY)
                VALUES (source.LINE_ITEM_ID, source.NAME, source.PRICE, source.QUANTITY, source.AMOUNT,
                source.CREATED_ON, source.UPDATED_ON, source.DEAL_ID, source.CURRENCY)
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
