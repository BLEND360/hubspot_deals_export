import json
import traceback
from datetime import datetime, timezone

import pytz

from .bulk_events import get_2026_book_lead_email
from .utils.config import SF_COMPANIES_TABLE, SF_DEAL_OWNERS_TABLE, SF_DEAL_COLLABORATORS_TABLE, SF_DEALS_TABLE, \
    SF_LINE_ITEMS_TABLE
from .utils.hubspot_api import get_deal, get_company_details, get_deal_to_company_association, get_owner_details, \
    get_deal_pipeline_stages, get_line_items_by_ids, get_deal_to_contact_association, get_contact_details, \
    get_file_name_by_id, get_msa_stage_labels, get_associated_msas_of_deals, get_msas_by_ids_batch, \
    build_msa_details_json


def handle_company_details(deal_id, sf_cursor):
    deal_company_assc = get_deal_to_company_association(deal_id)
    if len(deal_company_assc) > 0:
        company_id = deal_company_assc[0]['toObjectId']
        company_details = get_company_details(company_id)
        if not company_details['properties']:
            return {}
        company_name = company_details['properties'].get('name', "")
        company_name = company_name.replace("'", "''") if company_name else ""
        company_domain = company_details['properties'].get('domain', "")

        merge_sql = f"""
               MERGE INTO {SF_COMPANIES_TABLE} AS target
               USING (SELECT '{company_id}' AS COMPANY_ID, '{company_name}' AS NAME, '{company_domain}' AS DOMAIN) AS source
               ON target.company_id = source.company_id
               WHEN MATCHED THEN
                   UPDATE SET target.name = source.name, target.domain = source.domain
               WHEN NOT MATCHED THEN
                   INSERT (company_id, name, domain) 
                   VALUES (source.company_id, source.name, source.domain);
               """

        sf_cursor.execute(merge_sql)
        print(f"Upserted company {company_id} - {company_name}")
        return {"associations": deal_company_assc,
                "company_details": {"id": company_id, "name": company_name, "domain": company_domain}}
    return {}


def handle_contact_details(deal_id):
    deal_contact_assc = get_deal_to_contact_association(deal_id)
    contacts = []
    for assc in deal_contact_assc:
        contact_id = assc['toObjectId']
        contact_details = get_contact_details(contact_id)
        if not contact_details or not contact_details.get('properties'):
            continue
        firstname = contact_details['properties'].get('firstname')
        lastname = contact_details['properties'].get('lastname')
        contacts.append({"firstname": firstname.replace("'", "''") if firstname else firstname,
                         "lastname": lastname.replace("'", "''") if lastname else lastname})
    return contacts


def get_deal_msa_info(deal_id):
    """Return (MSA_NAME, MSA_DETAILS) for a deal from a single MSA batch fetch."""
    deals_to_associated_msa_ids = get_associated_msas_of_deals([deal_id])
    msa_ids = deals_to_associated_msa_ids.get(str(deal_id), [])
    msa_details_by_id = get_msas_by_ids_batch(msa_ids)
    msa_names = [
        msa_details_by_id[msa_id]["msa_name"]
        for msa_id in msa_ids
        if msa_id in msa_details_by_id and msa_details_by_id[msa_id]["msa_name"]
    ]
    return ("; ".join(msa_names) if msa_names else None,
            build_msa_details_json(msa_ids, msa_details_by_id))


def get_deleted_line_item_ids(updated_line_item_ids, existing_line_items):
    existing_line_item_ids = [str(item[0]) for item in existing_line_items]
    return list(set(existing_line_item_ids) - set(updated_line_item_ids))


def to_float(value: str) -> float:
    if not value:
        return float(0)
    try:
        return float(value)
    except Exception:
        return float(0)


def check_line_items_updation(existing_line_items, updated_line_items):
    try:
        for item in updated_line_items:
            exst_item = next((d for d in existing_line_items if str(d[0]) == item['id']), None)
            if exst_item:
                if exst_item[1] != item['properties']['name'] or to_float(str(exst_item[2])) != to_float(
                        str(item['properties'].get('amount', 0))):
                    return True
            else:
                return True
        return False
    except Exception:
        return False


def none_to_null_(value):
    return "NULL" if value is None or value == '' else value


def handle_line_items(deal, sf_cursor):
    try:
        line_item_ids = [item["id"] for item in deal.get("associations", {}).get("line items", {}).get("results", [])]

        sf_cursor.execute(
            f""" SELECT LINE_ITEM_ID, NAME, AMOUNT FROM {SF_LINE_ITEMS_TABLE} WHERE DEAL_ID='{deal['id']}' """)
        existing_line_items = sf_cursor.fetchall()

        if line_item_ids and len(line_item_ids) > 0:
            line_items_data = get_line_items_by_ids(line_item_ids)
            values_str = ", ".join([
                f"('{none_to_null_(item['id'])}', '{none_to_null_(item['properties']['name'])}', {none_to_null_(item['properties'].get('price', 0))}, {none_to_null_(item['properties'].get('quantity', 0))}, {none_to_null_(item['properties'].get('amount', 0))}, '{none_to_null_(item['createdAt'])}', '{none_to_null_(item['updatedAt'])}', '{none_to_null_(deal['id'])}', '{none_to_null_(item['properties'].get('hs_line_item_currency_code', 'USD'))}')"
                for item in line_items_data])
            upsert_query = f"""
            MERGE INTO {SF_LINE_ITEMS_TABLE} AS target
            USING (
                SELECT * FROM VALUES
                {values_str}
            ) AS source(LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID, CURRENCY)
            ON target.LINE_ITEM_ID = source.LINE_ITEM_ID and target.DEAL_ID = source.DEAL_ID
            WHEN MATCHED THEN
                UPDATE SET
                    target.NAME = source.NAME,
                    target.PRICE = source.PRICE,
                    target.QUANTITY = source.QUANTITY,
                    target.AMOUNT = source.AMOUNT,
                    target.CREATED_ON = source.CREATED_ON,
                    target.UPDATED_ON = source.UPDATED_ON,
                    target.CURRENCY = source.CURRENCY
            WHEN NOT MATCHED THEN
                INSERT (LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID, CURRENCY)
                VALUES (source.LINE_ITEM_ID, source.NAME, source.PRICE, source.QUANTITY, source.AMOUNT, source.CREATED_ON, source.UPDATED_ON, source.DEAL_ID, source.CURRENCY);
            """
            sf_cursor.execute(upsert_query)
            print(f"Upserted line items")

            deleted_line_item_ids = get_deleted_line_item_ids(line_item_ids, existing_line_items)

            if deleted_line_item_ids and len(deleted_line_item_ids) > 0:
                id_str = ', '.join(map(str, deleted_line_item_ids))
                print(f"""DELETING Line Items ({id_str})""")
                sf_cursor.execute(f"""DELETE FROM {SF_LINE_ITEMS_TABLE} WHERE LINE_ITEM_ID IN ({id_str})""")
                return True

            return
        else:
            if len(existing_line_items) > 0:
                id_str = ', '.join(map(str, [item[0] for item in existing_line_items]))
                print(f"""DELETING Line Items ({id_str})""")
                sf_cursor.execute(f"""DELETE FROM {SF_LINE_ITEMS_TABLE} WHERE LINE_ITEM_ID IN ({id_str})""")
                return True
            return False
    except Exception as ex:
        traceback.print_exc()
        print(f"Failed to upsert line items for the deal - {deal['id']}")
        return False


def handle_deal_owner_details(deal_owner, sf_cursor):
    if deal_owner:
        owner_details = get_owner_details(deal_owner)
        owner_details = parse_owner_details(owner_details)
        owner_email = owner_details['email']
        owner_name = owner_details['name']
        owner_id = owner_details['id']
        is_archived = owner_details['is_archived']

        merge_sql = f"""
               MERGE INTO {SF_DEAL_OWNERS_TABLE} AS target
               USING (SELECT '{owner_id}' AS OWNER_ID, '{owner_name}' AS NAME, '{owner_email}' AS EMAIL, '{is_archived}' AS IS_ARCHIVED) AS source
               ON target.owner_id = source.owner_id
               WHEN MATCHED THEN
                   UPDATE SET target.name = source.name, target.email = source.email, target.is_archived = source.is_archived
               WHEN NOT MATCHED THEN
                   INSERT (owner_id, name, email, is_archived) 
                   VALUES (source.owner_id, source.name, source.email, source.is_archived);
               """

        sf_cursor.execute(merge_sql)
        print(f"Upserted owner {owner_id} - {owner_name}")
        return owner_details
    return None


def handle_deal_lead_details(deal_owner):
    if deal_owner:
        owner_details = get_owner_details(deal_owner)
        owner_details = parse_owner_details(owner_details)
        return owner_details
    return {}


def parse_owner_details(owner_details):
    if owner_details:
        owner_email = str(owner_details['email'])
        owner_name_ = owner_details['firstName'] + owner_details['lastName']
        if owner_name_:
            owner_name = owner_details['firstName'] + ' ' + owner_details['lastName']
        else:
            owner_name = ' '.join(owner_email.split('@')[0].split('.')).title()
        owner_id = owner_details['id']
        is_archived = owner_details['userId'] is None
        return {"id": owner_id, "name": owner_name.replace("'", "''"), "email": owner_email, "is_archived": is_archived}
    return {}


def handle_deal_collaborators(deal_collaborators_str):
    if not deal_collaborators_str:
        return []
    collaborator_ids = deal_collaborators_str.split(";")
    collaborators = []
    for collaborator_id in collaborator_ids:
        collaborator_details = parse_owner_details(get_owner_details(collaborator_id))
        if collaborator_details:
            collaborators.append(collaborator_details)
    return collaborators


def create_deal_update_request(owner_details, collaborators_details, company_associations):
    owner_json = json.dumps(owner_details)
    collaborators_details_json = json.dumps(collaborators_details) if collaborators_details else ""
    deal_to_company_associations_json = json.dumps(company_associations) if company_associations else ""
    return {"owner_json": owner_json, 'collaborators_details_json': collaborators_details_json,
            'deal_to_company_associations_json': deal_to_company_associations_json}


def upsert_deal_collaborators(deal_id, collaborators_details, sf_cursor):
    curr_time = datetime.now(timezone.utc)
    formatted_datetime = curr_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    if collaborators_details and len(collaborators_details) > 0:
        values = ', '.join(
            f"('{col['email']}', {col['id']}, '{col['name']}', {col['is_archived']})" for col in collaborators_details
        )
        sql = f"""
            MERGE INTO {SF_DEAL_OWNERS_TABLE} AS target
            USING (SELECT column1.email, column1.owner_id, column1.name, column1.is_archived
               FROM VALUES {values} AS column1(email, owner_id, name, is_archived)) AS source
            ON target.owner_id = source.owner_id
            WHEN MATCHED THEN
            UPDATE SET
                target.name = source.name,
                target.email = source.email,
                target.is_archived = source.is_archived
            WHEN NOT MATCHED THEN
                INSERT (email, owner_id, name, is_archived)
                VALUES (source.email, source.owner_id, source.name, source.is_archived);
            """
        sf_cursor.execute(sql)

        values_clause = ', '.join(
            [f"('{deal_id}', '{formatted_datetime}', '{rec['id']}')" for rec in collaborators_details]
        )

        merge_sql = f"""
        MERGE INTO {SF_DEAL_COLLABORATORS_TABLE} AS target
        USING (SELECT * FROM VALUES {values_clause}) AS source (deal_id, last_updated, owner_id)
            ON target.deal_id = source.deal_id AND target.owner_id = source.owner_id
            WHEN MATCHED THEN
                UPDATE SET target.last_updated = source.last_updated
            WHEN NOT MATCHED THEN
                INSERT (deal_id, last_updated, owner_id)
                VALUES (source.deal_id, source.last_updated, source.owner_id);
        """
        sf_cursor.execute(merge_sql)
        print(f"Upserted Deal Collaborators")


def none_to_null(value):
    return "NULL" if value is None or value == '' else f"'{value}'"


def get_sales_decks_presentations(file_ids_str):
    """Resolve semicolon-separated file ids to a JSON list of {id, name} dicts (single-deal style)."""
    if not file_ids_str:
        return None
    result = []
    for file_id in file_ids_str.split(";"):
        file_id = file_id.strip()
        if not file_id:
            continue
        result.append({"id": file_id, "name": get_file_name_by_id(file_id)})
    return json.dumps(result) if result else None


def upsert_deal(sf_cursor, deal_id, deals_request, deal_properties, owner_details, company_details, stage_details,
                delivery_lead_details, solution_lead_details, contact_details=None):
    contact_details = contact_details or []
    company_name = None if not company_details else company_details['name'] if company_details['name'] else " ".join(
        company_details['domain'].split(".")[:-1]).title() if company_details['domain'] else None
    stage_name = next((stage['label'] for stage in stage_details if stage['id'] == deal_properties['dealstage']), None)

    # msa_pipeline_stage holds a stage ID from the MSA custom object; resolve it to its label
    msa_stage_id = deal_properties.get('msa_pipeline_stage')
    msa_stage_name = get_msa_stage_labels().get(msa_stage_id, msa_stage_id) if msa_stage_id else None
    msa_name, msa_details = get_deal_msa_info(deal_id)

    curr_time = datetime.now(pytz.timezone('America/New_York'))

    if deal_properties['work_ahead'] in ['No', 'blank']:
        work_ahead = 'No'
    else:
        work_ahead = deal_properties['work_ahead']

    sales_decks_presentations = get_sales_decks_presentations(deal_properties.get('sales_decks__presentations'))

    deal_data_raw = {
        "DEAL_ID": deal_id,
        "DEAL_NAME": deal_properties['dealname'].replace("'", "''"),
        "DEAL_OWNER": deals_request['owner_json'],
        "DEAL_OWNER_ID": deal_properties['hubspot_owner_id'],
        "DEAL_OWNER_EMAIL": owner_details['email'] if owner_details is not None else None,
        "DEAL_OWNER_NAME": owner_details['name'] if owner_details is not None else None,
        "DEAL_STAGE_ID": deal_properties['dealstage'],
        "DEAL_STAGE_NAME": stage_name,
        "COMPANY_ID": company_details['id'] if company_details is not None else None,
        "COMPANY_NAME": company_name,
        "DEAL_TO_COMPANY_ASSOCIATIONS": deals_request['deal_to_company_associations_json'],
        "PIPELINE_ID": deal_properties['pipeline'],
        "PROJECT_START_DATE": deal_properties['expected_project_start_date'],
        "PROJECT_CLOSE_DATE": deal_properties['closedate'],
        "ENGAGEMENT_TYPE": deal_properties['engagement_type__cloned_'],
        "DURATION_IN_MONTHS": deal_properties['expected_project_duration_in_months'],
        "DEAL_COLLABORATORS": deals_request['collaborators_details_json'],
        "DEAL_CREATED_ON": deal_properties['createdate'],
        "DEAL_UPDATED_ON": deal_properties['updatedAt'],
        "IS_ARCHIVED": False,
        "COMPANY_DOMAIN": company_details['domain'] if company_details is not None else None,
        "NS_PROJECT_ID": deal_properties['ns_project_id__finance_only_'],
        "DEAL_AMOUNT_IN_COMPANY_CURRENCY": deal_properties['amount'],
        "DEAL_TYPE": deal_properties['dealtype'],
        "SPECIAL_FIELDS_UPDATED_ON": curr_time,
        "WORK_AHEAD": work_ahead,
        "LAST_REFRESHED_ON": curr_time,
        "DELIVERY_LEAD_ID": deal_properties['delivery_lead'],
        "DELIVERY_LEAD_EMAIL": delivery_lead_details.get('email'),
        "DELIVERY_LEAD_NAME": delivery_lead_details.get('name'),
        "SOLUTION_LEAD_ID": deal_properties['solution_lead'],
        "SOLUTION_LEAD_EMAIL": solution_lead_details.get('email'),
        "SOLUTION_LEAD_NAME": solution_lead_details.get('name'),
        "REVENUE_TYPE": deal_properties['revenue_type'],
        "CURRENCY": deal_properties.get('deal_currency_code') or 'USD',
        "BOOK_LEADS_2026": deal_properties.get('n2026_book').replace("'", "''") if deal_properties.get('n2026_book') else None,
        "BOOK_2026_EMAIL": get_2026_book_lead_email(deal_properties.get('n2026_book')),
        "OFFERING": deal_properties.get('offering').replace("'", "''") if deal_properties.get('offering') else None,
        "DESCRIPTION": deal_properties.get('description', '').replace("'", "''") if deal_properties.get('description') else None,
        "TECH_INVOLVED": deal_properties.get('tech_involved', '').replace("'", "''") if deal_properties.get('tech_involved') else None,
        "PRIMARY_ENTITY": deal_properties.get('primary_entity', '').replace("'", "''") if deal_properties.get('primary_entity') else None,
        "PROJECT_END_DATE": deal_properties.get('est__project_end_date__cloned_'),
        "SOW_END_DATE": deal_properties.get('sow_end_date'),
        "SALES_DECKS_PRESENTATIONS": sales_decks_presentations.replace("'", "''") if sales_decks_presentations else None,
        "MSA_PAYMENT_TERMS": deal_properties.get('msa_payment_terms', '').replace("'", "''") if deal_properties.get('msa_payment_terms') else None,
        "DEAL_REGION": deal_properties.get('deal_region', '').replace("'", "''") if deal_properties.get('deal_region') else None,
        "MSA_PIPELINE_STAGE": msa_stage_name.replace("'", "''") if msa_stage_name else None,
        "MSA_NAME": msa_name.replace("'", "''") if msa_name else None,
        "MSA_DETAILS": msa_details.replace("'", "''") if msa_details else None,
        "PRIMARY_ASSOCIATED_COMPANY_ID": deal_properties.get('primary_associated_company_id', '').replace("'", "''") if deal_properties.get('primary_associated_company_id') else None,
        "EMEA_CONTRACTING_ENTITY": deal_properties.get('emea_contracting_entity', '').replace("'", "''") if deal_properties.get('emea_contracting_entity') else None,
        "COMPANY_PASSED_TESTER": deal_properties.get('company_passed_tester', '').replace("'", "''") if deal_properties.get('company_passed_tester') else None,
        "DEAL_CONTACTS": json.dumps(contact_details) if contact_details else None
    }
    deal_data = {key: none_to_null(value) for key, value in deal_data_raw.items()}

    merge_sql = f"""
            MERGE INTO {SF_DEALS_TABLE} AS target
            USING (SELECT
                        {deal_data['DEAL_ID']} AS DEAL_ID,
                        {deal_data['DEAL_NAME']} AS DEAL_NAME,
                        {deal_data['DEAL_OWNER']} AS DEAL_OWNER,
                        {deal_data['DEAL_OWNER_ID']} AS DEAL_OWNER_ID,
                        {deal_data['DEAL_OWNER_EMAIL']} AS DEAL_OWNER_EMAIL,
                        {deal_data['DEAL_OWNER_NAME']} AS DEAL_OWNER_NAME,
                        {deal_data['DEAL_STAGE_ID']} AS DEAL_STAGE_ID,
                        {deal_data['DEAL_STAGE_NAME']} AS DEAL_STAGE_NAME,
                        {deal_data['COMPANY_ID']} AS COMPANY_ID,
                        {deal_data['COMPANY_NAME']} AS COMPANY_NAME,
                        {deal_data['DEAL_TO_COMPANY_ASSOCIATIONS']} AS DEAL_TO_COMPANY_ASSOCIATIONS,
                        {deal_data['PIPELINE_ID']} AS PIPELINE_ID,
                        {deal_data['PROJECT_START_DATE']} AS PROJECT_START_DATE,
                        {deal_data['PROJECT_CLOSE_DATE']} AS PROJECT_CLOSE_DATE,
                        {deal_data['ENGAGEMENT_TYPE']} AS ENGAGEMENT_TYPE,
                        {deal_data['DURATION_IN_MONTHS']} AS DURATION_IN_MONTHS,
                        {deal_data['DEAL_COLLABORATORS']} AS DEAL_COLLABORATORS,
                        {deal_data['DEAL_CREATED_ON']} AS DEAL_CREATED_ON,
                        {deal_data['DEAL_UPDATED_ON']} AS DEAL_UPDATED_ON,
                        {deal_data['IS_ARCHIVED']} AS IS_ARCHIVED,
                        {deal_data['COMPANY_DOMAIN']} AS COMPANY_DOMAIN,
                        {deal_data['NS_PROJECT_ID']} AS NS_PROJECT_ID,
                        {deal_data['DEAL_AMOUNT_IN_COMPANY_CURRENCY']} AS DEAL_AMOUNT_IN_COMPANY_CURRENCY,
                        {deal_data['DEAL_TYPE']} AS DEAL_TYPE,
                        {deal_data['SPECIAL_FIELDS_UPDATED_ON']} AS SPECIAL_FIELDS_UPDATED_ON,
                        {deal_data['WORK_AHEAD']} AS WORK_AHEAD,
                        {deal_data['LAST_REFRESHED_ON']} as LAST_REFRESHED_ON,
                        {deal_data['DELIVERY_LEAD_ID']} as DELIVERY_LEAD_ID,
                        {deal_data['DELIVERY_LEAD_EMAIL']} as DELIVERY_LEAD_EMAIL,
                        {deal_data['DELIVERY_LEAD_NAME']} as DELIVERY_LEAD_NAME,
                        {deal_data['SOLUTION_LEAD_ID']} as SOLUTION_LEAD_ID,
                        {deal_data['SOLUTION_LEAD_EMAIL']} as SOLUTION_LEAD_EMAIL,
                        {deal_data['SOLUTION_LEAD_NAME']} as SOLUTION_LEAD_NAME,
                        {deal_data['REVENUE_TYPE']} as REVENUE_TYPE,
                        {deal_data['CURRENCY']} as CURRENCY,
                        {deal_data['BOOK_LEADS_2026']} as BOOK_LEADS_2026,
                        {deal_data['BOOK_2026_EMAIL']} as BOOK_2026_EMAIL,
                        {deal_data['OFFERING']} as OFFERING,
                        {deal_data['DESCRIPTION']} as DESCRIPTION,
                        {deal_data['TECH_INVOLVED']} as TECH_INVOLVED,
                        {deal_data['PRIMARY_ENTITY']} as PRIMARY_ENTITY,
                        {deal_data['PROJECT_END_DATE']} as PROJECT_END_DATE,
                        {deal_data['SOW_END_DATE']} as SOW_END_DATE,
                        {deal_data['SALES_DECKS_PRESENTATIONS']} as SALES_DECKS_PRESENTATIONS,
                        {deal_data['MSA_PAYMENT_TERMS']} as MSA_PAYMENT_TERMS,
                        {deal_data['DEAL_REGION']} as DEAL_REGION,
                        {deal_data['MSA_PIPELINE_STAGE']} as MSA_PIPELINE_STAGE,
                        {deal_data['MSA_NAME']} as MSA_NAME,
                        {deal_data['MSA_DETAILS']} as MSA_DETAILS,
                        {deal_data['PRIMARY_ASSOCIATED_COMPANY_ID']} as PRIMARY_ASSOCIATED_COMPANY_ID,
                        {deal_data['EMEA_CONTRACTING_ENTITY']} as EMEA_CONTRACTING_ENTITY,
                        {deal_data['COMPANY_PASSED_TESTER']} as COMPANY_PASSED_TESTER,
                        {deal_data['DEAL_CONTACTS']} as DEAL_CONTACTS
                    ) AS source
            ON (target.DEAL_ID = source.DEAL_ID)
            WHEN MATCHED THEN
                UPDATE SET
                    target.DEAL_NAME = source.DEAL_NAME,
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
                INSERT (DEAL_ID, DEAL_NAME, DEAL_OWNER, DEAL_OWNER_ID, DEAL_OWNER_EMAIL, DEAL_OWNER_NAME, DEAL_STAGE_ID, DEAL_STAGE_NAME, COMPANY_ID, COMPANY_NAME, DEAL_TO_COMPANY_ASSOCIATIONS, PIPELINE_ID, PROJECT_START_DATE, PROJECT_CLOSE_DATE, ENGAGEMENT_TYPE, DURATION_IN_MONTHS, DEAL_COLLABORATORS, DEAL_CREATED_ON, DEAL_UPDATED_ON, IS_ARCHIVED, COMPANY_DOMAIN, NS_PROJECT_ID, DEAL_AMOUNT_IN_COMPANY_CURRENCY, DEAL_TYPE, SPECIAL_FIELDS_UPDATED_ON, WORK_AHEAD, LAST_REFRESHED_ON, DELIVERY_LEAD_ID, DELIVERY_LEAD_EMAIL, DELIVERY_LEAD_NAME, SOLUTION_LEAD_ID, SOLUTION_LEAD_EMAIL, SOLUTION_LEAD_NAME, REVENUE_TYPE, CURRENCY, BOOK_LEADS_2026, BOOK_2026_EMAIL, OFFERING, DESCRIPTION, TECH_INVOLVED, PRIMARY_ENTITY, PROJECT_END_DATE, SOW_END_DATE, SALES_DECKS_PRESENTATIONS, MSA_PAYMENT_TERMS, DEAL_REGION, MSA_PIPELINE_STAGE, MSA_NAME, MSA_DETAILS, PRIMARY_ASSOCIATED_COMPANY_ID, EMEA_CONTRACTING_ENTITY, COMPANY_PASSED_TESTER, DEAL_CONTACTS)
                VALUES (source.DEAL_ID, source.DEAL_NAME, source.DEAL_OWNER, source.DEAL_OWNER_ID, source.DEAL_OWNER_EMAIL, source.DEAL_OWNER_NAME, source.DEAL_STAGE_ID, source.DEAL_STAGE_NAME, source.COMPANY_ID, source.COMPANY_NAME, source.DEAL_TO_COMPANY_ASSOCIATIONS, source.PIPELINE_ID, source.PROJECT_START_DATE, source.PROJECT_CLOSE_DATE, source.ENGAGEMENT_TYPE, source.DURATION_IN_MONTHS, source.DEAL_COLLABORATORS, source.DEAL_CREATED_ON, source.DEAL_UPDATED_ON, source.IS_ARCHIVED, source.COMPANY_DOMAIN, source.NS_PROJECT_ID, source.DEAL_AMOUNT_IN_COMPANY_CURRENCY, source.DEAL_TYPE, source.SPECIAL_FIELDS_UPDATED_ON, source.WORK_AHEAD, source.LAST_REFRESHED_ON, source.DELIVERY_LEAD_ID, source.DELIVERY_LEAD_EMAIL, source.DELIVERY_LEAD_NAME, source.SOLUTION_LEAD_ID, source.SOLUTION_LEAD_EMAIL, source.SOLUTION_LEAD_NAME, source.REVENUE_TYPE, source.CURRENCY, source.BOOK_LEADS_2026, source.BOOK_2026_EMAIL, source.OFFERING, source.DESCRIPTION, source.TECH_INVOLVED, source.PRIMARY_ENTITY, source.PROJECT_END_DATE, source.SOW_END_DATE, source.SALES_DECKS_PRESENTATIONS, source.MSA_PAYMENT_TERMS, source.DEAL_REGION, source.MSA_PIPELINE_STAGE, source.MSA_NAME, source.MSA_DETAILS, source.PRIMARY_ASSOCIATED_COMPANY_ID, source.EMEA_CONTRACTING_ENTITY, source.COMPANY_PASSED_TESTER, source.DEAL_CONTACTS);
        """

    sf_cursor.execute(merge_sql)
    print(f"Upserted Deal {deal_id} - {deal_data['DEAL_NAME']}")


def to_int(value: str) -> int:
    if not value:
        return 0
    try:
        return int(float(value))
    except Exception:
        return 0


def compare_dicts(dict1, dict2, fields):
    for field in fields:
        value1 = dict1.get(field, '')
        value2 = dict2.get(field, '')
        if field == 'DEAL_AMOUNT_IN_COMPANY_CURRENCY':
            if to_int(value1) != to_int(value2):
                return False
        elif field == 'PROJECT_START_DATE':
            if str(value1).split(' ')[0] != str(value2).split(' ')[0]:
                return False
        elif str(value1) != str(value2):
            return False
    return True


def handle_special_fields(deal_id, updated_deal_properties, does_line_items_updated, sf_cursor):
    if does_line_items_updated:
        return updated_deal_properties['updatedAt']
    sql_query = f"""
    SELECT
        DEAL_ID,
        DEAL_AMOUNT_IN_COMPANY_CURRENCY,
        ENGAGEMENT_TYPE,
        PROJECT_START_DATE,
        DURATION_IN_MONTHS,
        SPECIAL_FIELDS_UPDATED_ON
    FROM
        {SF_DEALS_TABLE}
    WHERE
        DEAL_ID = '{deal_id}';
    """
    sf_cursor.execute(sql_query)
    results = sf_cursor.fetchall()
    if len(results) < 1:
        return updated_deal_properties['updatedAt']
    columns = [col[0] for col in sf_cursor.description]
    result_dicts = []
    for row in results:
        row_dict = dict(zip(columns, row))
        result_dicts.append(row_dict)

    updated_deal_fields = {
        'DEAL_ID': deal_id,
        'DEAL_AMOUNT_IN_COMPANY_CURRENCY': updated_deal_properties['amount'],
        'ENGAGEMENT_TYPE': updated_deal_properties['engagement_type__cloned_'],
        'PROJECT_START_DATE': updated_deal_properties['expected_project_start_date'],
        'DURATION_IN_MONTHS': updated_deal_properties['expected_project_duration_in_months']
    }
    fields_to_compare = ['DEAL_AMOUNT_IN_COMPANY_CURRENCY', 'ENGAGEMENT_TYPE', 'PROJECT_START_DATE',
                         'DURATION_IN_MONTHS']
    if compare_dicts(result_dicts[0], updated_deal_fields, fields_to_compare):
        return result_dicts[0]['SPECIAL_FIELDS_UPDATED_ON']
    return updated_deal_properties['updatedAt']


def handle_deal(deal, sf_cursor):
    try:
        deal_properties = deal['properties']
        deal_id = deal['id']

        deal_owner = deal_properties['hubspot_owner_id']
        deal_collaborators = deal_properties['hs_all_collaborator_owner_ids']
        pipeline_id = deal_properties['pipeline']
        deal['properties']['updatedAt'] = deal['updatedAt']

        print(f"Upserting Deal {deal_id} - {deal_properties['dealname']}")

        company_associations = handle_company_details(deal_id, sf_cursor)
        contact_details = handle_contact_details(deal_id)
        owner_details = handle_deal_owner_details(deal_owner, sf_cursor)
        delivery_lead_details = handle_deal_lead_details(deal_properties['delivery_lead'])
        solution_lead_details = handle_deal_lead_details(deal_properties['solution_lead'])
        collaborators_details = handle_deal_collaborators(deal_collaborators)
        upsert_deal_collaborators(deal_id, collaborators_details, sf_cursor)

        stage_details = get_deal_pipeline_stages(pipeline_id)
        deals_request = create_deal_update_request(owner_details, collaborators_details,
                                                   company_associations.get('associations', None))

        # special_fields_updated_on = handle_special_fields(deal_id, deal_properties, does_line_items_updated, sf_cursor)
        upsert_deal(sf_cursor, deal_id, deals_request, deal_properties, owner_details,
                    company_associations.get('company_details', None),
                    stage_details, delivery_lead_details, solution_lead_details, contact_details)
        handle_line_items(deal, sf_cursor)

    except Exception as ex:
        traceback.print_exc()
        print(f"Failed to updated deal - {deal['id']}, Exception: {ex}")
        raise


def handle_deal_upsert(deal, sf_cursor):
    deal_id = deal['id']

    complete_deal_details = get_deal(deal_id)
    handle_deal(complete_deal_details, sf_cursor)
