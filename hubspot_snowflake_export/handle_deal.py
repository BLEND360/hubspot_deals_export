import json
import traceback
from datetime import datetime, timezone

from .utils.config import SF_COMPANIES_TABLE, SF_DEAL_OWNERS_TABLE, SF_DEAL_COLLABORATORS_TABLE, SF_DEALS_TABLE, \
    SF_LINE_ITEMS_TABLE
from .utils.hubspot_api import get_deal, get_company_details, get_deal_to_company_association, get_owner_details, \
    get_deal_pipeline_stages, get_line_items_by_ids

curr_time = datetime.now(timezone.utc)
formatted_datetime = curr_time.strftime('%Y-%m-%dT%H:%M:%SZ')


def handle_company_details(deal_id, sf_cursor):
    deal_company_assc = get_deal_to_company_association(deal_id)
    if len(deal_company_assc) > 0:
        company_id = deal_company_assc[0]['toObjectId']
        company_details = get_company_details(company_id)
        company_name = company_details['properties']['name'].replace("'", "''")
        company_domain = company_details['properties']['domain']

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


def handle_line_items(deal, sf_cursor):
    try:
        line_item_ids = [item["id"] for item in deal.get("associations", {}).get("line items", {}).get("results", [])]
        if line_item_ids and len(line_item_ids)>0:
            line_items_data = get_line_items_by_ids(line_item_ids)
            values_str = ", ".join([f"('{item['id']}', '{item['properties']['name']}', {item['properties'].get('price', 0)}, {item['properties'].get('quantity', 0)}, {item['properties'].get('amount', 0)}, '{item['createdAt']}', '{item['updatedAt']}', '{deal['id']}')"
                                    for item in line_items_data])
            upsert_query = f"""
            MERGE INTO {SF_LINE_ITEMS_TABLE} AS target
            USING (
                SELECT * FROM VALUES
                {values_str}
            ) AS source(LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID)
            ON target.LINE_ITEM_ID = source.LINE_ITEM_ID and target.DEAL_ID = source.DEAL_ID
            WHEN MATCHED THEN
                UPDATE SET
                    target.NAME = source.NAME,
                    target.PRICE = source.PRICE,
                    target.QUANTITY = source.QUANTITY,
                    target.AMOUNT = source.AMOUNT,
                    target.CREATED_ON = source.CREATED_ON,
                    target.UPDATED_ON = source.UPDATED_ON
            WHEN NOT MATCHED THEN
                INSERT (LINE_ITEM_ID, NAME, PRICE, QUANTITY, AMOUNT, CREATED_ON, UPDATED_ON, DEAL_ID)
                VALUES (source.LINE_ITEM_ID, source.NAME, source.PRICE, source.QUANTITY, source.AMOUNT, source.CREATED_ON, source.UPDATED_ON, source.DEAL_ID);
            """
            sf_cursor.execute(upsert_query)
            print(f"Upserted line items")
    except Exception as ex:
        print(f"Failed to upsert line items for the deal - {deal['id']}")


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


def parse_owner_details(owner_details):
    if owner_details:
        owner_email = str(owner_details['email'])
        owner_name_ = owner_details['firstName'] + owner_details['lastName']
        if owner_name_:
            owner_name = owner_details['firstName'] + ' ' + owner_details['lastName']
        else:
            owner_name = ' '.join(owner_email.split('@')[0].split('.')).upper()
        owner_id = owner_details['id']
        is_archived = owner_details['userId'] is None
        return {"id": owner_id, "name": owner_name, "email": owner_email, "is_archived": is_archived}
    return None


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
    return "NULL" if value is None else f"'{value}'"


def upsert_deal(sf_cursor, deal_id, deals_request, deal_properties, owner_details, company_details, stage_details,
                special_fields_updated_on):
    company_name = None if not company_details else company_details['name'] if company_details['name'] else " ".join(
        company_details['domain'].split(".")[:-1]).title() if company_details['domain'] else None
    stage_name = next((stage['label'] for stage in stage_details if stage['id'] == deal_properties['dealstage']), None)

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
        "ENGAGEMENT_TYPE": deal_properties['engagement_type'],
        "DURATION_IN_MONTHS": deal_properties['expected_project_duration_in_months'],
        "DEAL_COLLABORATORS": deals_request['collaborators_details_json'],
        "DEAL_CREATED_ON": deal_properties['createdate'],
        "DEAL_UPDATED_ON": deal_properties['updatedAt'],
        "IS_ARCHIVED": False,
        "COMPANY_DOMAIN": company_details['domain'] if company_details is not None else None,
        "NS_PROJECT_ID": deal_properties['ns_project_id__finance_only_'],
        "DEAL_AMOUNT_IN_COMPANY_CURRENCY": deal_properties['amount_in_home_currency'],
        "DEAL_TYPE": deal_properties['dealtype'],
        "SPECIAL_FILEDS_UPDATED_ON": special_fields_updated_on
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
                        {deal_data['SPECIAL_FILEDS_UPDATED_ON']} AS SPECIAL_FILEDS_UPDATED_ON
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
                    target.SPECIAL_FILEDS_UPDATED_ON = source.SPECIAL_FILEDS_UPDATED_ON
            WHEN NOT MATCHED THEN
                INSERT (DEAL_ID, DEAL_NAME, DEAL_OWNER, DEAL_OWNER_ID, DEAL_OWNER_EMAIL, DEAL_OWNER_NAME, DEAL_STAGE_ID, DEAL_STAGE_NAME, COMPANY_ID, COMPANY_NAME, DEAL_TO_COMPANY_ASSOCIATIONS, PIPELINE_ID, PROJECT_START_DATE, PROJECT_CLOSE_DATE, ENGAGEMENT_TYPE, DURATION_IN_MONTHS, DEAL_COLLABORATORS, DEAL_CREATED_ON, DEAL_UPDATED_ON, IS_ARCHIVED, COMPANY_DOMAIN, NS_PROJECT_ID, DEAL_AMOUNT_IN_COMPANY_CURRENCY, DEAL_TYPE, SPECIAL_FILEDS_UPDATED_ON)
                VALUES (source.DEAL_ID, source.DEAL_NAME, source.DEAL_OWNER, source.DEAL_OWNER_ID, source.DEAL_OWNER_EMAIL, source.DEAL_OWNER_NAME, source.DEAL_STAGE_ID, source.DEAL_STAGE_NAME, source.COMPANY_ID, source.COMPANY_NAME, source.DEAL_TO_COMPANY_ASSOCIATIONS, source.PIPELINE_ID, source.PROJECT_START_DATE, source.PROJECT_CLOSE_DATE, source.ENGAGEMENT_TYPE, source.DURATION_IN_MONTHS, source.DEAL_COLLABORATORS, source.DEAL_CREATED_ON, source.DEAL_UPDATED_ON, source.IS_ARCHIVED, source.COMPANY_DOMAIN, source.NS_PROJECT_ID, source.DEAL_AMOUNT_IN_COMPANY_CURRENCY, source.DEAL_TYPE, source.SPECIAL_FILEDS_UPDATED_ON);
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


def handle_special_fields(deal_id, updated_deal_properties, sf_cursor):
    sql_query = f"""
    SELECT
        DEAL_ID,
        DEAL_STAGE_ID,
        DEAL_AMOUNT_IN_COMPANY_CURRENCY,
        ENGAGEMENT_TYPE,
        PROJECT_START_DATE,
        DURATION_IN_MONTHS,
        SPECIAL_FILEDS_UPDATED_ON
    FROM
        {SF_DEALS_TABLE}
    WHERE
        DEAL_ID = '{deal_id}';
    """
    sf_cursor.execute(sql_query)
    results = sf_cursor.fetchall()
    if len(results) < 1:
        return formatted_datetime
    columns = [col[0] for col in sf_cursor.description]
    result_dicts = []
    for row in results:
        row_dict = dict(zip(columns, row))
        result_dicts.append(row_dict)

    updated_deal_fields = {
        'DEAL_ID': deal_id,
        'DEAL_STAGE_ID': updated_deal_properties['dealstage'],
        'DEAL_AMOUNT_IN_COMPANY_CURRENCY': updated_deal_properties['amount_in_home_currency'],
        'ENGAGEMENT_TYPE': updated_deal_properties['engagement_type'],
        'PROJECT_START_DATE': updated_deal_properties['expected_project_start_date'],
        'DURATION_IN_MONTHS': updated_deal_properties['expected_project_duration_in_months']
    }
    fields_to_compare = ['DEAL_STAGE_ID', 'DEAL_AMOUNT_IN_COMPANY_CURRENCY', 'ENGAGEMENT_TYPE', 'PROJECT_START_DATE',
                         'DURATION_IN_MONTHS']
    if compare_dicts(result_dicts[0], updated_deal_fields, fields_to_compare):
        return result_dicts[0]['SPECIAL_FILEDS_UPDATED_ON']
    return formatted_datetime


def handle_deal(deal, sf_cursor):
    try:
        deal_properties = deal['properties']
        deal_id = deal['id']

        deal_owner = deal_properties['hubspot_owner_id']
        deal_collaborators = deal_properties['hs_all_collaborator_owner_ids']
        pipeline_id = deal_properties['pipeline']
        deal['properties']['updatedAt'] = deal['updatedAt']

        print(f"Upserting Deal {deal_id} - {deal_properties['dealname']}")

        handle_line_items(deal, sf_cursor)
        company_associations = handle_company_details(deal_id, sf_cursor)
        owner_details = handle_deal_owner_details(deal_owner, sf_cursor)
        collaborators_details = handle_deal_collaborators(deal_collaborators)
        upsert_deal_collaborators(deal_id, collaborators_details, sf_cursor)

        stage_details = get_deal_pipeline_stages(pipeline_id)
        deals_request = create_deal_update_request(owner_details, collaborators_details,
                                                   company_associations.get('associations', None))

        special_fields_updated_on = handle_special_fields(deal_id, deal_properties, sf_cursor)
        upsert_deal(sf_cursor, deal_id, deals_request, deal_properties, owner_details,
                    company_associations.get('company_details', None),
                    stage_details, special_fields_updated_on)
    except Exception as ex:
        traceback.print_exc()
        print(f"Failed to updated deal - {deal['id']}, Exception: {ex}")


def handle_deal_upsert(deal, sf_cursor):
    deal_id = deal['id']

    complete_deal_details = get_deal(deal_id)
    handle_deal(complete_deal_details, sf_cursor)
