import os

import json
from datetime import datetime, timedelta

import requests

from hubspot_snowflake_export.utils.config import SYNC_ALERT_TO_EMAILS, SYNC_ALERT_CC_EMAILS
from hubspot_snowflake_export.utils.send_mail import send_email

# HubSpot API base URL
BASE_URL = "https://api.hubapi.com"

# HubSpot API Key
API_KEY = os.getenv("HUBSPOT_API_KEY")

auth_headers = {
    'authorization': f'Bearer {API_KEY}',
}

deal_properties = [
    "billing_hours",
    "deal_source",
    "dp_rate",
    "estimated_salary",
    "expected_project_duration_in_months",
    "expected_project_start_date",
    "fulfillment_type",
    "hourly_pay_rate",
    "hs_acv",
    "hs_all_collaborator_owner_ids",
    "hs_all_deal_split_owner_ids",
    "hs_analytics_latest_source_data_2_company",
    "hs_analytics_latest_source_timestamp",
    "hs_analytics_latest_source_timestamp_contact",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hs_campaign",
    "hs_closed_amount",
    "hs_closed_amount_in_home_currency",
    "hs_closed_won_count",
    "hs_closed_won_date",
    "hs_created_by_user_id",
    "hs_deal_stage_probability",
    "internal_jd_and_notes",
    "job_department",
    "job_function",
    "leading_team",
    "lfapp_latest_visit",
    "lfapp_view_in_leadfeeder",
    "location",
    "partner_involved",
    "placement_type",
    "portfolio_lead",
    "quantity___number_of_positions",
    "team",
    "work_location_details",
    "deal_registration_hubspot_shared_selling",
    "deal_registration_most_likely_hubspot_product_s_",
    "dealname",
    "original_hs_object_id",
    "amount",
    "dealstage",
    "pipeline",
    "closedate",
    "createdate",
    "hs_sales_email_last_replied",
    "hubspot_owner_id",
    "hs_createdate",
    "dealtype",
    "description",
    "ta_lead",
    "smartrecruiter_id",
    "push_to_smart_recruiters_",
    "billing_rate",
    "tcv_and_amount_delta",
    "amount_in_home_currency",
    "job_title",
    "original_deal_id",
    "est__project_end_date__cloned_",
    "ns_project_id__finance_only_",
    "current_stage",
    "engagement_type",
    "engagement_type__cloned_",
    "work_ahead"
]


def fetch_updated_or_created_deals(start_date_time, sync_older=False, created_after="2024-01-01T00:00:00Z"):
    url = f"{BASE_URL}/crm/v3/objects/deals/search"

    deals = []
    has_more = True
    after = "0"
    filters = [
        {
            "propertyName": "hs_lastmodifieddate",
            "operator": "GT",
            "value": start_date_time
        }
    ]
    if not sync_older:
        filters.append(
            {
                "propertyName": "createdate",
                "operator": "GT",
                "value": created_after
            }
        )
    is_first = True
    while has_more:
        payload = json.dumps({
            "after": after,
            "limit": 100,
            "filterGroups": [
                {
                    "filters": filters
                }
            ]
        })

        headers = {
            'authorization': f'Bearer {API_KEY}',
            'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        if response.status_code == 200:
            data = response.json()
            total_deals = data['total']
            time_gap = datetime.utcnow() - datetime.strptime(start_date_time, "%Y-%m-%dT%H:%M:%SZ")
            if is_first and total_deals > 25 and time_gap<timedelta(hours=8):
                # need to send alert email to one email group

                subject = "HubSpot Deals Sync Alert!"
                content = f'''
                    <html>
                    <body>
                    <p>Dear Team,</p>
                    <p>HubSpot Deals Sync Alert!</p>
                    <p>There are more than 25 deals created/updated since {start_date_time}.</p>
                    <p>Please check the HubSpot Deals Sync.</p>
                    </body>
                    </html>
                '''
                email_to_list = SYNC_ALERT_TO_EMAILS.split(",")
                email_cc_list = SYNC_ALERT_CC_EMAILS.split(",")
                send_email(email_to_list, subject=subject, content=content, content_type="html",
                           email_cc_list=email_cc_list, importance=True)
            if total_deals > 200:
                print('More than 200 Deals updated - Skipping.')
                return []
            deals.extend(data['results'])
            # Check if there is more data to fetch (pagination)
            has_more = 'paging' in data and 'next' in data['paging']
            after = data['paging']['next']['after'] if has_more else None
        else:
            has_more = False
            print(f"Error fetching deals: {response.status_code} - {response.text}")
            break
        is_first = False

    return deals


def get_updated_or_new_deals():
    start_date = datetime.now() - timedelta(minutes=5)
    start_date_str = start_date.isoformat()

    url = f"{BASE_URL}/crm/v3/objects/deals"

    params = {
        'hapikey': API_KEY,
        'limit': 100,
        'properties': 'dealname,amount,dealstage,createdate,hs_lastmodifieddate',
        'createdAt__gte': start_date_str,
        'updatedAt__gte': start_date_str,
    }

    deals = []

    while True:
        response = requests.get(url, params=params)

        if response.status_code == 200:
            data = response.json()
            deals.extend(data.get('results', []))

            if 'paging' in data and 'next' in data['paging']:
                params['after'] = data['paging']['next']['after']
                print(f"Fetched {len(data['results'])} deals. Fetching next page...")
            else:
                print(f"Fetched all deals. Total deals: {len(deals)}")
                break
        else:
            print(f"Error fetching deals: {response.status_code} - {response.text}")
            break

    return deals


def get_deal_to_company_association(deal_id):
    url = f"{BASE_URL}/crm/v4/objects/deals/{deal_id}/associations/company"
    response = requests.get(url, headers=auth_headers)
    if response.status_code == 200:
        company_associations = response.json().get('results', [])
        print(f"Found {len(company_associations)} companies associated with deal {deal_id}.")
        return company_associations
    else:
        print(f"Error fetching company associations for deal {deal_id}: {response.status_code} - {response.text}")
        return []


def get_company_details(company_id):
    url = f"{BASE_URL}/crm/v3/objects/companies/{company_id}"
    params = {
        'properties': 'domain,name',
    }
    response = requests.get(url, params=params, headers=auth_headers)
    if response.status_code == 200:
        company_details = response.json()
        return company_details
    else:
        print(f"Error fetching company details for company {company_id}: {response.status_code} - {response.text}")
        return None


def get_owner_details(owner_id, search_in_archive=True):
    owner_details = call_owner_api(owner_id, False)
    if search_in_archive and not owner_details:
        return call_owner_api(owner_id, True)
    return owner_details


def call_owner_api(owner_id, archive):
    url = f"{BASE_URL}/crm/v3/owners/{owner_id}?archived={archive}".lower()
    response = requests.get(url, headers=auth_headers)

    if response.status_code == 200:
        owner_details = response.json()
        return owner_details
    else:
        print(
            f"Error fetching owner details for {owner_id} with archive: {archive} : {response.status_code} - {response.text}")
        return None


def get_deal_pipeline_stages(pipeline_id):
    if not pipeline_id:
        return None
    url = f"{BASE_URL}/crm/v3/pipelines/deals/{pipeline_id}/stages"
    response = requests.get(url, headers=auth_headers)
    if response.status_code == 200:
        stage_details = response.json()
        return stage_details['results']
    else:
        print(f"Error fetching deal pipeline stages {pipeline_id}: {response.status_code} - {response.text}")
        return None


def get_deal(deal_id):
    if not deal_id:
        return None
    url = f"{BASE_URL}/crm/v3/objects/deals/{deal_id}"

    params = {
        'properties': ','.join(deal_properties),
        'associations': 'company,line_item'
    }
    response = requests.get(url, params=params, headers=auth_headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching deal details for id {deal_id}: {response.status_code} - {response.text}")
        return None

def get_line_items_by_ids(line_item_ids):
    if not line_item_ids or len(line_item_ids) < 1:
        return None
    url = f"{BASE_URL}/crm/v3/objects/line_items/batch/read"

    inputs = [{"id": id} for id in line_item_ids]
    payload = json.dumps({
        "inputs":inputs,
        "limit": 100,
        "properties": [
            "name",
            "quantity",
            "price",
            "amount"
        ]
    })
    headers = {
        'authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code in range(200, 300):
        line_items = response.json()
        return line_items['results']
    else:
        print(f"Error fetching line items: {response.status_code} - {response.text}")
        return None