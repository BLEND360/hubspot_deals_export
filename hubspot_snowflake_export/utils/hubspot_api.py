import os

import json
from collections import defaultdict
from datetime import datetime, timedelta

import requests

from hubspot_snowflake_export.utils.config import LOCAL_CACHE

# HubSpot API base URL
BASE_URL = "https://api.hubapi.com"

# HubSpot API Key
API_KEY = os.getenv("HUBSPOT_API_KEY")

auth_headers = {
    'authorization': f'Bearer {API_KEY}',
}

deal_properties = [
    # "billing_hours",
    # "deal_source",
    # "dp_rate",
    # "estimated_salary",
    "expected_project_duration_in_months",
    "expected_project_start_date",
    # "fulfillment_type",
    # "hourly_pay_rate",
    # "hs_acv",
    "hs_all_collaborator_owner_ids",
    # "hs_all_deal_split_owner_ids",
    # "hs_analytics_latest_source_data_2_company",
    # "hs_analytics_latest_source_timestamp",
    # "hs_analytics_latest_source_timestamp_contact",
    # "hs_analytics_source",
    # "hs_analytics_source_data_1",
    # "hs_analytics_source_data_2",
    # "hs_campaign",
    # "hs_closed_amount",
    # "hs_closed_amount_in_home_currency",
    # "hs_closed_won_count",
    # "hs_closed_won_date",
    # "hs_created_by_user_id",
    # "hs_deal_stage_probability",
    # "internal_jd_and_notes",
    # "job_department",
    # "job_function",
    # "leading_team",
    # "lfapp_latest_visit",
    # "lfapp_view_in_leadfeeder",
    # "location",
    # "partner_involved",
    # "placement_type",
    # "portfolio_lead",
    # "quantity___number_of_positions",
    # "team",
    # "work_location_details",
    # "deal_registration_hubspot_shared_selling",
    # "deal_registration_most_likely_hubspot_product_s_",
    "dealname",
    # "original_hs_object_id",
    # "amount",
    "dealstage",
    "pipeline",
    "closedate",
    # "createdate",
    "hs_sales_email_last_replied",
    "hubspot_owner_id",
    "hs_createdate",
    "dealtype",
    # "description",
    # "ta_lead",
    # "smartrecruiter_id",
    # "push_to_smart_recruiters_",
    # "billing_rate",
    # "tcv_and_amount_delta",
    "amount_in_home_currency",
    # "job_title",
    # "original_deal_id",
    # "est__project_end_date__cloned_",
    "ns_project_id__finance_only_",
    # "current_stage",
    # "engagement_type",
    "engagement_type__cloned_",
    "work_ahead"
]

max_all_retry = 3
# function to call api and retry if fail return json response or raise exception
def call_api(method, url, headers=None, payload=None, timeout=120):
    if payload is None:
        payload = {}
    if headers is None:
        headers = auth_headers
    print(url)
    url_name = url.split('/')[-1].split('?')[0]
    max_retry = 1
    global max_all_retry
    first_attempt = True
    while min(max_retry, max_all_retry) > 0 or first_attempt:
        first_attempt = False
        response = requests.request(method, url, headers=headers, data=payload, timeout=timeout)
        if response.status_code == 200:
            return response.json()
        else:
            max_all_retry -= 1
            max_retry -= 1
            print(f"Error fetching data({url_name}): {response.status_code} - {response.text}")
            if max_retry < 0 or max_all_retry < 0:
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")
            print(f"Retrying... {min(max_all_retry, max_retry)} attempts left")



def fetch_updated_or_created_deals(start_date_time, sync_older=False, created_after="2024-01-01T00:00:00Z", use_backup=False):
    if use_backup:
        with open("deals.json", "r") as f:
            return json.load(f)
    url = f"{BASE_URL}/crm/v3/objects/deals/search"

    deals = []
    has_more = True
    after = "0"
    filters = [
        {
            "propertyName": "hs_lastmodifieddate",
            "operator": "GT",
            "value": start_date_time
        },
        {
            "propertyName": "pipeline",
            "operator": "IN",
            "values": ["74948272", "35923868", "663516528"]
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
    while has_more:
        payload = json.dumps({
            "after": after,
            "limit": 200,
            "properties": deal_properties,
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
        # response = requests.request("POST", url, headers=headers, data=payload, timeout=120)
        # if response.status_code == 200:
        #     data = response.json()
        data = call_api("POST", url, headers=headers, payload=payload)
        deals.extend(data['results'])
        has_more = 'paging' in data and 'next' in data['paging']
        after = data['paging']['next']['after'] if has_more else None
    if LOCAL_CACHE == "True":
        with open("deals.json", "w") as f:
            f.write(json.dumps(deals, indent=4))
    return deals


def get_all_companies(use_backup=False):
    if use_backup:
        with open("companies.json", "r") as f:
            return json.load(f)

    url = f"{BASE_URL}/crm/v3/objects/companies?limit=100&associations=deal"

    # payload = {}
    # headers = {
    #     'Authorization': f'Bearer {API_KEY}',
    # }
    deals_with_companies = {}
    while url:
        # print(url)
        # response = requests.request("GET", url, headers=headers, data=payload, timeout=120)
        # # print(response.status_code)
        # # print(response.text)
        # if response.status_code != 200:
        #     raise Exception(f"Error fetching companies: {response.status_code} - {response.text}")
        data = call_api("GET", url)
        companies = data["results"]
        url = data.get("paging", {}).get("next", {}).get("link")
        for company in companies:
            deals = company.get("associations", {}).get("deals", {}).get("results", [])
            additional_associated_deals_link = company.get("associations", {}).get("deals", {}).get("paging", {}).get("next", {}).get("link")
            if additional_associated_deals_link:
                additional_deals = get_additional_association_deals_of_company(additional_associated_deals_link)
                deals.extend(additional_deals)
            name = company["properties"]["name"]
            if not name:
                name = ' '.join(company["properties"]["domain"].split('.')[:-1]).title() if company["properties"]["domain"] else None
            for deal in deals:
                deal_id = deal["id"]
                deals_with_companies[deal_id] = {"id": company["id"],
                                                 "name": name,
                                                 "domain": company["properties"]["domain"]}
    if LOCAL_CACHE == "True":
        with open("companies.json", "w") as f:
            f.write(json.dumps(deals_with_companies, indent=4))
    return deals_with_companies


# print(get_all_companies())

def get_all_line_items(use_backup=False):
    if use_backup:
        with open("line_items.json", "r") as f:
            return json.load(f)
    url = f"{BASE_URL}/crm/v3/objects/line_items?properties=name,amount,quantity,price&limit=100&associations=deals"

    # payload = {}
    # headers = {
    #     'Authorization': f'Bearer {API_KEY}',
    # }
    deals_with_line_items = defaultdict(list)
    while url:
        # print(url)
        # response = requests.request("GET", url, headers=headers, data=payload, timeout=120)
        # if response.status_code != 200:
        #     raise Exception(f"Error fetching line items: {response.status_code} - {response.text}")
        data = call_api("GET", url)
        items = data["results"]
        url = data.get("paging", {}).get("next", {}).get("link")
        for item in items:
            deals = item.get("associations", {}).get("deals", {}).get("results", [])
            for deal in deals:
                deal_id = deal["id"]
                deals_with_line_items[deal_id].append({"id": item["id"],
                                                       "name": item["properties"]["name"],
                                                       "amount": item["properties"]["amount"],
                                                       "quantity": item["properties"]["quantity"],
                                                       "price": item["properties"]["price"],
                                                       "updated_at": item["updatedAt"],
                                                       "created_at": item["createdAt"],
                                                       "deal_id": deal_id})

    if LOCAL_CACHE == "True":
        with open("line_items.json", "w") as f:
            f.write(json.dumps(deals_with_line_items, indent=4))
    return deals_with_line_items


# print(get_all_line_items())

def get_all_owners(use_backup=False):
    if use_backup:
        with open("owners.json", "r") as f:
            return json.load(f)

    urls = [f"{BASE_URL}/crm/v3/owners?limit=100&archived=false", f"{BASE_URL}/crm/v3/owners?limit=100&archived=true"]
    # payload = {}
    # headers = {
    #     'Authorization': f'Bearer {API_KEY}',
    # }
    owner_details = {}
    for url in urls:
        while url:
            # print(url)
            # response = requests.request("GET", url, headers=headers, data=payload)
            # if response.status_code != 200:
            #     raise Exception(f"Error fetching owners: {response.status_code} - {response.text}")
            data = call_api("GET", url)
            owners = data["results"]
            url = data.get("paging", {}).get("next", {}).get("link")
            for owner in owners:
                owner_id = owner["id"]
                name = owner["firstName"] + " " + owner["lastName"]
                if name.strip() == "":
                    name = ' '.join(owner["email"].split('@')[0].split('.')).title() if owner["email"] else ""
                owner_details[owner_id] = {"id": owner["id"],
                                           "email": owner["email"],
                                           "name": name,
                                           "archived": owner["archived"]}
    if LOCAL_CACHE == "True":
        with open("owners.json", "w") as f:
            f.write(json.dumps(owner_details, indent=4))
    return owner_details

# print(get_all_owners())


def get_all_stages():
    url = f"{BASE_URL}/crm/v3/pipelines/deals"

    # payload = {}
    # headers = {
    #     'Authorization': f'Bearer {API_KEY}',
    # }
    pipeline_stages = defaultdict(dict)
    # response = requests.request("GET", url, headers=headers, data=payload, timeout=120)
    # if response.status_code != 200:
    #     raise Exception(f"Error fetching pipeline stages: {response.status_code} - {response.text}")
    data = call_api("GET", url)
    pipelines = data["results"]
    for pipeline in pipelines:
        pipeline_stage_dict = {stage["id"]: stage["label"] for stage in pipeline["stages"]}
        pipeline_stages[pipeline["id"]] = pipeline_stage_dict

    return pipeline_stages

# print(json.dumps(get_all_stages()))

def get_additional_association_deals_of_company(url):
    # payload = {}
    # headers = {
    #     'Authorization': f'Bearer {API_KEY}',
    # }
    associated_deals = []
    while url:
        # response = requests.request("GET", url, headers=headers, data=payload, timeout=120)
        # if response.status_code != 200:
        #     raise Exception(f"Error fetching additional association deals: {response.status_code} - {response.text}")
        data = call_api("GET", url)
        associated_deals.extend(data["results"])
        url = data.get("paging", {}).get("next", {}).get("link")
    return associated_deals

