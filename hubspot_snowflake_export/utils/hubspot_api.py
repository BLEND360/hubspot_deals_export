import os

import json
from collections import defaultdict
from datetime import datetime, timedelta

import requests

from hubspot_snowflake_export.utils.config import SYNC_ALERT_TO_EMAILS, SYNC_ALERT_CC_EMAILS, ENV_, LOCAL_CACHE, \
    MSA_OBJECT_TYPE_ID
from hubspot_snowflake_export.utils.send_mail import send_email

# HubSpot API base URL
BASE_URL = "https://api.hubapi.com"

# HubSpot API Key
API_KEY = os.getenv("HUBSPOT_API_KEY")

auth_headers = {
    'authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

deal_properties = [
    "expected_project_duration_in_months",
    "expected_project_start_date",
    "hs_all_collaborator_owner_ids",
    "dealname",
    "dealstage",
    "pipeline",
    "closedate",
    "hs_sales_email_last_replied",
    "hubspot_owner_id",
    "hs_createdate",
    "dealtype",
    "amount_in_home_currency",
    "ns_project_id__finance_only_",
    "engagement_type__cloned_",
    "work_ahead",
    "delivery_lead",
    "solution_lead",
    "revenue_type",
    "deal_currency_code",
    "amount",
    "n2026_book",
    "offering",
    "description",
    "tech_involved",
    "primary_entity",
    "est__project_end_date__cloned_",
    "sales_decks__presentations",
    "deal_region",
    "primary_associated_company_id",
    "emea_contracting_entity",
    "company_passed_tester",
    "sow_end_date"
]

# Properties fetched from the MSA custom object (MSA_OBJECT_TYPE_ID).
# msa_name feeds HUBSPOT_DEALS.MSA_NAME; the whole set is stored as JSON in HUBSPOT_DEALS.MSA_DETAILS
msa_properties = [
    "msa_name",
    "payment_terms",
    "hs_pipeline_stage",
    "liability_cap",
    "non_compete_details",
    "non_solicitation_details",
    "msa_notes",
    "price_increase_restriction_details",
    "rebate_or_volume_discount_details",
    "subcontracting_details",
    "msa_term",
    "resource_location_restriction_details",
    "primary_associated_company",
    "msa_effective_date",
    "termination_terms_summary",
    "convenience_notice_period_days",
    "createdate",
    "hs_lastmodifieddate",
]

def fetch_updated_or_created_deals(start_date_time, sync_older=False, created_after="2024-01-01T00:00:00Z", use_backup=False,
                                   deal_ids=None):
    if deal_ids is None:
        deal_ids = []
    if use_backup:
        with open("deals.json", "r") as f:
            return json.load(f)
    url = f"{BASE_URL}/crm/v3/objects/deals/search"

    deals = []
    has_more = True
    after = "0"
    filters = [
        {
            "propertyName": "pipeline",
            "operator": "IN",
            "values": ["74948272", "35923868", "663516528", "45724251"]
        }
    ]
    if len(deal_ids) > 0:
        filters.append(
            {
                "propertyName": "hs_object_id",
                "operator": "IN",
                "values": deal_ids
            }
        )
    if start_date_time:
        filters.append(
            {
                "propertyName": "hs_lastmodifieddate",
                "operator": "GT",
                "value": start_date_time
            }
        )
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


def get_deal_to_contact_association(deal_id):
    url = f"{BASE_URL}/crm/v4/objects/deals/{deal_id}/associations/contact"
    response = requests.get(url, headers=auth_headers)
    if response.status_code == 200:
        contact_associations = response.json().get('results', [])
        print(f"Found {len(contact_associations)} contacts associated with deal {deal_id}.")
        return contact_associations
    else:
        print(f"Error fetching contact associations for deal {deal_id}: {response.status_code} - {response.text}")
        return []


def get_contact_details(contact_id):
    url = f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}"
    params = {
        'properties': 'firstname,lastname',
    }
    response = requests.get(url, params=params, headers=auth_headers)
    if response.status_code == 200:
        contact_details = response.json()
        return contact_details
    else:
        print(f"Error fetching contact details for contact {contact_id}: {response.status_code} - {response.text}")
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
        "inputs": inputs,
        "limit": 100,
        "properties": [
            "name",
            "quantity",
            "price",
            "amount",
            "hs_line_item_currency_code"
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
        if response.status_code in (207, 200):
            return response.json()
        else:
            max_all_retry -= 1
            max_retry -= 1
            print(f"Error fetching data({url_name}): {response.status_code} - {response.text}")
            if max_retry < 0 or max_all_retry < 0:
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")
            print(f"Retrying... {min(max_all_retry, max_retry)} attempts left")


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
            additional_associated_deals_link = company.get("associations", {}).get("deals", {}).get("paging", {}).get(
                "next", {}).get("link")
            if additional_associated_deals_link:
                additional_deals = get_additional_association_deals_of_company(additional_associated_deals_link)
                deals.extend(additional_deals)
            name = company["properties"]["name"]
            if not name:
                name = ' '.join(company["properties"]["domain"].split('.')[:-1]).title() if company["properties"][
                    "domain"] else None
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

def get_all_owners(use_backup=False, archived_types=["false", "true"]):
    if use_backup:
        with open("owners.json", "r") as f:
            return json.load(f)

    urls = [f"{BASE_URL}/crm/v3/owners?limit=100&archived={archived}" for archived in archived_types]
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
                                           "name": name,
                                           "email": owner["email"],
                                           "archived": owner["archived"]}
    if LOCAL_CACHE == "True":
        with open("owners.json", "w") as f:
            f.write(json.dumps(owner_details, indent=4))
    return owner_details


# print(get_all_owners())


# MSA custom object type id (holds the hs_pipeline_stage stages) — from config/env
_msa_stage_label_cache = None


def get_msa_stage_labels():
    """Return a flat {stageId: label} map for every stage across all pipelines
    of the MSA custom object. Cached for the lifetime of the Lambda container
    since stages rarely change."""
    global _msa_stage_label_cache
    if _msa_stage_label_cache is not None:
        return _msa_stage_label_cache

    url = f"{BASE_URL}/crm/v3/pipelines/{MSA_OBJECT_TYPE_ID}"
    stage_labels = {}
    try:
        data = call_api("GET", url)
        for pipeline in data.get("results", []):
            for stage in pipeline.get("stages", []):
                stage_labels[stage["id"]] = stage["label"]
    except Exception as ex:
        print(f"Error fetching MSA pipeline stages: {ex}")

    _msa_stage_label_cache = stage_labels
    return stage_labels


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


def get_associated_companies_of_deals(deal_ids):
    """
    Get associated companies of deals
    :param deal_ids: The maximum allowed batch size is 1000
    :return: dict of deal_id to company_id
    :sample:
    {
        "status": "COMPLETE",
        "results": [
            {
                "from": {
                    "id": "17982944313"
                },
                "to": [
                    {
                        "id": "17496863841",
                        "type": "deal_to_company"
                    }
                ]
            },
            {
                "from": {
                    "id": "17982944854"
                },
                "to": [
                    {
                        "id": "6680028191",
                        "type": "deal_to_company"
                    }
                ]
            }
        ],
        "numErrors": 1,
        "errors": [
            {
                "status": "error",
                "category": "OBJECT_NOT_FOUND",
                "subCategory": "crm.associations.NO_ASSOCIATIONS_FOUND",
                "message": "No company is associated with deal 34461455164.",
                "context": {
                    "fromObjectId": [
                        "34461455164"
                    ],
                    "fromObjectType": [
                        "deal"
                    ],
                    "toObjectType": [
                        "company"
                    ]
                }
            }
        ],
        "startedAt": "2025-03-25T18:22:11.060Z",
        "completedAt": "2025-03-25T18:22:11.102Z"
    }
    """
    url = f"{BASE_URL}/crm/v3/associations/deal/company/batch/read"
    deals_as_batch_of_1000 = [deal_ids[i:i + 1000] for i in range(0, len(deal_ids), 1000)]
    deals_to_associated_company_ids = {}
    for deal_ids_ in deals_as_batch_of_1000:
        payload = {"inputs": [{"id": deal_id} for deal_id in deal_ids_]}
        data = call_api("POST", url, headers=auth_headers, payload=json.dumps(payload))
        # print("get_associated_companies_of_deals", "data", data)
        deals_to_associated_company_ids.update({association["from"]["id"]: association["to"][0]["id"] if association["to"] else None for association in data["results"]})
    return deals_to_associated_company_ids

def get_associated_contacts_of_deals(deal_ids):
    """
    Get all associated contacts of deals
    :param deal_ids: The maximum allowed batch size is 1000
    :return: dict of deal_id to list of contact_ids
    """
    url = f"{BASE_URL}/crm/v3/associations/deal/contacts/batch/read"
    deals_as_batch_of_1000 = [deal_ids[i:i + 1000] for i in range(0, len(deal_ids), 1000)]
    deals_to_associated_contact_ids = {}
    for deal_ids_ in deals_as_batch_of_1000:
        payload = {"inputs": [{"id": deal_id} for deal_id in deal_ids_]}
        data = call_api("POST", url, headers=auth_headers, payload=json.dumps(payload))
        deals_to_associated_contact_ids.update({association["from"]["id"]: [i["id"] for i in association["to"]] for association in data["results"]})
    return deals_to_associated_contact_ids


def get_associated_msas_of_deals(deal_ids):
    """
    Get all associated MSA custom-object records of deals.
    :param deal_ids: The maximum allowed batch size is 1000
    :return: dict of deal_id to list of msa ids
    """
    url = f"{BASE_URL}/crm/v3/associations/0-3/{MSA_OBJECT_TYPE_ID}/batch/read"
    deals_as_batch_of_1000 = [deal_ids[i:i + 1000] for i in range(0, len(deal_ids), 1000)]
    deals_to_associated_msa_ids = {}
    for deal_ids_ in deals_as_batch_of_1000:
        payload = {"inputs": [{"id": deal_id} for deal_id in deal_ids_]}
        data = call_api("POST", url, headers=auth_headers, payload=json.dumps(payload))
        deals_to_associated_msa_ids.update(
            {association["from"]["id"]: [i["id"] for i in association["to"]] for association in data["results"]}
        )
    return deals_to_associated_msa_ids


def get_associated_msas_of_companies(company_ids):
    """Return MSA associations keyed by company id."""
    url = f"{BASE_URL}/crm/v3/associations/0-2/{MSA_OBJECT_TYPE_ID}/batch/read"
    result = {}
    unique_ids = list(dict.fromkeys(str(value) for value in company_ids if value))
    for offset in range(0, len(unique_ids), 1000):
        payload = {"inputs": [{"id": value} for value in unique_ids[offset:offset + 1000]]}
        data = call_api("POST", url, headers=auth_headers, payload=json.dumps(payload))
        result.update({
            association["from"]["id"]: [item["id"] for item in association["to"]]
            for association in data.get("results", [])
        })
    return result


def get_associated_msas_for_deals(deal_ids, company_ids_by_deal=None):
    """Combine direct Deal→MSA and primary Company→MSA associations."""
    direct = get_associated_msas_of_deals(deal_ids)
    company_ids_by_deal = company_ids_by_deal or {}
    by_company = get_associated_msas_of_companies(company_ids_by_deal.values())
    return {
        str(deal_id): list(dict.fromkeys(
            direct.get(str(deal_id), [])
            + by_company.get(str(company_ids_by_deal.get(str(deal_id)) or ""), [])
        ))
        for deal_id in deal_ids
    }


def get_associated_line_items_of_deals(deal_ids):
    url = f"{BASE_URL}/crm/v3/associations/deal/line_item/batch/read"
    deals_as_batch_of_1000 = [deal_ids[i:i + 1000] for i in range(0, len(deal_ids), 1000)]
    deals_to_associated_line_item_ids = {}
    for deal_ids_ in deals_as_batch_of_1000:
        payload = {"inputs": [{"id": deal_id} for deal_id in deal_ids_]}
        data = call_api("POST", url, headers=auth_headers, payload=json.dumps(payload))
        deals_to_associated_line_item_ids.update({association["from"]["id"]: [i["id"] for i in  association["to"]] for association in data["results"]})
    return deals_to_associated_line_item_ids



def get_companies_by_ids_search(company_ids):
    """
    Get companies by company ids
    :param company_ids: max allowed 100
    :return: list of companies
    :sample:
        {
        "total": 100,
        "results": [
            {
                "id": "4633339402",
                "properties": {
                    "createdate": "2020-10-14T23:19:31.260Z",
                    "domain": "verizon.com",
                    "hs_lastmodifieddate": "2025-03-25T12:29:27.211Z",
                    "hs_object_id": "4633339402",
                    "name": "Verizon"
                },
                "createdAt": "2020-10-14T23:19:31.260Z",
                "updatedAt": "2025-03-25T12:29:27.211Z",
                "archived": false
            },
            {
                "id": "5352671139",
                "properties": {
                    "createdate": "2021-02-10T14:44:46.997Z",
                    "domain": "linkedin.com",
                    "hs_lastmodifieddate": "2025-03-25T12:29:27.017Z",
                    "hs_object_id": "5352671139",
                    "name": "LinkedIn"
                },
                "createdAt": "2021-02-10T14:44:46.997Z",
                "updatedAt": "2025-03-25T12:29:27.017Z",
                "archived": false
            }
        ],
        "paging": {
            "next": {
                "after": "10"
            }
        }
    }

    """
    url = f"{BASE_URL}/crm/v3/objects/company/search"
    company_details = {}
    company_ids_batch_of_100 = [company_ids[i:i + 100] for i in range(0, len(company_ids), 100)]
    for company_ids_ in company_ids_batch_of_100:
        payload = json.dumps({
            "limit": 100,
            # "properties": [ "name", "domain", "hs_object_id", "hs_lastmodifieddate", "createdate"],
            "filterGroups": [
                {
                    "filters": [
            {
                "propertyName": "hs_object_id",
                "operator": "IN",
                "values": company_ids_
            }
        ]
                }
            ]
        })
        data = call_api("POST", url, payload=payload)
        for company in data["results"]:
            name = company["properties"]["name"]
            if not name:
                name = ' '.join(company["properties"]["domain"].split('.')[:-1]).title() if company["properties"][
                    "domain"] else None
            company_details[company["id"]] = {"id": company["id"],
                                             "name": name,
                                             "domain": company["properties"]["domain"]}

    return company_details

def get_line_items_by_ids_search(line_item_ids):
    url = f"{BASE_URL}/crm/v3/objects/line_items/search"
    line_item_details = {}
    line_item_ids_batch_of_100 = [line_item_ids[i:i + 100] for i in range(0, len(line_item_ids), 100)]
    for line_item_ids_ in line_item_ids_batch_of_100:
        payload = json.dumps({
            "limit": 100,
            "properties": [
                "name",
                "amount",
                "quantity",
                "price"
            ],
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_object_id",
                            "operator": "IN",
                            "values": line_item_ids_
                        }
                    ]
                }
            ]
        })
        data = call_api("POST", url, payload=payload)
        for line_item in data["results"]:
            line_item_details[line_item["id"]] = {"id": line_item["id"],
                                                       "name": line_item["properties"]["name"],
                                                       "amount": line_item["properties"]["amount"],
                                                       "quantity": line_item["properties"]["quantity"],
                                                       "price": line_item["properties"]["price"],
                                                       "updated_at": line_item["updatedAt"],
                                                       "created_at": line_item["createdAt"]
                                                  }
    return line_item_details

def get_companies_by_ids_batch(company_ids):
    url = f"{BASE_URL}/crm/v3/objects/company/batch/read"
    company_details = {}
    company_ids_batch_of_100 = [company_ids[i:i + 100] for i in range(0, len(company_ids), 100)]
    for company_ids_ in company_ids_batch_of_100:
        payload = json.dumps({
            "inputs": [{"id": company_id} for company_id in company_ids_],
            "limit": 100,
            "properties": [
                "name",
                "domain"
            ]
        })
        data = call_api("POST", url, payload=payload)
        for company in data["results"]:
            name = company["properties"]["name"]
            if not name:
                name = ' '.join(company["properties"]["domain"].split('.')[:-1]).title() if company["properties"][
                    "domain"] else None
            company_details[company["id"]] = {"id": company["id"],
                                             "name": name,
                                             "domain": company["properties"]["domain"]}
    return company_details


def get_contacts_by_ids_batch(contact_ids):
    url = f"{BASE_URL}/crm/v3/objects/contacts/batch/read"
    contact_details = {}
    contact_ids_batch_of_100 = [contact_ids[i:i + 100] for i in range(0, len(contact_ids), 100)]
    for contact_ids_ in contact_ids_batch_of_100:
        payload = json.dumps({
            "inputs": [{"id": contact_id} for contact_id in contact_ids_],
            "limit": 100,
            "properties": [
                "firstname",
                "lastname"
            ]
        })
        data = call_api("POST", url, payload=payload)
        for contact in data["results"]:
            contact_details[contact["id"]] = {"id": contact["id"],
                                              "firstname": contact["properties"].get("firstname"),
                                              "lastname": contact["properties"].get("lastname")}
    return contact_details


def _normalize_msa_record(msa, stage_labels):
    props = msa.get("properties") or {}
    stage = props.get("hs_pipeline_stage")
    return {
        "id": msa["id"],
        **{prop: props.get(prop) for prop in msa_properties},
        "msa_pipeline_stage_id": stage,
        "msa_pipeline_stage": stage_labels.get(stage, stage),
    }


def get_msas_by_ids_batch(msa_ids):
    url = f"{BASE_URL}/crm/v3/objects/{MSA_OBJECT_TYPE_ID}/batch/read"
    msa_details = {}
    stage_labels = get_msa_stage_labels()
    msa_ids_batch_of_100 = [msa_ids[i:i + 100] for i in range(0, len(msa_ids), 100)]
    for msa_ids_ in msa_ids_batch_of_100:
        payload = json.dumps({
            "inputs": [{"id": msa_id} for msa_id in msa_ids_],
            "limit": 100,
            "properties": msa_properties
        })
        data = call_api("POST", url, payload=payload)
        for msa in data["results"]:
            msa_details[msa["id"]] = _normalize_msa_record(msa, stage_labels)
    return msa_details


def build_msa_details_json(msa_ids, msa_details_by_id):
    """
    Build the HUBSPOT_DEALS.MSA_DETAILS value: a JSON array of the full MSA records
    associated to a deal, or None when the deal has no associated MSA.
    """
    records = [msa_details_by_id[msa_id] for msa_id in msa_ids if msa_id in msa_details_by_id]
    return json.dumps(records) if records else None


def select_msa_for_deal(msa_ids, msa_details_by_id, company_id=None):
    records = [msa_details_by_id[msa_id] for msa_id in msa_ids if msa_id in msa_details_by_id]
    if company_id and any(record.get("primary_associated_company") for record in records):
        records = [
            record for record in records
            if str(record.get("primary_associated_company")) == str(company_id)
        ]
    if not records:
        return None

    active = [
        record for record in records
        if str(record.get("msa_pipeline_stage") or "").strip().lower() == "active"
    ]
    candidates = active or records
    return max(candidates, key=lambda record: (
        record.get("msa_effective_date") or "" if active else record.get("hs_lastmodifieddate") or "",
        record.get("hs_lastmodifieddate") or "",
        record.get("createdate") or "",
        str(record.get("id") or ""),
    ))


def get_line_items_by_ids_batch(line_item_ids):
    url = f"{BASE_URL}/crm/v3/objects/line_items/batch/read"
    line_item_details = {}
    line_item_ids_batch_of_100 = [line_item_ids[i:i + 100] for i in range(0, len(line_item_ids), 100)]
    for line_item_ids_ in line_item_ids_batch_of_100:
        payload = json.dumps({
            "inputs": [{"id": line_item_id} for line_item_id in line_item_ids_],
            "limit": 100,
            "properties": [
                "name",
                "amount",
                "quantity",
                "price",
                "hs_line_item_currency_code"
            ]
        })
        data = call_api("POST", url, payload=payload)
        for line_item in data["results"]:
            line_item_details[line_item["id"]] = {"id": line_item["id"],
                                                       "name": line_item["properties"]["name"],
                                                       "amount": line_item["properties"]["amount"],
                                                       "quantity": line_item["properties"]["quantity"],
                                                       "price": line_item["properties"]["price"],
                                                       "updated_at": line_item["updatedAt"],
                                                       "created_at": line_item["createdAt"],
                                                       "currency": line_item["properties"].get("hs_line_item_currency_code", "USD")
                                                  }
    return line_item_details


def build_file_name(file_obj):
    """Build 'name.extension' from a HubSpot file object. Falls back to name if extension missing."""
    if not file_obj:
        return None
    name = file_obj.get('name')
    extension = file_obj.get('extension')
    if name and extension:
        return f"{name}.{extension}"
    return name or None


def get_file_name_by_id(file_id):
    """Single file lookup (single-deal style). Returns 'name.extension' or None."""
    if not file_id:
        return None
    url = f"{BASE_URL}/files/v3/files/{file_id}"
    response = requests.get(url, headers=auth_headers)
    if response.status_code == 200:
        return build_file_name(response.json())
    else:
        print(f"Error fetching file details for file {file_id}: {response.status_code} - {response.text}")
        return None


def get_files_by_ids_search(file_ids):
    """
    Batch file lookup (bulk style) using the Files search API.
    Note: the search API does NOT return files with access "HIDDEN_PRIVATE";
    those are resolved with a per-id GET fallback.
    :param file_ids: list of file ids
    :return: dict of file_id -> 'name.extension'
    """
    file_names_by_id = {}
    if not file_ids:
        return file_names_by_id
    url = f"{BASE_URL}/files/v3/files/search"
    file_ids_batch_of_100 = [file_ids[i:i + 100] for i in range(0, len(file_ids), 100)]
    for file_ids_ in file_ids_batch_of_100:
        after = None
        while True:
            params = [('properties', 'name'), ('properties', 'extension'), ('limit', 100)]
            for fid in file_ids_:
                params.append(('ids', fid))
            if after:
                params.append(('after', after))
            response = requests.get(url, params=params, headers=auth_headers)
            if response.status_code != 200:
                print(f"Error searching files: {response.status_code} - {response.text}")
                break
            data = response.json()
            for file_obj in data.get('results', []):
                file_names_by_id[file_obj['id']] = build_file_name(file_obj)
            after = data.get('paging', {}).get('next', {}).get('after')
            if not after:
                break

    # Search omits HIDDEN_PRIVATE files - fall back to a per-id GET for anything missed.
    missed_file_ids = set(file_ids) - set(file_names_by_id.keys())
    for missed_file_id in missed_file_ids:
        file_name = get_file_name_by_id(missed_file_id)
        if file_name:
            file_names_by_id[missed_file_id] = file_name
        else:
            print(f"File not found via search or GET: {missed_file_id}")
    return file_names_by_id


def get_owners_by_ids_users_search(owner_ids):
    print("get_owners_by_ids_users_search", owner_ids)
    url = f"{BASE_URL}/crm/v3/objects/users/search"
    owner_details = {}
    owner_ids_batch_of_100 = [owner_ids[i:i + 100] for i in range(0, len(owner_ids), 100)]
    for owner_ids_ in owner_ids_batch_of_100:
        payload = json.dumps({
            "limit": 100,
            "properties": [
                "hubspot_owner_id",
                # "hs_searchable_calculated_name",
                "hs_email",
                "hs_given_name",
                "hs_family_name"
            ],
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hubspot_owner_id",
                            "operator": "IN",
                            "values": owner_ids_
                        }
                    ]
                }
            ]
        })
        data = call_api("POST", url, payload=payload)
        for owner in data["results"]:
            name = ""
            if owner["properties"]["hs_given_name"] and owner["properties"]["hs_family_name"]:
                name = owner["properties"]["hs_given_name"] + " " + owner["properties"]["hs_family_name"]
            elif owner["properties"]["hs_given_name"]:
                name = owner["properties"]["hs_given_name"]
            elif owner["properties"]["hs_family_name"]:
                name = owner["properties"]["hs_family_name"]
            if not name:
                name = ' '.join(owner["properties"]["hs_email"].split('@')[0].split('.')).title() if owner["properties"][
                    "hs_email"] else None
            owner_details[owner["properties"]["hubspot_owner_id"]] = {"id": owner["properties"]["hubspot_owner_id"],
                                          "name": name,
                                          "email": owner["properties"]["hs_email"],
                                          "archived": False}

    missed_owner_ids = set(owner_ids) - set(owner_details.keys())
    if len(missed_owner_ids) == 1:
        print("missed_owner_ids", missed_owner_ids)
        missed_owner_id = list(missed_owner_ids)[0]
        missed_owner_details = call_owner_api(missed_owner_id, True)
        if missed_owner_details:
            name = ""
            if missed_owner_details["firstName"] and missed_owner_details["lastName"]:
                name = missed_owner_details["firstName"] + " " + missed_owner_details["lastName"]
            elif missed_owner_details["firstName"]:
                name = missed_owner_details["firstName"]
            elif missed_owner_details["lastName"]:
                name = missed_owner_details["lastName"]
            if not name:
                name = ' '.join(missed_owner_details["email"].split('@')[0].split('.')).title() if missed_owner_details[
                    "email"] else None
            owner_details[missed_owner_id] = {"id": missed_owner_id,
                                              "name": name,
                                              "email": missed_owner_details["email"],
                                              "archived": True}
        else:
            print(f"Owner not found in archived owners also: {missed_owner_id}")
    if len(missed_owner_ids) > 1:
        print("missed_owner_ids", missed_owner_ids)
        missed_owners = get_all_owners(use_backup=False, archived_types=["true"])
        for missed_owner_id in missed_owner_ids:
            if missed_owner_id in missed_owners:
                owner_details[missed_owner_id] = {"id": missed_owners[missed_owner_id]["id"],
                                                  "name": missed_owners[missed_owner_id]["name"],
                                                  "email": missed_owners[missed_owner_id]["email"],
                                                  "archived": True}
            else:
                print(f"Owner not found in archived owners also: {missed_owner_id}")
    return owner_details

