import os

SF_DATABASE = os.getenv("SF_DATABASE")
SF_SCHEMA = os.getenv("SF_SCHEMA")
SF_WAREHOUSE = os.getenv("SF_WAREHOUSE")
SF_ROLE = os.getenv("SF_ROLE")

SF_COMPANIES_TABLE = 'HUBSPOT_COMPANIES'
SF_DEAL_OWNERS_TABLE = 'HUBSPOT_DEAL_OWNERS'
SF_DEALS_TABLE = 'HUBSPOT_DEALS'
SF_DEAL_COLLABORATORS_TABLE = "HUBSPOT_DEAL_COLLABORATORS"
SF_LINE_ITEMS_TABLE = "HUBSPOT_DEAL_LINE_ITEMS"
SF_SYNC_INFO_TABLE = "HUBSPOT_ENTITY_SYNC_INFO"