# HubSpot Deals Export to Snowflake

## Project Overview
AWS Lambda serverless application that syncs HubSpot CRM deal data to Snowflake. Handles real-time webhooks and bulk/scheduled syncs for deals, companies, line items, owners, and collaborators.

## Tech Stack
- **Runtime**: Python 3.12 (AWS Lambda)
- **Infrastructure**: AWS SAM (CloudFormation)
- **Key Libraries**: requests, snowflake-connector-python, boto3
- **Cloud Services**: Lambda, API Gateway, SQS, S3, Secrets Manager
- **Email**: Microsoft Graph API (OAuth) for failure notifications

## Project Structure
```
hubspot_deals_export/
├── template.yaml                    # SAM CloudFormation template
├── samconfig.toml                   # SAM deploy config (dev & prod)
├── requirements.txt                 # Python dependencies
├── hubspot_snowflake_export/        # Main Lambda package
│   ├── handler.py                   # Lambda entry point (API + async events)
│   ├── sqs_handler.py               # SQS webhook processor
│   ├── events.py                    # Schedule, bulk, backfill handlers
│   ├── handle_deal.py               # Single deal sync logic
│   ├── hubspot_events.py            # HubSpot webhook receiver
│   ├── bulk_events.py               # Legacy bulk sync
│   ├── bulk_events_new.py           # Current bulk sync (preferred)
│   └── utils/
│       ├── config.py                # Environment variable configuration
│       ├── snowflake_db.py          # Snowflake connection management
│       ├── hubspot_api.py           # HubSpot API client with retry
│       ├── s3.py                    # S3 sync metadata operations
│       └── send_mail.py             # Email notifications (MS Graph)
```

## Lambda Functions
1. **HubspotDealsExportToSnowflake** (`handler.lambda_handler`) - Main handler for API requests and async events
2. **HubspotWebhookSync** (`sqs_handler.lambda_handler`) - Processes HubSpot webhook messages from SQS queue

## API Endpoints
- `POST /sync/deal/{dealId}` - Sync a single deal
- `POST /sync/deals` - Trigger bulk sync (optional `sync_from` param)
- `POST /hubspot/deals/sync` - HubSpot webhook receiver

## Event Types
- `SCHEDULE_FETCH` - Periodic sync of recently changed deals
- `SINGLE_DEAL_UPDATE` - Sync individual deal by ID
- `MANUAL_SYNC` - Full sync from a specified date (uses bulk_events_new.py)
- `MANUAL_SYNC_OLD` - Legacy bulk sync (uses bulk_events.py)
- `BACK_FILL_FETCH` - Historical data backfill from timestamp
- `BULK_DEALS_UPDATE` - Bulk update multiple deals

## Snowflake Tables
- `HUBSPOT_COMPANIES`, `HUBSPOT_DEALS`, `HUBSPOT_DEAL_OWNERS`
- `HUBSPOT_DEAL_COLLABORATORS`, `HUBSPOT_DEAL_LINE_ITEMS`
- `HUBSPOT_ENTITY_SYNC_INFO` (sync status tracking)

## HubSpot Deal Fields Synced
Key fields synced from HubSpot to `HUBSPOT_DEALS`: DEAL_NAME, DEAL_STAGE_NAME, COMPANY_NAME, PROJECT_START_DATE, PROJECT_CLOSE_DATE, ENGAGEMENT_TYPE, DURATION_IN_MONTHS, DEAL_AMOUNT, WORK_AHEAD, OFFERING, DESCRIPTION (deal description), TECH_INVOLVED (tech stack), CURRENCY, PIPELINE_ID, REVENUE_TYPE, NS_PROJECT_ID, DEAL_TYPE, BOOK_LEADS_2026, BOOK_2026_EMAIL, SALES_DECKS_PRESENTATIONS, and more.
- HubSpot API property names: `description` → DESCRIPTION, `tech_involved` → TECH_INVOLVED, `offering` → OFFERING, `sales_decks__presentations` → SALES_DECKS_PRESENTATIONS

### SALES_DECKS_PRESENTATIONS (file resolution)
- The `sales_decks__presentations` deal property holds semicolon-separated HubSpot **file IDs** (e.g. `"217029166877;217029169823"`).
- These IDs are resolved to file names via the HubSpot Files API and stored in `SALES_DECKS_PRESENTATIONS` as a **JSON string** (VARCHAR column, same convention as DEAL_CONTACTS / DEAL_COLLABORATORS): `[{"id": "217029166877", "name": "DeckA.pdf"}, ...]`. The `name` is `"<name>.<extension>"`; it is `null` if the file can't be resolved (the ID is always preserved).
- File-resolution helpers live in `utils/hubspot_api.py`:
  - `get_file_name_by_id(file_id)` — single `GET /files/v3/files/{id}` (used by the single-deal path in `handle_deal.py`).
  - `get_files_by_ids_search(file_ids)` — batched `GET /files/v3/files/search?ids=...&properties=name&properties=extension` (used by the bulk paths in `bulk_events_new.py` / `bulk_events.py`). Batches 100 IDs/request.
- **Caveat:** the Files *search* API does NOT return files with access `HIDDEN_PRIVATE`. `get_files_by_ids_search` falls back to a per-ID `get_file_name_by_id` GET for any ID search omits.
- The Files API is `v3` and current — HubSpot's docs shelve it under "Legacy", but that is only a docs-site label, not a deprecated endpoint (there is no v4).

## Build & Deploy
```bash
# Prerequisites
aws configure sso
pip install -r requirements.txt

# Build
sam build

# Deploy
sam deploy --config-env dev      # Dev (PRICING_TOOL_SANDBOX_HYBRID schema)
sam deploy --config-env prod     # Prod (PRICING_TOOL_PROD schema)

# AWS SSO login
aws sso login --profile dev-admin
aws sso login --profile prod-admin
```

## Environment / Secrets (AWS Secrets Manager)
- `HUBSPOT_TOKEN` - HubSpot API key
- `SF_ACCOUNT`, `SF_USER`, `SF_PASSWORD` - Snowflake credentials
- `HUBSPOT_SYNC_API_KEY` - API authentication for endpoints
- `RRT_TENANT_ID`, `RRT_CLIENT_ID`, `RRT_CLIENT_SECRET` - MS OAuth for emails
- `SYNC_ALERT_TO_EMAILS`, `SYNC_ALERT_CC_EMAILS` - Notification recipients

## Key Patterns
- **Batching**: API calls process up to 1000 items per request
- **S3 sync state**: Tracks last sync time in S3 for idempotency
- **Error notifications**: Failures send HTML-formatted email alerts to team
- **Merge/Upsert**: Snowflake operations use MERGE for idempotent writes
- **Timezone**: Uses UTC and America/New_York for scheduling
