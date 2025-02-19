from .events import sync_deals

def lambda_handler(event, context):
    sync_deals(event)




