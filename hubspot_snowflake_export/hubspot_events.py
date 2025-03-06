def handle_webhook_from_hubspot(event):
    print("======== Start: Received Webhook from HubSpot ========")
    print(event)
    print("==>BODY<===")
    print(event.get('body', None))
    print("======== End: Received Webhook from HubSpot ========")