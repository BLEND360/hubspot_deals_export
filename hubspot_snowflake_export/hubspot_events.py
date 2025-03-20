import json

from hubspot_snowflake_export.utils.config import HUBSPOT_SYNC_QUEUE


def handle_webhook_from_hubspot(event, sqs):
    print("======== Start: Received Webhook from HubSpot ========")
    event_body = event.get('body', None)

    queue_url = HUBSPOT_SYNC_QUEUE

    if not event_body:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Bad Request"})
        }

    try:

        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=event_body
        )

        print("Message Sent! Message ID:", response['MessageId'])

        return {
            'statusCode': 201,
            'body': json.dumps({"message": "Accepted"})
        }

    except Exception as e:
        print(f"Error sending message to SQS: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({"message": f"Internal Error"})
        }
