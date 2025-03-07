import json

import requests

from .config import RRT_TENANT_ID, RRT_CLIENT_ID, RRT_CLIENT_SECRET


def send_email(mail_to, subject, content, content_type="Text", email_cc_list=None, importance=None):

    if email_cc_list is None:
        email_cc_list = []
    tenant_id = RRT_TENANT_ID
    client_id = RRT_CLIENT_ID
    client_secret = RRT_CLIENT_SECRET
    token_res = requests.post(f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token",
                              data={
                                  "client_id": client_id,
                                  "client_secret": client_secret,
                                  "scope": "https://graph.microsoft.com/.default",
                                  "grant_type": "client_credentials"
                              }
                              )
    if token_res.status_code >= 300:
        raise Exception(token_res.content)
    mail_token = json.loads(token_res.content)["access_token"]

    # create message
    mail_to_list = []
    for mail in mail_to:
        mail_to_list.append({
            "emailAddress": {
                "address": mail
            }
        })



    cc_to_list = []
    # add in additional cc emails
    for mail in email_cc_list:
        cc_to_list.append({
            "emailAddress": {
                "address": mail
            }
        })

    message = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": content_type,
                "content": content
            },
            "toRecipients": mail_to_list,
            "ccRecipients": cc_to_list
        }
    }
    if importance:
        message["message"]["importance"] = "high"
    print("message", message)
    # send message
    mail_res = requests.post(f"https://graph.microsoft.com/v1.0/users/resourcerequest@blend360.com/sendMail",
                             headers={
                                 "Authorization": f"Bearer {mail_token}", "Content-Type": "application/json"
                             },
                             json=message
                             )

    if mail_res.status_code >= 300:
        raise Exception(mail_res.content)
