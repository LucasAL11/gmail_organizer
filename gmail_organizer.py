from __future__ import print_function

import os.path

import pandas as pd
import re

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

def get_creds():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

email_metadata = []
def process_email_metadata(request_id, response, exception):
    global email_metadata

    message_id = response.get('id')
    headers = response.get('payload').get('headers');
    if(headers is not None):
        for header in headers:
            if header['name'] == "From":
                username, domain = re.match(
                    r'(?:.*<)?(.*)@(.*?)(?:>.*|$)', header['value']
                ).groups()
                email_metadata.append({
                    'message_id':message_id,
                    'username':username,
                    'domain':domain})
                break

def get_inbox_emails(service):
    # Call the Gmail API
    response = service.users().messages().list(
            userId='me',
            labelIds=['INBOX'],
            maxResults=5000
    ).execute()

    # Retrieve all message ids
    messages = []
    messages.extend(response['messages'])
    while 'nextPageToken' in response:
      page_token = response['nextPageToken']
      response = service.users().messages().list(
              userId='me',
              labelIds=['INBOX'],
              maxResults=5000,
              pageToken=page_token
      ).execute()
      messages.extend(response['messages'])

# Retrieve the metadata for all messages
    step = 100
    num_messages = len(messages)
    for batch in range(0, num_messages, step):
        batch_req = service.new_batch_http_request(callback=process_email_metadata)
        for i in range(batch, min(batch + step, num_messages)):
            batch_req.add(service.users().messages().get(
                userId='me',
                id=messages[i]['id'],
                format="metadata")
            )
        batch_req.execute()
    
def main():
    creds = get_creds()
    service = build('gmail', 'v1', credentials=creds)

    get_inbox_emails(service)

if __name__ == '__main__':
    main()