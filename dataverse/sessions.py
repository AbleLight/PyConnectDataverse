from datetime import datetime, timedelta
import os
import time
import msal
import requests
import json
import urllib

AUTHORITY_BASE = "https://login.microsoftonline.com/"
SCOPE_SUFFIX = "user_impersonation"
CACHE_DIR = '_cache'

class DataverseSession(requests.Session):
    def __init__(self, environmentURI: str) -> None:
        super().__init__()
        self.environmentURI = environmentURI

    def query(self, endpoint: str, query_params: dict = None):
        uri = self.build_uri(endpoint, query_params)

        print(f'Sending GET request to: {uri}')
        response = super().get(uri)

        if response.status_code not in [200, 201]:
            self._handle_response_error(response)
        
        print("Request successful")
        return response

    def mutate(self, entity_set_name: str, payloads: list = []):
        self.headers.update({"Prefer" : "return=representation"})

        # the post uri
        row = 0
        successful_updates = 0
        failures = 0
        expected_updates = len(payloads)
        percent_complete = 0
        timeStart = time.perf_counter()

        request_uri = self.build_uri(entity_set_name)
        
        processed = []  
        for payload in payloads:
            req = requests.Request('POST', request_uri, json=payload, headers = self.headers).prepare()
            r = self.send(req)
            payload['_REQUEST'] = {
                'REQUEST_URI': request_uri,
                'HTTP_RESPONSE': r.status_code,
                'HTTP_CONTENT': json.loads(r.content.decode('utf-8'))
            }
            
            if r.status_code != 201:
                failures += 1

            else:
                successful_updates +=1 

            row += 1
            if round(row/expected_updates * 100,0) != percent_complete:
                percent_complete = round(row/expected_updates * 100,0)
                print(f"{percent_complete}% complete")

            processed.append(payload)

        print(f'{successful_updates} UPDATES MADE OF {expected_updates} EXPECTED UPDATES. {failures} FAILURES.') 
        print(f'IMPORTING TOOK: {round(time.perf_counter() - timeStart,0)} SECONDS ')

        return processed
    
    def accept(self, encoding: str):
        self.headers['ACCEPT'] = encoding

    def build_uri(self, endpoint: str, query_params: dict = {}):
        uri = f'{self.environmentURI.removesuffix("/")}/api/data/v9.2/{endpoint.removesuffix("/")}'
        uri += '?' + '&'.join([f'{k}={urllib.parse.quote(v, safe="/")}' for k, v in query_params.items()])
        return uri.rstrip('?')

    def _handle_response_error(self, response):
        try:
            message = response.json().get('error', {}).get('message', '')
        except json.JSONDecodeError:
            message = response.text
        raise requests.HTTPError(f'Error ({response.status_code}): {message}')

class DataverseSessions:
    @staticmethod
    def getSession(environmentURI: str, clientID: str, tenantID: str):
        token_file = f'{CACHE_DIR}/.token'

        # Check if token is recent
        if os.path.exists(token_file):
            last_modified_time = datetime.fromtimestamp(os.path.getmtime(token_file))
            if datetime.now() - last_modified_time < timedelta(hours=1):
                print("Token was fetched within the last hour. Returning cached token.")
                with open(token_file, "r") as file:
                    token = file.read()
                    return DataverseSessions._create_dataverse_session(environmentURI, token)

        # Acquire new token
        scope = [environmentURI + '/' + SCOPE_SUFFIX]
        authority = AUTHORITY_BASE + tenantID
        app = msal.PublicClientApplication(clientID, authority=authority)

        print("A local browser window will open for you to sign in. CTRL+C to cancel.")
        result = app.acquire_token_interactive(scope)

        if "access_token" in result:
            print("Token received successfully")
            with open(token_file, "w") as file:
                file.write(result["access_token"])
            return DataverseSessions._create_dataverse_session(environmentURI, result["access_token"])
        else:
            DataverseSessions._handle_token_error(result)

    @staticmethod
    def _create_dataverse_session(environmentURI: str, token: str):
        session = DataverseSession(environmentURI)
        session.headers.update({
            'Authorization': f'Bearer {token}',
            'OData-MaxVersion': '4.0', 
            'OData-Version': '4.0', 
            'If-None-Match': 'null', 
            'Accept': 'application/json'
        })
        return session

    @staticmethod
    def _handle_token_error(result: dict):
        error = result.get("error", "Unknown error")
        description = result.get("error_description", "No description provided")
        correlation_id = result.get("correlation_id", "N/A")
        raise Exception(f"Error obtaining token: {error}, Description: {description}, Correlation ID: {correlation_id}")
