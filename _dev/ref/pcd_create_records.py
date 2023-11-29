import authenticate_with_msal
import json
import pandas as pd
import time
from requests import Request, Session 

# Parameters
PathToEnvironmentJSON = "data-analytics-dev.json"
EntityBeingAddedTo = "able_locations"
PathToCSVOfRecords = "data\surveys\locations.csv"
AttributesToReturn = "" # optionally include the attributes to return, otherwise all are returned

columns = {
    "AU": {
        "schema_name": "able_au",
        "type": "text",
    },
    "Description": {
        "schema_name": "able_description",
        "type": "text",
    },
    "State": {
        "schema_name": "able_State",
        "type": "text",
        "lookup": {
            "set_name": "able_states",
            "keys": { 
                "able_name": "State"
            }
        }
    },
    "Status": {
        "schema_name": "statecode",
        "type": "number",
    }
}

# read the CSV and convert to dataframe
df = pd.read_csv(PathToCSVOfRecords)

records = json.loads(df.to_json(orient = "records"))

# Getting access token.
authentication = authenticate_with_msal.getAuthenticatedSession(PathToEnvironmentJSON)
session = authentication[0]
environmentURI = authentication[1]
session.headers.update({"Prefer" : "return=representation"})

# the post uri
request_uri = f'{environmentURI}api/data/v9.2/{EntityBeingAddedTo}{AttributesToReturn}'

row = 0
successful_updates = 0
failures = 0
expected_updates = len(df)
percent_complete = 0
timeStart = time.perf_counter()

#{"name":"Sample Account","primarycontactid@odata.bind":"/contacts(00000000-0000-0000-0000-000000000001)"}

processed = []

for record in records:
    payload = {}
    for column in columns:
        if column in record:
            column_name = column
            if 'schema_name' in columns[column]:
                column_name = columns[column]['schema_name']
                
            value = record[column]
            
            if columns[column] == "text" or ("type" in columns[column] and columns[column]["type"] == "text"):
                value = f"{value}"
            
            if "lookup" in columns[column]:    
                lookup = columns[column]['lookup']

                lookup_value = value
                if columns[column] == "text" or ("type" in columns[column] and columns[column]["type"] == "text"):
                    lookup_value = f"'{value}'"

                query_params = []
                for key in lookup['keys']:
                    query_params.append(f'{key}={lookup_value}')
                

                payload[f'{column_name}@odata.bind'] = f'/{lookup["set_name"]}({",".join(query_params)})'
            else:
                payload[column_name] = value

    req = Request('POST', request_uri, json=payload, headers = session.headers).prepare()
    r = session.send(req)
    payload['HTTP_RESPONSE'] = r.status_code
    payload['HTTP_CONTENT'] = json.loads(r.content.decode('utf-8'))

    if r.status_code != 201:
        failures += 1

    else:
        successful_updates +=1 

    row += 1
    if round(row/expected_updates * 100,0) != percent_complete:
        percent_complete = round(row/expected_updates * 100,0)
        print(f"{percent_complete}% complete")

    processed.append(payload)
    break

print(f'{successful_updates} UPDATES MADE OF {expected_updates} EXPECTED UPDATES. {failures} FAILURES.') 
print(f'IMPORTING TOOK: {round(time.perf_counter() - timeStart,0)} SECONDS ')

# Writing to output.json
with open("output/output.json", "w") as outfile:
    outfile.write(json.dumps(processed))