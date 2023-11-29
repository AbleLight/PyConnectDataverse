import authenticate_with_msal
import json

# Parameters
PathToEnvironmentJSON = "data-analytics-dev.json"

# Getting access token.
authentication = authenticate_with_msal.getAuthenticatedSession(PathToEnvironmentJSON)
session = authentication[0]
environmentURI = authentication[1]

# an example download request to the URI
request_uri = f'{environmentURI}api/data/v9.2/$metadata'
session.headers['Accept'] = 'application/xml'
r = session.get(request_uri)

if r.status_code != 200:
    print(f'Request failed. Error code: {r.status_code}')

else:
    print("Request successful")

# Writing to output.json
with open("output/metadata.xml", "w") as outfile:
    outfile.write(r.text)