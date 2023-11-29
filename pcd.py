import json
import datetime
import os
from dataverse.sessions import DataverseSessions 
from dataverse.api import DataverseAPI

# Parameters
PathToEnvironmentJSON = "data-analytics-dev.json"
OUTPUT_PATH = f"_output/{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
os.makedirs(OUTPUT_PATH)

config = json.load(open(PathToEnvironmentJSON))
environmentURI = config["environmentURI"]
clientID = config["clientID"]
tenantID = config["tenantID"]

api = DataverseAPI(DataverseSessions.getSession(environmentURI, clientID, tenantID))

def create(entity_name, csv):
    results = api.create(entity_name, csv)
    
    with open(f"{OUTPUT_PATH}/{entity_name}.json", "w") as outfile:
        outfile.write(json.dumps(results))

# done: create('Survey List Sanction', 'data/surveys/Sanctions.csv')
# done: create('Survey List Category', 'data/surveys/Survey List Category.csv')
# done: create('Survey List Finding Category', 'data/surveys/Survey List Finding Category.csv')
# done: create('Survey List Finding Subcategory', 'data/surveys/Survey List Finding Subcategory.csv')
# done: create('Survey List Service', 'data/surveys/Survey List Services.csv')
# done: create('Survey List State Survey Type', 'data/surveys/Survey List State Survey Type.csv')
# done: create('Survey List State Service', 'data/surveys/Survey List State Service.csv')
# done: create('Survey', 'data/surveys/Survey.csv')
create('Survey Finding', 'data/surveys/Survey Finding.csv')