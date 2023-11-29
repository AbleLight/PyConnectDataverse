import json
from typing import Dict
import pandas as pd
import urllib
from dataverse._requests.metadata import EntityDef, get_entity_definitions
from .sessions import DataverseSession

class DataverseAPI:
    def __init__(self, session: DataverseSession):
        self.session = session
        self.entities = get_entity_definitions(session)

    def create(self, display_name: str, csv: str):
        entity = self.entities.get_entity(display_name)
        df = self._get_dataframe(csv)
        payloads = self._build_payloads(entity, df)
        return self.session.mutate(entity.entity_set_name, payloads)
    
    def relate(self, from_entity: str, to_entity: str, csv: str):
        entity = self.entities.get_entity(display_name)
        df = self._get_dataframe(csv)
        payloads = self._build_payloads(entity, df)
        return self.session.mutate(entity.entity_set_name, payloads)
    
    def _get_dataframe(self, csv: str):
        # read the CSV and convert to dataframe
        df = pd.read_csv(csv)
        return json.loads(df.to_json(orient = "records"))
    
    def _build_payloads(self, entity: EntityDef, df):
        payloads = []
        for record in df:
            payloads.append(self._build_payload(entity, record))
        return payloads

    def _build_payload(self, entity: EntityDef, values: Dict[str, str | int]):
        payload = {}
        for column_name, value in values.items():
            if value == None: continue

            # Find the logical name using the Names map
            column = entity.get_column(column_name)

            if column.attribute_type == "Lookup":
                related_entity = self.entities.get_entity(column.related)
                related_entity_key_column = related_entity.key_column
                # Adjust lookup binding as per the odata.bind format
                payload[f"{column.schema_name}@odata.bind"] = f'/{related_entity.entity_set_name}({related_entity_key_column}=\'{(value)}\')'
            else:
                payload[column.logical_name] = value
        return payload