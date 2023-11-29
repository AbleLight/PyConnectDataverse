import json
import os
from datetime import datetime, timedelta
from typing import Dict
from ..sessions import DataverseSession

CACHE_DIR = '_cache'

class ColumnDef:
    """
    Represents the definition of a column in the entity metadata.
    """
    def __init__(self, display_name: str, logical_name: str, schema_name: str, attribute_type: str, related=None):
        self.display_name = display_name
        self.logical_name = logical_name
        self.schema_name = schema_name
        self.attribute_type = attribute_type
        self.related = related

    def __repr__(self):
        return f"ColumnDef(logical_name={self.logical_name}, attribute_type={self.attribute_type}, related={self.related})"

class EntityDef:
    """
    Represents the definition of an entity, including its columns.
    """
    def __init__(self, display_name: str, logical_name: str, key_column: str, entity_set_name: str, columns: Dict[str, ColumnDef]):
        self.display_name = display_name
        self.logical_name = logical_name
        self.key_column = key_column
        self.entity_set_name = entity_set_name
        self._columns = columns

    def get_column(self, name: str) -> ColumnDef:
        """
        Retrieves a column by its display name or logical name.
        If the column is not found, raises a KeyError.
        """
        # First, try to get the column by its display name
        column = self._columns.get(name)
        if column is not None:
            return column

        # If not found, search by logical name
        for col in self._columns.values():
            if col.logical_name == name:
                return col

        # If the column is still not found, raise an error
        raise KeyError(f"Column '{name}' not found in EntityDef '{self.display_name}'.")
    
    def __repr__(self):
        return f"EntityDef(logical_name={self.logical_name}, entity_set_name={self.entity_set_name}, columns={self._columns})"

class EntityDict:
    """
    Represents a collection of entity definitions, allowing for easy access and management.
    """
    def __init__(self, entities: Dict[str, EntityDef] = {}):
        self.entities = entities

    def add_entity(self, display_name, logical_name, key_column, entity_set_name, columns):
        entity_def = EntityDef(display_name, logical_name, key_column, entity_set_name, columns)
        self.entities[display_name] = entity_def

    def get_entity(self, name: str) -> EntityDef:
        entity = self.entities.get(name)
        if entity is None:
            # Search by logical name as a fallback
            for ent in self.entities.values():
                if ent.logical_name == name:
                    return ent
            raise KeyError(f"Entity '{name}' not found.")
        return entity

    def __repr__(self):
        return f"EntityDict(entities={self.entities})"

    @classmethod
    def from_json(cls, json_data):
        entity_dict = cls()
        for display_name, entity_data in json_data.items():
            columns = {col_name: ColumnDef(**col_data) for col_name, col_data in entity_data['columns'].items()}
            entity_def = EntityDef(entity_data['display_name'], entity_data['logical_name'], entity_data['key_column'], entity_data['entity_set_name'], columns)
            entity_dict.entities[display_name] = entity_def
        return entity_dict

    def to_json(self):
        return {
            display_name: {
                'display_name': display_name,
                'logical_name': entity.logical_name,
                'key_column': entity.key_column,
                'entity_set_name': entity.entity_set_name,
                'columns': {
                    col_name: {
                        'display_name': col_def.display_name,
                        'logical_name': col_def.logical_name,
                        'schema_name': col_def.schema_name,
                        'attribute_type': col_def.attribute_type,
                        'related': col_def.related
                    } for col_name, col_def in entity._columns.items()}
            } for display_name, entity in self.entities.items()
        }
    
def get_display_name(entity_json):
    """
    Extracts and returns the display name from a given JSON structure.
    """
    try:
        return entity_json['DisplayName']['UserLocalizedLabel']['Label']
    except (KeyError, TypeError):
        return ''

def is_recently_modified(file_path, hours=1):
    """
    Checks if the given file was modified within the specified number of hours.
    """
    last_modified_time = datetime.fromtimestamp(os.path.getmtime(file_path))
    return datetime.now() - last_modified_time < timedelta(hours=hours)

def parse_attributes(attributes):
    """
    Parses and returns a dictionary of ColumnDef objects from the given list of attributes.
    """
    columns = {}
    for attribute in attributes:
        if 'AttributeOf' in attribute and attribute['AttributeOf']:
            continue

        display_name = get_display_name(attribute)
        related = attribute['Targets'][0] if 'Targets' in attribute else None
        columns[display_name] = ColumnDef(
            display_name=display_name,
            logical_name=attribute['LogicalName'],
            schema_name=attribute['SchemaName'],
            attribute_type=attribute['AttributeType'],
            related=related
        )

    return columns

def get_entity_definitions(session: DataverseSession):
    """
    Retrieves entity definitions from the Dataverse session.
    """
    if not os.path.exists(CACHE_DIR):
        os.makedirs(CACHE_DIR)

    entities_file = os.path.join(CACHE_DIR, 'entities.json')
    entities_debug_file = os.path.join(CACHE_DIR, 'entities_debug.json')

    if not os.path.exists(entities_file) or not is_recently_modified(entities_file):
        entity_dict = EntityDict()
        entities_debug = []
        response = session.query("EntityDefinitions", {"$expand": "Attributes,OneToManyRelationships"})
        all_entities = response.json()

        for entity in all_entities['value']:
            # Assuming entities with 'able_' prefix are relevant
            if not str(entity['LogicalName']).startswith('able_'): 
                continue

            entity_name = get_display_name(entity)
            if entity_name == '':
                continue

            entities_debug.append(entity)
            columns = parse_attributes(entity['Attributes'])
            logical_name = entity['LogicalName']
            key_column = entity['PrimaryNameAttribute']
            entity_dict.add_entity(entity_name, logical_name, key_column, entity['EntitySetName'], columns)

        if len(entities_debug) > 0:
            with open(entities_debug_file, "w") as outfile:
                json.dump(entities_debug, outfile)

        if entity_dict.entities:
            with open(entities_file, "w") as outfile:
                json.dump(entity_dict.to_json(), outfile)

    # Read from cache
    with open(entities_file, "r") as this_file:
        json_contents = json.load(this_file)
        return EntityDict.from_json(json_contents)