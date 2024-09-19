import abc
import json
from jsonschema import validate as validate_json
import jsonschema


class BaseValidator(abc.ABC):

    def validate(self, to_validate_definition: dict | str) -> bool: ...


class JsonValidator(BaseValidator):

    def __init__(self, schema_path: str):

        self.schema_path = schema_path

        with open(schema_path, 'r') as schema_file:
            self.schema = json.load(schema_file)

    def validate(self, to_validate_definition: dict | str) -> bool:

        if isinstance(to_validate_definition, str):
            with open(to_validate_definition, 'r') as schema_file:
                to_validate = json.load(schema_file)
        else:
            to_validate = to_validate_definition

        try:
            validate_json(instance=to_validate, schema=self.schema)
            return True
        except jsonschema.exceptions.ValidationError as e:
            print(e)
            return False
        except jsonschema.exceptions.SchemaError as e:
            print(e)
            return False
        except jsonschema.exceptions.RefResolutionError as e:
            print(e)
            return False
        except Exception as e:
            print(e)
            return False
