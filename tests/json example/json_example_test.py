from src.validation.base import JsonValidator

SCHEMA_PATH: str = "../../schemas/json_schema.json"

if JsonValidator(SCHEMA_PATH).validate("example.json"):
    print("Validation successful.")
else:
    print("Validation failed.")