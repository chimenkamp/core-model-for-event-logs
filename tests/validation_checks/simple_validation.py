import json

from src.validation.jsonocel_validation import JsonOCEL

with open("case_study_manufacturing_ocel.jsonocel") as f:
    jsonocel_data = json.load(f)


validator = JsonOCEL(jsonocel_data)
is_valid, errors = validator.validate()
print(f"Is the JSON OCEL data valid? {is_valid}")
if not is_valid:
    for error in errors:
        print(error)
