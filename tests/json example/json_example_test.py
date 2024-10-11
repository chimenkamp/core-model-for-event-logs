import pandas as pd

from src.validation.base import JsonValidator
from src.wrapper.ocel_wrapper import OCELWrapper

ocel_wrapper: OCELWrapper = OCELWrapper().load_from_json_schema("example.json")

ocel_table: pd.DataFrame = ocel_wrapper.get_extended_table()

print(ocel_table)

ocel_wrapper.save_to_json("example_output_[custom_schema].json")
ocel_wrapper.save_ocel("example_output_[ocel_schema].jsonocel")
