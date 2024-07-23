import pandas as pd
import pm4py

from src.classes_ import CCM
from src.mapping.ocel_to_ccm import OCELToCCMMapper

ocel: pm4py.OCEL = pm4py.read_ocel("data/ocel_50_objects.jsonocel")

ocel_to_ccm_mapper: OCELToCCMMapper = OCELToCCMMapper(ocel)

ccm: CCM = ocel_to_ccm_mapper.ccm

table: pd.DataFrame = ccm.get_extended_table()
print(table)