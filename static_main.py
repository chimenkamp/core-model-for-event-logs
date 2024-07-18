import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL

log: OCEL = pm4py.read_ocel("data/ocel_50_objects.jsonocel")
extt: pd.DataFrame = log.get_extended_table()
print(log)
