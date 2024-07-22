import pm4py

from src.classes_ import CCM
from src.mapping.ocel_to_ccm import OCELToCCMMapper


ocel: pm4py.OCEL = pm4py.read_ocel("/Users/christianimenkamp/Documents/Data-Repository/Community/turmv4_batch4_ocel/log.jsonocel")
ocel_to_ccm_mapper: OCELToCCMMapper = OCELToCCMMapper(ocel)
ccm: CCM = ocel_to_ccm_mapper.ccm

print(ccm)