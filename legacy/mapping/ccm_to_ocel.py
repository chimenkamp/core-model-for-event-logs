import json
import datetime
from typing import List, Optional, Union

import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL
from uuid import uuid4

from src.classes_ import ProcessEvent, CCM


class CCMToOcelMapper:
    """
    Class to map data from a custom format to OCEL format.
    """

    def __init__(self, ccm: CCM) -> None:
        self.ccm = ccm
        self.ocel: OCEL = self.map_to_ocel()

    def map_to_ocel(self) -> OCEL:
        """
        Converts the custom data format (CCM) to OCEL format.

        :return: An OCEL object.
        """

        events_df = pd.DataFrame(columns=["ocel:eid", "ocel:activity", "ocel:timestamp", "ocel:vmap"])
        objects_df = pd.DataFrame(columns=["ocel:oid", "ocel:type", "ocel:ovmap"])
        relations_df = pd.DataFrame(columns=["ocel:eid", "ocel:oid"])

        for ccm_object in self.ccm.objects:
            obj_data = {
                "ocel:oid": ccm_object.object_id,
                "ocel:type": ccm_object.object_type,
                "ocel:ovmap": {
                    "object_id": ccm_object.object_id,
                    "object_type": ccm_object.object_type
                }
            }
            if ccm_object.data_source:
                obj_data["ocel:ovmap"]["data_source"] = ccm_object.data_source.data_source_type

            objects_df = pd.concat([objects_df, pd.DataFrame(obj_data)])

        for ccm_event in self.ccm.event_log:
            event_data = {
                "ocel:eid": ccm_event.event_id,
                "ocel:activity": ccm_event.event_type,
                "ocel:timestamp": ccm_event.timestamp.isoformat(),
                "ocel:vmap": {}
            }
            if ccm_event.data_source:
                event_data["ocel:vmap"]["data_source"] = ccm_event.data_source.data_source_type
            if isinstance(ccm_event, ProcessEvent) and ccm_event.activity:
                event_data["ocel:activity"] = ccm_event.activity.activity_type
            events_df = pd.concat([events_df, pd.DataFrame(event_data)])

            index: int = 0
            for obj in ccm_event.related_objects:
                relation_data = {
                    "ocel:eid": ccm_event.event_id,
                    "ocel:oid": obj.object_id,
                }

                relations_df = pd.concat([relations_df, pd.DataFrame(relation_data, index=[0])])
                index += 1

        ocel = OCEL(events=events_df, objects=objects_df, relations=relations_df)

        return ocel
