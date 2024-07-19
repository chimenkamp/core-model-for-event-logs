from datetime import datetime
from random import random, randint
from typing import List, Dict, Any

import pandas as pd
import pm4py
from pm4py.objects.ocel.obj import OCEL

from src.classes_ import ProcessEvent, Attribute, Activity, Object, IS, CCM, DataSource


def map_ocel_event_to_ccm_event(df: pd.DataFrame) -> List[ProcessEvent]:
    events = []

    for index, row in df.iterrows():
        attributes = [
            Attribute("ocel:eid", row["ocel:eid"]),
            Attribute("ocel:timestamp", row["ocel:timestamp"]),
            Attribute("concept:instance", row["concept:instance"]),
            Attribute("id:id", row["id:id"]),
            Attribute("cpee:activity_uuid", row["cpee:activity_uuid"]),
            Attribute("lifecycle:transition", row["lifecycle:transition"]),
            Attribute("cpee:lifecycle:transition", row["cpee:lifecycle:transition"]),
            Attribute("data", row["data"]),
            Attribute("concept:endpoint", row["concept:endpoint"]),
            Attribute("sub:root", row["sub:root"]),
        ]

        event = ProcessEvent(event_id=row["ocel:eid"], attributes=attributes)
        event.add_activity(Activity(activity_type=row["ocel:activity"], activity_id=str(randint(0, 1000))))

        events.append(event)

    return events


def map_ocel_object_to_ccm_object(object_def: Dict[str, str]) -> Object:
    return Object(object_id=object_def["ocel:oid"], object_type=object_def["ocel:type"], attributes=[])


def remove_inner_dict(d: Dict[str, Dict[Any, Any]]) -> Dict[str, Any]:
    new_dict: Dict[str, Any] = {}
    for key, value in d.items():
        if isinstance(value, dict):
            for inner_key, inner_value in value.items():
                new_dict[key] = inner_value
        else:
            new_dict[key] = value
    return new_dict


def get_object_for_event(event_id: str, objects: pd.DataFrame, relations: pd.DataFrame) -> pd.DataFrame:
    relation_row = relations.loc[relations["ocel:eid"] == event_id]
    object_id = relation_row["ocel:oid"].values[0]
    object_row = objects.loc[objects["ocel:oid"] == object_id]
    return object_row


# Load and map an existing log from ocel to ccm
log: OCEL = pm4py.read_ocel(
    "/Users/christianimenkamp/Documents/Data-Repository/Community/turmv4_batch4_ocel/log.jsonocel")
extt: pd.DataFrame = log.get_extended_table()

ccm: CCM = CCM()

all_event_ids = list(log.events["ocel:eid"].unique())

ccm_events: List[ProcessEvent] = map_ocel_event_to_ccm_event(log.events)

cmm_objects: List[Object] = []
cmm_data_sources: List[IS] = []

for eid in all_event_ids:

    object_def: dict[str, str] = (remove_inner_dict(get_object_for_event(eid, log.objects, log.relations).to_dict()))
    ccm_object: Object = map_ocel_object_to_ccm_object(object_def)
    event = next(filter(lambda x: x.event_id == eid, ccm_events))
    ccm_object.add_event(event)

    datasource: IS = IS("SAP ERP")
    ccm_object.add_data_source(datasource)

    cmm_data_sources.append(datasource)
    cmm_objects.append(ccm_object)


for event in ccm_events:
    ccm.add_event(event)

for obj in cmm_objects:
    ccm.add_object(obj)

for ds in cmm_data_sources:
    ccm.add_information_system(ds)
