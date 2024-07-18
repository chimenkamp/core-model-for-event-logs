import random
import string
import pandas as pd
from typing import List, Optional, Union, Literal
from uuid import uuid4
import datetime
import json
from graphviz import Digraph
import pm4py
from pm4py import OCEL

from src.classes_ import IS, SOSA, Object, Attribute, Activity, Event, ProcessEvent, CCM, IoTEvent

# Example IS and IoT Devices
EXAMPLE_IS: List[IS] = [
    IS("SAP_ERP"),
    IS("SAP_S4HANA"),
    IS("SAP_BW"),
]

EXAMPLE_IOT_DEVICE: List[SOSA.IoTDevice] = [
    SOSA.IoTDevice("sensor1"),
    SOSA.IoTDevice("sensor2"),
    SOSA.IoTDevice("sensor3"),
]


# Function to map OCEL objects to CCM objects
def object_from_ocel(log: pd.DataFrame) -> List[Object]:
    units: List[str] = ["bar", "kg", "m", "l", "cm", "mm", "g", "s", "min", "h", "°C", "°F", "K", "Pa", "bar", "N", "J"]
    objects: List[Object] = []

    for _, row in log.objects.iterrows():
        object_id = row['ocel:oid']
        object_type = row['ocel:type']
        attributes = [
            Attribute(key="Unit", value=random.choice(units)),
        ]
        obj = Object(object_id, object_type, attributes)
        objects.append(obj)
    return objects


# Function to map OCEL events to CCM events
def event_from_ocel(log: pd.DataFrame, activities: List[Activity]) -> List[Event]:
    events: List[Event] = []
    for _, row in log.events.iterrows():
        e_id = row['ocel:eid']
        timestamp = row['ocel:timestamp']
        activity = row['ocel:activity']
        instance = row['concept:instance']
        id_ = row['id:id']
        activity_uuid = row['cpee:activity_uuid']
        lifecycle_transition = row['lifecycle:transition']
        cpee_lifecycle_transition = row['cpee:lifecycle:transition']
        data = row['data']
        endpoint = row['concept:endpoint']
        root = row['sub:root']

        attributes: List[Attribute] = [
            Attribute(key='activity_uuid', value=activity_uuid),
            Attribute(key='lifecycle_transition', value=lifecycle_transition),
            Attribute(key='cpee_lifecycle_transition', value=cpee_lifecycle_transition),
            Attribute(key='data', value=data),
            Attribute(key='endpoint', value=endpoint),
            Attribute(key='timestamp', value=timestamp),
            Attribute(key='instance', value=instance),
            Attribute(key='id', value=id_),
            Attribute(key='root', value=root),
        ]

        process_event = ProcessEvent(e_id, attributes, random.choice(EXAMPLE_IS))
        relevant_activities = [a for a in activities if a.activity_type == activity]
        for act in relevant_activities:
            process_event.add_activity(act)
        events.append(process_event)

    return events


def load_ocel_to_ccm_from_dataframe(log: OCEL) -> CCM:
    ccm = CCM()

    # get all activities
    table: pd.DataFrame = log.get_extended_table()
    activities: List[Activity] = [Activity(a, str(uuid4())) for a in table['ocel:activity'].unique()]
    objects: List[Object] = object_from_ocel(log)
    events_process: List[Event] = event_from_ocel(log, activities)

    events_iot: List[Event] = [
        IoTEvent(str(uuid4()), [Attribute("label", "detect peak")], random.choice(EXAMPLE_IOT_DEVICE)),
        IoTEvent(str(uuid4()), [Attribute("label", "value change")], random.choice(EXAMPLE_IOT_DEVICE)),
    ]

    for obj in objects:
        obj.add_event(random.choice(events_process))
        ccm.add_object(obj)

    for act in activities:
        ccm.add_activity(act)

    for event in events_process:
        ccm.add_event(event)

    for is_ in EXAMPLE_IS:
        ccm.add_information_system(is_)

    for iot_device in EXAMPLE_IOT_DEVICE:
        ccm.add_iot_device(iot_device)

    for event in events_iot:
        ccm.add_event(event)

    return ccm


# Function to generate a random value
def generate_random_value() -> Union[str, int, float]:
    value_type = random.choice(['str', 'int', 'float'])
    if value_type == 'str':
        return ''.join(random.choices(string.ascii_letters + string.digits, k=5))
    elif value_type == 'int':
        return random.randint(1, 100)
    else:
        return round(random.uniform(1.0, 100.0), 2)


# Example usage (assuming pm4py and the OCEL file are properly defined)

ocel_log = pm4py.read_ocel(
    '/Users/christianimenkamp/Documents/Data-Repository/Community/turmv4_batch4_ocel/log.jsonocel')

ccm = load_ocel_to_ccm_from_dataframe(ocel_log)
ext = ccm.get_extended_table()
ccm.visualize("ccm.png")
print(ext)
