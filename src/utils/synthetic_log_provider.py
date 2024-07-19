import random
from typing import List, Dict
from datetime import datetime, timedelta
import pandas as pd

from src.classes_ import CCM, Event, ProcessEvent, IoTEvent, SOSA, Activity, Attribute, Object, IS
from src.utils.types import CCMEntry


class SyntheticLogProvider:
    def __init__(self, log_length: int):
        self.log_length = log_length
        self.ccm = CCM()
        self.example_objects = self.create_example_objects()
        self.transition_matrix = self.create_transition_matrix()

    def create_example_objects(self) -> Dict[str, List[CCMEntry]]:
        example_objects = {
            'Object': [
                Object(object_id="batch1", object_type="batch", attributes=[
                    Attribute(key="recipe", value="13a4a45"),
                    Attribute(key="location", value="Supplier Warehouse")
                ]),
                Object(object_id="tank1", object_type="tank", attributes=[
                    Attribute(key="flow_status", value="drift"),
                    Attribute(key="product_flow", value=206),
                    Attribute(key="location", value="Main Manufacturing Plant")
                ]),
                Object(object_id="sensor1", object_type="sensor", attributes=[
                    Attribute(key="precision", value="0.1"),
                    Attribute(key="unit", value="l/h"),
                    Attribute(key="location", value="Main Manufacturing Plant")
                ])
            ],
            'Event': [
                ProcessEvent(event_id="event1", attributes=[
                    Attribute(key="label", value="sample taken"),
                    Attribute(key="location", value="Supplier Warehouse")
                ]),
                IoTEvent(event_id="event2", attributes=[
                    Attribute(key="label", value="detect peak"),
                    Attribute(key="location", value="Main Manufacturing Plant")
                ])
            ],
            'Activity': [
                Activity(activity_id="activity1", activity_type="take sample"),
                Activity(activity_id="activity2", activity_type="apply sample")
            ],
            'DataSource': [
                IS(system_id="SAP ERP"),
                SOSA.IoTDevice(device_id="device1")
            ],
            'SOSA.IoTDevice': [
                SOSA.IoTDevice(device_id="device2")
            ],
            'SOSA.Observation': [
                SOSA.Observation(observation_id="obs1", observed_property="flow_status", value="normal"),
                SOSA.Observation(observation_id="obs2", observed_property="drift", value=0.1)
            ]
        }
        return example_objects

    def create_transition_matrix(self) -> Dict[str, List[str]]:
        return {
            'ProcessEvent': ['ProcessEvent', 'IoTEvent'],
            'IoTEvent': ['ProcessEvent', 'IoTEvent']
        }

    def generate_synthetic_log(self) -> CCM:
        current_event_type = 'ProcessEvent'
        for _ in range(self.log_length):
            current_event = self.create_event(current_event_type)
            self.ccm.add_event(current_event)
            current_event_type = random.choice(self.transition_matrix[current_event_type])
        return self.ccm

    def create_event(self, event_type: str) -> Event:
        event_id = f"event_{len(self.ccm.event_log) + 1}"
        attributes = [random.choice(self.example_objects['Object']).attributes[0]]
        if event_type == 'ProcessEvent':
            event = ProcessEvent(event_id=event_id, attributes=attributes)
            event.add_activity(random.choice(self.example_objects['Activity']))
        else:
            event = IoTEvent(event_id=event_id, attributes=attributes)
        event.timestamp = datetime.now() + timedelta(minutes=len(self.ccm.event_log) * 5)
        return event

