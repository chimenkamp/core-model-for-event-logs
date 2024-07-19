import json
from abc import ABC, abstractmethod
from typing import List, Union, Optional, Literal, Any
import datetime

import pandas as pd

from src.utils import _match_conditions, _parse_query


class CCMClass(ABC):
    """
    Abstract base class to enforce serialization implementation.
    """

    @abstractmethod
    def serialize(self) -> dict:
        pass


class Attribute(CCMClass):
    """
    Class to represent an attribute of an object or event.
    """

    def __init__(self, key: str, value: Union[str, float, int]) -> None:
        self.key = key
        self.value = value

    def __repr__(self) -> str:
        return f"Attribute(key={self.key}, value={self.value})"

    def serialize(self) -> dict:
        return {
            "key": self.key,
            "value": self.value
        }


class Event(CCMClass):
    """
    Class to represent an event.
    """

    def __init__(self, event_id: str, event_type: Literal["process event", "iot event"],
                 attributes: List[Attribute], data_source: Optional['DataSource'] = None) -> None:
        self.event_id = event_id
        self.event_type = event_type
        self.attributes: List[Attribute] = attributes
        self.timestamp: datetime.datetime = datetime.datetime.now()
        self.data_source: Optional['DataSource'] = data_source
        self.related_objects: List['Object'] = []

    def __repr__(self) -> str:
        return (f"Event(id={self.event_id}, type={self.event_type}, attributes={self.attributes}, "
                f"object_ids={[o.object_id for o in self.related_objects]}, data_source_id={self.data_source.source_id if self.data_source else None})")

    def add_data_source(self, data_source: 'DataSource') -> None:
        self.data_source = data_source
        data_source.record_event(self)

    def serialize(self) -> dict:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "attributes": [attr.serialize() for attr in self.attributes],
            "timestamp": self.timestamp.isoformat(),
            "data_source": self.data_source.serialize() if self.data_source else None,
            "related_objects": [obj.object_id for obj in self.related_objects]
        }


class IoTEvent(Event):
    """
    Class to represent an IoT event.
    """

    def __init__(self, event_id: str, attributes: List[Attribute], data_source: Optional['DataSource'] = None) -> None:
        super().__init__(event_id, "iot event", attributes, data_source)


class Activity(CCMClass):
    """
    Class to represent an activity.
    """

    def __init__(self, activity_id: str, activity_type: str) -> None:
        self.activity_id: str = activity_id
        self.activity_type: str = activity_type

    def serialize(self) -> dict:
        return {
            "activity_id": self.activity_id,
            "activity_type": self.activity_type
        }


class ProcessEvent(Event):
    """
    Class to represent a process event.
    """

    def __init__(self, event_id: str, attributes: List[Attribute], data_source: Optional['DataSource'] = None) -> None:
        super().__init__(event_id, "process event", attributes, data_source)
        self.activities: List[Activity] = []

    def add_activity(self, activity: Activity) -> None:
        self.activities.append(activity)

    def serialize(self) -> dict:
        data = super().serialize()
        data["activities"] = [activity.serialize() for activity in self.activities]
        return data


class DataSource(CCMClass):
    """
    Class to represent a data source.
    """

    def __init__(self, source_id: str, source_type: str) -> None:
        self.source_id = source_id
        self.source_type = source_type
        self.events: List[Event] = []

    def record_event(self, event: Event) -> None:
        event.data_source = self
        self.events.append(event)

    def __repr__(self) -> str:
        return f"DataSource(id={self.source_id}, type={self.source_type}, events={self.events})"

    def serialize(self) -> dict:
        return {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "events": [event.event_id for event in self.events]
        }


class IS(DataSource):
    """
    Class to represent an information system.
    """

    def __init__(self, system_id: str) -> None:
        super().__init__(system_id, "information system")


class SOSA:
    """
    Class to represent the SOSA ontology / Namespace.
    """

    class Observation(CCMClass):
        """
        Class to represent an observation.
        """

        def __init__(self, observation_id: str, observed_property: str, value: Union[str, float, int]) -> None:
            self.observation_id = observation_id
            self.observed_property = observed_property
            self.value = value

        def __repr__(self) -> str:
            return f"Observation(id={self.observation_id}, property={self.observed_property}, value={self.value})"

        def serialize(self) -> dict:
            return {
                "observation_id": self.observation_id,
                "observed_property": self.observed_property,
                "value": self.value
            }

    class IoTDevice(DataSource):
        """
        Class to represent an IoT device.
        """

        def __init__(self, device_id: str) -> None:
            super().__init__(device_id, "iot device")
            self.observations: List[SOSA.Observation] = []

        def add_observation(self, observation: 'SOSA.Observation') -> None:
            self.observations.append(observation)

        def serialize(self) -> dict:
            data = super().serialize()
            data["observations"] = [obs.serialize() for obs in self.observations]
            return data


class Object(CCMClass):
    """
    Class to represent an object.
    """

    def __init__(self, object_id: str, object_type: str, attributes: List[Attribute]) -> None:
        self.object_id = object_id
        self.object_type = object_type
        self.attributes = attributes
        self.related_objects: List['Object'] = []
        self.events: List[Event] = []
        self.data_sources: List[DataSource] = []

    def add_event(self, event: Event) -> None:
        event.related_objects.append(self)
        self.events.append(event)

    def add_related_object(self, related_object: 'Object') -> None:
        self.related_objects.append(related_object)

    def add_data_source(self, data_source: DataSource) -> None:
        self.data_sources.append(data_source)

    def __repr__(self) -> str:
        return (
            f"Object(id={self.object_id}, type={self.object_type}, attributes={self.attributes}, events={self.events})")

    def serialize(self) -> dict:
        return {
            "object_id": self.object_id,
            "object_type": self.object_type,
            "attributes": [attr.serialize() for attr in self.attributes],
            "related_objects": [obj.object_id for obj in self.related_objects],
            "events": [event.event_id for event in self.events],
            "data_sources": [ds.source_id for ds in self.data_sources]
        }


class CCM(CCMClass):
    """
    Class to represent a Common-Core Model (CCM) dataset.
    """

    def __init__(self) -> None:
        self.event_log: List[Event] = []
        self.objects: List[Object] = []
        self.data_sources: List[DataSource] = []
        self.information_systems: List[IS] = []
        self.iot_devices: List[SOSA.IoTDevice] = []
        self.activities: List[Activity] = []

    def add_event(self, event: Event) -> None:
        self.event_log.append(event)

    def add_object(self, obj: Object) -> None:
        self.objects.append(obj)

    def add_information_system(self, information_system: IS) -> None:
        self.information_systems.append(information_system)
        self.data_sources.append(information_system)

    def add_iot_device(self, iot_device: SOSA.IoTDevice) -> None:
        self.iot_devices.append(iot_device)
        self.data_sources.append(iot_device)

    def add_activity(self, activity: Activity) -> None:
        self.activities.append(activity)

    def get_events(self) -> List[Event]:
        return self.event_log

    def get_extended_table(self) -> pd.DataFrame:
        rows = []

        for event in self.event_log:
            related_objects = [obj for obj in self.objects if obj in event.related_objects]

            for related_object in related_objects:
                row: dict[str, Any] = {
                    'ccm:event_id': event.event_id,
                    'ccm:event_type': event.event_type,
                    'ccm:timestamp': event.timestamp,
                    'ccm:object_id': related_object.object_id,
                    'ccm:object_type': related_object.object_type,
                    'ccm:data_source_id': event.data_source.source_id if event.data_source else None,
                    'ccm:data_source_type': event.data_source.source_type if event.data_source else None
                }

                for attr in event.attributes:
                    row[f'event:{attr.key}'] = attr.value

                for attr in related_object.attributes:
                    row[f'object:{attr.key}'] = attr.value

                if event.data_source:
                    for ds in self.data_sources:
                        if ds.source_id == event.data_source.source_id:
                            for attr in ds.events:
                                if attr.event_id == event.event_id:
                                    for e_attr in attr.attributes:
                                        row[f'data_source_event:{e_attr.key}'] = e_attr.value

                if isinstance(event, ProcessEvent):
                    for activity in event.activities:
                        row[f'activity:{activity.activity_id}_type'] = activity.activity_type

                rows.append(row)

        df = pd.DataFrame(rows)
        return df

    def save_to_json(self, file_path: str) -> None:
        data = self.serialize()

        with open(file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def __repr__(self) -> str:
        return (f"CCM(event_log={self.event_log}, objects={self.objects}, data_sources={self.data_sources}, "
                f"information_systems={self.information_systems}, iot_devices={self.iot_devices})")

    def serialize(self) -> dict:
        return {
            "ccm:events": [event.serialize() for event in self.event_log],
            "ccm:objects": [obj.serialize() for obj in self.objects],
            # "ccm:data_sources": [ds.serialize() for ds in self.data_sources],
            "ccm:information_systems": [is_system.serialize() for is_system in self.information_systems],
            "ccm:iot_devices": [iot_device.serialize() for iot_device in self.iot_devices],
            "ccm:activities": [activity.serialize() for activity in self.activities]
        }

    def visualize(self, output_file: str) -> None:
        """
        This method generates a visualization of the CCM dataset using Graphviz.
        :param output_file:
        :return:
        """

        try:
            from graphviz import Digraph
        except ImportError:
            raise ImportError("Please install Graphviz using 'pip install graphviz'")

        dot = Digraph(comment='CCM Diagram')
        edges = set()

        for obj in self.objects:
            dot.node(obj.object_id, label=f"Object\n{obj.object_type}\n{obj.object_id}", shape='box', style='filled',
                     color='lightyellow')

            for attr in obj.attributes:
                attr_id = f"{obj.object_id}_{attr.key}"
                dot.node(attr_id, label=f"Attribute\n{attr.key}: {attr.value}", shape='ellipse', style='filled',
                         color='lightgrey')
                edge = (obj.object_id, attr_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

            for related_obj in obj.related_objects:
                edge = (obj.object_id, related_obj.object_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

        for event in self.event_log:
            event_label = "Process Event" if event.event_type == "process event" else "IoT Event"
            dot.node(event.event_id, label=f"{event_label}\n{event.event_id}", shape='box', style='filled',
                     color='lightblue')

            for attr in event.attributes:
                attr_id = f"{event.event_id}_{attr.key}"
                dot.node(attr_id, label=f"Event Attribute\n{attr.key}: {attr.value}", shape='ellipse', style='filled',
                         color='lightgreen')
                edge = (event.event_id, attr_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

            for obj in event.related_objects:
                edge = (obj.object_id, event.event_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

            if isinstance(event, ProcessEvent):
                for activity in event.activities:
                    dot.node(activity.activity_id, label=f"Activity\n{activity.activity_type}\n{activity.activity_id}",
                             shape='diamond', style='filled', color='lightcoral')
                    edge = (event.event_id, activity.activity_id)
                    if edge not in edges:
                        dot.edge(*edge)
                        edges.add(edge)

        for ds in self.data_sources:
            dot.node(ds.source_id, label=f"Data Source\n{ds.source_type}\n{ds.source_id}", shape='box', style='filled',
                     color='lightpink')

            for event in ds.events:
                edge = (ds.source_id, event.event_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

        for device in self.iot_devices:
            dot.node(device.source_id, label=f"IoT Device\n{device.source_id}", shape='box', style='filled',
                     color='lightcyan')

            for obs in device.observations:
                obs_id = obs.observation_id
                dot.node(obs_id, label=f"Observation\n{obs.observed_property}: {obs.value}", shape='ellipse',
                         style='filled', color='lightgoldenrod')
                edge = (device.source_id, obs_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

            for event in device.events:
                edge = (device.source_id, event.event_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

        for is_system in self.information_systems:
            dot.node(is_system.source_id, label=f"Information System\n{is_system.source_id}", shape='box',
                     style='filled', color='black', fontcolor='white')

            for event in self.event_log:
                if event.data_source and isinstance(event.data_source,
                                                    IS) and event.data_source.source_id == is_system.source_id:
                    edge = (is_system.source_id, event.event_id)
                    if edge not in edges:
                        dot.edge(*edge)
                        edges.add(edge)

        for obj in self.objects:
            for ds in obj.data_sources:
                edge = (obj.object_id, ds.source_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

        dot.format = 'png'
        dot.render(output_file, view=True)

    def filter_events(self, conditions: dict) -> List[Event]:
        """
        Filters events based on the provided conditions.
        :param conditions: A dictionary with keys as attributes and values as desired values.
        :return: A list of events that match the conditions.
        """
        filtered_events = []

        for event in self.event_log:
            if _match_conditions(event, conditions):
                filtered_events.append(event)

        return filtered_events

    def query(self, query_str: str) -> pd.DataFrame:
        """
        Filters events based on a query string and returns the result as a DataFrame.
        :param query_str: The query string, e.g., "event_type == 'iot event' and event:temperature > 25".
        :return: A DataFrame of the filtered events.
        """
        conditions = _parse_query(query_str)
        filtered_events = self.filter_events(conditions)
        return pd.DataFrame([event.serialize() for event in filtered_events])
