import json
import typing
from abc import ABC, abstractmethod
from typing import List, Union, Optional, Literal, Any
import datetime

import pandas as pd

from src.utils.table_utils import create_extended_table
from src.utils.query_utils import query_classes
from src.utils.types import CCMEntry
from src.utils.visualize_utils import create_graph
from uuid import uuid4


class Attribute(CCMEntry):
    """
    Class to represent an attribute of an object or event.
    """

    def __init__(self, key: str, value: Union[str, float, int], attribute_id: Optional[str] = None) -> None:
        super().__init__()
        self.attribute_id: str = attribute_id if attribute_id else str(uuid4())
        self.key: str = key
        self.value: Union[str, float, int] = value

    def __repr__(self) -> str:
        return f"Attribute(key={self.key}, value={self.value})"

    def serialize(self) -> dict:
        return {
            "key": self.key,
            "value": self.value
        }


class Activity(CCMEntry):
    """
    Class to represent an activity.
    """

    def __init__(self, activity_id: Optional[str] = None,
                 activity_type: Optional[Union[str, float, int]] = None) -> None:
        super().__init__()
        self.activity_id: str = activity_id if activity_id else str(uuid4())
        self.activity_type: Optional[Union[str, float, int]] = activity_type

    def serialize(self) -> dict:
        return {
            "activity_id": self.activity_id,
            "activity_value": self.activity_type
        }

    def add_activity_value(self, activity_value: Optional[Union[str, float, int]]) -> None:
        self.activity_type = activity_value


class DataSource(CCMEntry):
    """
    Class to represent a data source.
    """

    def __init__(self, data_source_type: Literal["information system", "iot device"],
                 data_source_id: Optional[str] = None) -> None:
        super().__init__()
        self.data_source_id: str = data_source_id if data_source_id else str(uuid4())
        self.data_source_type: Literal["information system", "iot device"] = data_source_type

    def serialize(self) -> dict:
        return {
            "data_source_id": self.data_source_id,
            "data_source_type": self.data_source_type
        }


class Event(CCMEntry):
    """
    Class to represent an event.
    """

    def __init__(self,
                 event_typ: Literal['process event', 'iot event'], timestamp: Optional[datetime.datetime] = None,
                 event_id: Optional[str] = None, objs: Optional[List['Object']] = None,
                 data_source: Optional['DataSource'] = None, derived_from_events: List[typing.Self] = None) -> None:
        super().__init__()
        self.event_id: str = event_id if event_id else str(uuid4())
        self.event_type: Literal['process event', 'iot event'] = event_typ
        self.related_objects: List[Object] = objs if objs else []
        self.data_source: Optional[DataSource] = data_source
        self.derived_from_events: List[typing.Self] = derived_from_events
        self.timestamp: datetime.datetime = timestamp if timestamp else datetime.datetime.now()

    def __repr__(self) -> str:
        return (f"Event(id={self.event_id}, obj={self.related_objects}, data_source={self.data_source}, "
                f"derived_from_event={self.derived_from_event})")

    def serialize(self) -> dict:
        return {
            "event_id": self.event_id,
            "object": [obj.object_id for obj in self.related_objects] if self.related_objects else None,
            "data_source": self.data_source.data_source_id if self.data_source else None,
            "derived_from_event": self.derived_from_event.event_id if self.derived_from_event else None
        }

    def add_object(self, obj: 'Object') -> None:
        self.related_objects.append(obj)

    def add_data_source(self, data_source: 'DataSource') -> None:
        self.data_source = data_source

    def add_derived_from_event(self, derived_from_event: 'Event') -> None:
        self.derived_from_event = derived_from_event


class ProcessEvent(Event):
    """
    Class to represent a process event.
    """

    def __init__(self, event_id: Optional[str] = None) -> None:
        super().__init__("process event", event_id)
        self.process_event_id: str = self.event_id
        self.activities: List[Activity] = []

    def serialize(self) -> dict:
        data = super().serialize()
        data["process_event_id"] = self.process_event_id
        return data

    def add_activity(self, activity: Activity) -> None:
        self.activities.append(activity)


class IoTEvent(Event):
    """
    Class to represent an IoT event.
    """

    def __init__(self, event_id: Optional[str] = None) -> None:
        super().__init__("iot event", event_id)
        self.iot_event_id: str = self.event_id

    def serialize(self) -> dict:
        data = super().serialize()
        data["iot_event_id"] = self.iot_event_id
        return data


class IS(DataSource):
    """
    Class to represent an information system.
    """

    def __init__(self, is_id: Optional[str] = None, event: Optional[ProcessEvent] = None) -> None:
        super().__init__("information system", is_id)
        self.is_id: str = self.data_source_id
        self.event: Optional[ProcessEvent] = event

    def serialize(self) -> dict:
        return {
            "is_id": self.is_id,
            "event": self.event.event_id if self.event else None
        }

    def add_event(self, event: ProcessEvent) -> None:
        self.event = event


class SOSA:
    class Observation(CCMEntry):
        """
        Class to represent an observation.
        """

        def __init__(self, observation_id: Optional[str] = None, iot_device: Optional['SOSA.IoTDevice'] = None) -> None:
            super().__init__()
            self.observation_id: str = observation_id if observation_id else str(uuid4())
            self.iot_device: Optional['SOSA.IoTDevice'] = iot_device

        def __repr__(self) -> str:
            return f"Observation(id={self.observation_id}, iot_device={self.iot_device})"

        def serialize(self) -> dict:
            return {
                "observation_id": self.observation_id,
                "iot_device": self.iot_device.data_source_id if self.iot_device else None
            }

        def add_iot_device(self, iot_device: Optional['SOSA.IoTDevice']) -> None:
            self.iot_device = iot_device

    class IoTDevice(DataSource):
        """
        Class to represent an IoT device.
        """

        def __init__(self, iot_device_id: Optional[str] = None) -> None:
            super().__init__("iot device", iot_device_id)
            self.iot_device_id: str = self.data_source_id

        def serialize(self) -> dict:
            return {
                "iot_device_id": self.iot_device_id
            }


class Object(CCMEntry):
    """
    Class to represent an object.
    """

    def __init__(self, object_type: str, object_id: Optional[str] = None,
                 data_source: Optional[DataSource] = None) -> None:
        super().__init__()
        self.object_id: str = object_id if object_id else str(uuid4())
        self.object_type: str = object_type
        self.related_objects: List[Object] = []
        self.data_source: Optional[DataSource] = data_source

    def __repr__(self) -> str:
        return f"Object(id={self.object_id})"

    def serialize(self) -> dict:
        return {
            "object_id": self.object_id
        }

    def add_related_object(self, related_object: 'Object') -> None:
        self.related_objects.append(related_object)

    def add_data_source(self, data_source: Optional[DataSource]) -> None:
        self.data_source = data_source


class CCM(CCMEntry):
    """
    Class to represent a Common-Core Model (CCM) dataset.
    """

    def __init__(self) -> None:
        super().__init__()
        self.event_log: List[Event] = []
        self.objects: List[Object] = []
        self.data_sources: List[DataSource] = []
        self.information_systems: List[IS] = []
        self.iot_devices: List[SOSA.IoTDevice] = []
        self.activities: List[Activity] = []
        self.observation: List[SOSA.Observation] = []

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

    def add_observation(self, observation: SOSA.Observation) -> None:
        self.observation.append(observation)

    def get_extended_table(self) -> pd.DataFrame:
        return create_extended_table(self.objects, self.event_log, self.data_sources)

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
        :param output_file: The path where the output file will be saved.
        :return: None
        """
        try:
            from graphviz import Digraph
        except ImportError:
            raise ImportError("Please install Graphviz using 'pip install graphviz'")

        # Assuming create_graph is a predefined function
        dot = create_graph(
            self.objects,
            self.event_log,
            self.data_sources,
            self.iot_devices,
            self.information_systems,
            self.observation
        )
        dot.format = 'png'
        dot.render(output_file, view=True)

    def query(self, query_str: str, return_as_dataframe: bool = True) -> pd.DataFrame:
        """
        Filters events based on a query string and returns the result as a DataFrame.

        Query Examples:
        - "SELECT * FROM Event WHERE self.event_type == 'process event'"
        - "SELECT * FROM Object WHERE self.object_type == 'tank'"
        - "SELECT observation_id FROM SOSA.Observation WHERE self.value > 0.5"

        :param query_str: The query string to filter the events.
        :param return_as_dataframe: Whether to return the result as a DataFrame or a list of objects.
        :return: A DataFrame of the filtered events.
        """
        # Assuming query_classes is a predefined function
        classes = {
            'Object': self.objects,
            'Event': self.event_log,
            'InformationSystem': self.information_systems,
            'IoTDevice': self.iot_devices,
            'Observation': self.observation,
            'Activity': self.activities
        }
        return query_classes(query_str, classes, return_as_dataframe)
