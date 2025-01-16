from datetime import datetime
from typing import Dict, List, Any, Optional
from uuid import uuid4
import xml

import pm4py
import xmltodict
from pathlib import Path
from pydantic import BaseModel, ConfigDict

from src.types_defintion.event_definition import IotEvent, ProcessEvent, Observation
from src.types_defintion.object_definition import Object, ObjectClassEnum
from src.types_defintion.relationship_definitions import (
    EventObjectRelationship,
    EventEventRelationship,
    ObjectObjectRelationship
)
from src.wrapper.ocel_wrapper import COREMetamodel


class StreamPoint(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    system: str
    system_type: str
    observation: str
    procedure_type: str
    interaction_type: str
    timestamp: datetime
    value: str


class ProcessEventData(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    concept_name: str
    org_resource: str
    timestamp: datetime
    operation_end_time: datetime
    data_stream: List[StreamPoint]

def get_file_paths(folder_path: str, pattern: str = "*.xes", recursive: bool = False) -> List[str]:
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")
    if not folder.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {folder_path}")

    glob_func = folder.rglob if recursive else folder.glob
    return [str(file_path) for file_path in glob_func(pattern) if file_path.is_file()]


def find_value_by_key(data_list: List[Dict[str, str]], search_key: str) -> str:
    """Find a value in a list of dictionaries by its key."""
    for item in data_list:
        if item['@key'] == search_key:
            return item['@value']
    raise ValueError(f"Key {search_key} not found in data_list.")


class SensorStreamParser:
    def __init__(self) -> None:
        """Initialize the SensorStreamParser with empty collections."""
        self.objects: List[Object] = []
        self.process_events: List[ProcessEvent] = []
        self.observations: List[Observation] = []
        self.event_event_relationships: List[EventEventRelationship] = []
        self.event_object_relationships: List[EventObjectRelationship] = []

    def _parse_stream_point(self, stream_point: Dict[str, Any]) -> StreamPoint:
        """Parse a stream point from the raw data."""
        return StreamPoint(
            system=stream_point.get("@stream:system", "NO VALUE"),
            system_type=stream_point.get("@stream:system_type", "NO VALUE"),
            observation=stream_point.get("@stream:observation", "NO VALUE"),
            procedure_type=stream_point.get("@stream:procedure_type", "NO VALUE"),
            interaction_type=stream_point.get("@stream:interaction_type", "NO VALUE"),
            timestamp=stream_point.get("date", {}).get("@stream:timestamp", "NO VALUE"),
            value=stream_point.get("string", {}).get("@stream:value", "NO VALUE")
        )

    def _parse_process_event(self, event: Dict[str, Any]) -> ProcessEventData:
        """Parse a process event from the raw data."""
        return ProcessEventData(
            concept_name=find_value_by_key(event["string"], "concept:name"),
            org_resource=find_value_by_key(event["string"], "org:resource"),
            timestamp=find_value_by_key(event["date"], "time:timestamp"),
            operation_end_time=find_value_by_key(event["date"], "operation_end_time"),
            data_stream=[self._parse_stream_point(point) for point in event["list"]["list"]]
        )

    def _create_object(self, resource_id: str) -> None:
        """Create a new object if it doesn't exist."""
        if not any(obj.object_id == resource_id for obj in self.objects):
            self.objects.append(Object(
                object_id=resource_id,
                object_type="resource",
                object_class=ObjectClassEnum.RESOURCE,
                attributes={}
            ))

    def _create_process_event(self, event_data: ProcessEventData) -> ProcessEvent:
        """Create a process event from parsed data."""
        return ProcessEvent(
            event_id=f"process_event_{str(uuid4())[:8]}",
            event_class="process_event",
            event_type=event_data.concept_name,
            timestamp=event_data.timestamp,
            activity=event_data.concept_name,
            attributes={
                "org_resource": event_data.org_resource,
                "operation_end_time": event_data.operation_end_time,
                "case:concept:name": event_data.concept_name
            }
        )

    def _create_observation(self, stream_point: StreamPoint) -> Observation:
        """Create an observation from a stream point."""
        return Observation(
            event_id=f"observation_{str(uuid4())[:8]}",
            event_class="observation",
            event_type="observation",
            timestamp=stream_point.timestamp,
            attributes={
                "system": stream_point.system,
                "system_type": stream_point.system_type,
                "observation": stream_point.observation,
                "procedure_type": stream_point.procedure_type,
                "interaction_type": stream_point.interaction_type,
                "value": stream_point.value
            }
        )

    def parse_sensor_stream_log(self, process_events_raw: List[Dict[str, Any]]) -> COREMetamodel:
        """Parse a SensorStream log and return an OCELWrapper object."""

        for event_raw in process_events_raw:
            # Parse the event data
            event_data = self._parse_process_event(event_raw)

            # Create object for the resource
            self._create_object(event_data.org_resource)

            # Create process event
            process_event = self._create_process_event(event_data)
            self.process_events.append(process_event)

            # Create event-object relationship
            self.event_object_relationships.append(EventObjectRelationship(
                event_id=process_event.event_id,
                object_id=event_data.org_resource
            ))

            # Process stream points
            for stream_point in event_data.data_stream:
                observation = self._create_observation(stream_point)
                self.observations.append(observation)

                # Create event-event relationship
                self.event_event_relationships.append(EventEventRelationship(
                    event_id=process_event.event_id,
                    derived_from_event_id=observation.event_id,
                    qualifier="derived_from"
                ))

        return COREMetamodel(
            objects=self.objects,
            process_events=self.process_events,
            observations=self.observations,
            event_object_relationships=self.event_object_relationships,
            event_event_relationships=self.event_event_relationships
        )


def parse_event_logs(folder_path: str) -> COREMetamodel:
    """Parse all event logs in a folder and return an OCELWrapper."""
    event_log_paths = get_file_paths(folder_path)
    all_process_events: List[Dict[str, Any]] = []

    for file_path in event_log_paths:
        try:
            with open(file_path, 'r') as file:
                xml_dict = xmltodict.parse(file.read())

            events = xml_dict["log"]["trace"]["event"]
            if isinstance(events, dict):
                events = [events]
            all_process_events.extend(events)

        except xml.parsers.expat.ExpatError as e:
            print(f"ERROR in file {file_path}:")
            print(e)
            continue

    parser = SensorStreamParser()
    return parser.parse_sensor_stream_log(all_process_events)


if __name__ == "__main__":
    ocel_wrapper = parse_event_logs("./event_logs")
    ocel = ocel_wrapper.get_ocel()
    print(ocel.get_summary())
    discovered_df = pm4py.discover_oc_petri_net(ocel)
    pm4py.view_ocpn(discovered_df)
    table = ocel.get_extended_table()
    print(table)