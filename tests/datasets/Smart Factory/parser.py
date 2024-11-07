import xml
from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
import pm4py
import xmltodict

from src.wrapper.ocel_wrapper import OCELWrapper

from pathlib import Path
from typing import List, Optional


def get_file_paths(folder_path: str, pattern: str = "*.xes", recursive: bool = False) -> List[str]:
    folder = Path(folder_path)
    if not folder.exists():
        raise FileNotFoundError(f"Folder not found: {folder_path}")

    if not folder.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {folder_path}")

    # Use rglob for recursive search, glob for non-recursive
    glob_func = folder.rglob if recursive else folder.glob

    # Get all files matching the pattern and convert to strings
    return [str(file_path) for file_path in glob_func(pattern) if file_path.is_file()]


def find_value_by_key(data_list: List[Dict[str, str]], search_key: str) -> str:
    for item in data_list:
        if item['@key'] == search_key:
            return item['@value']
    raise ValueError(f"Key {search_key} not found in data_list.")


def id_exists(data_list: List[Dict[str, Any]], new_id: str) -> bool:
    return any(item['object_id'] == new_id for item in data_list)


class SensorStreamParser:
    def __init__(self) -> None:
        """
        Initializes the SensorStreamParser class.
        """
        self.objects = []
        self.iot_events = []
        self.process_events = []
        self.iot_devices = []
        self.observations = []
        self.information_systems = []
        self.object_object_relationships = []
        self.event_object_relationships = []
        self.event_event_relationships = []
        self.event_data_source_relationships = []

    def parse_sensor_stream_log(self, process_events: List[Dict[str, Any]]) -> OCELWrapper:
        """
        Parses a SensorStream log and returns an OCELWrapper object.

        :param sensorstream_log: List of SensorStream events as dictionaries.
        :return: OCELWrapper object containing the parsed data.
        """

        # xml_dict["log"]["trace"](dict_2)["event"](list_5)[0](dict_3)["string"] <- concept:name and org:resource
        # xml_dict["log"]["trace"](dict_2)["event"](list_5)[0](dict_3)["date"] <- timestamp and operation end time
        # xml_dict["log"]["trace"](dict_2)["event"](list_5)[0](dict_3)["list"] <- stream:datastream

        # stream:datastream(list_178)[0](dict_8) <- stream:point

        for process_event in process_events:
            concept_name: str = find_value_by_key(process_event["string"], "concept:name")
            org_resource: str = find_value_by_key(process_event["string"], "org:resource")

            self.create_object(org_resource)

            timestamp: str = find_value_by_key(process_event["date"], "time:timestamp")
            operation_end_time: str = find_value_by_key(process_event["date"], "operation_end_time")

            process_event_id: str = f"process_event_{str(uuid4())[:8]}"

            self.process_events.append({
                "event_id": process_event_id,
                "timestamp": timestamp,
                "activity": {
                    "activity_type": concept_name
                },
                "attributes": {
                    "org_resource": org_resource,
                    "operation_end_time": operation_end_time
                }

            })

            data_stream: List[dict] = process_event["list"]["list"]

            for stream_point in data_stream:
                attr_system: str = stream_point.get("@stream:system", "NO VALUE")
                attr_system_type: str = stream_point.get("@stream:system_type", "NO VALUE")
                attr_observation: str = stream_point.get("@stream:observation", "NO VALUE")
                attr_procedure_type: str = stream_point.get("@stream:procedure_type", "NO VALUE")
                attr_interaction_type: str = stream_point.get("@stream:interaction_type", "NO VALUE")
                point_time_stamp: str = stream_point.get("date", {}).get("@stream:timestamp", "NO VALUE")
                point_value: str = stream_point.get("string", {}).get("@stream:value", "NO VALUE")

                observation_id: str = f"observation_{str(uuid4())[:8]}"

                self.observations.append({
                    "observation_id": observation_id,
                    "timestamp": point_time_stamp,
                    "iot_device_link": "",
                    "value": point_value,
                    "attributes": {
                        "system": attr_system,
                        "system_type": attr_system_type,
                        "observation": attr_observation,
                        "procedure_type": attr_procedure_type,
                        "interaction_type": attr_interaction_type
                    }
                })

                self.event_event_relationships.append({
                    "event_id": process_event_id,
                    "derived_from_event_id": observation_id
                })

            self.event_object_relationships.append({
                "event_id": process_event_id,
                "object_id": org_resource
            })

        return OCELWrapper(
            objects=self.objects,
            iot_events=self.iot_events,
            process_events=self.process_events,
            iot_devices=self.iot_devices,
            observations=self.observations,
            information_systems=self.information_systems,
            object_object_relationships=self.object_object_relationships,
            event_object_relationships=self.event_object_relationships,
            event_event_relationships=self.event_event_relationships,
            event_data_source_relationships=self.event_data_source_relationships
        )

    def create_object(self, resource_id: str) -> None:
        """
        Create objects from a list of objects.

        :param objects: List of objects.
        """
        if not id_exists(self.objects, resource_id):
            self.objects.append({
                "object_id": resource_id,
                "object_type": "resource"
            })


if __name__ == "__main__":
    event_log_paths: List[str] = get_file_paths("./event_logs")

    all_process_events: List[Dict[str, Any]] = []
    for FILE_NAME in event_log_paths:
        with open(FILE_NAME, 'r') as file:
            xml_string = file.read()
        # Parse xml string to dict
        try:
            xml_dict = xmltodict.parse(xml_string)
        except xml.parsers.expat.ExpatError as e:
            print("ERROR in line", FILE_NAME)
            print(e)
            continue

        events: List[dict] = xml_dict["log"]["trace"]["event"]
        if isinstance(events, dict):
            events = [events]
        all_process_events.extend(events)

    # Parse the log
    parser = SensorStreamParser()
    ocel_wrapper = parser.parse_sensor_stream_log(all_process_events)

    ocel_pointer: pm4py.OCEL = ocel_wrapper.get_ocel()
    # ocel_wrapper.save_ocel("v4_output.jsonocel")
    print(ocel_pointer.get_summary())
    discovered_df = pm4py.discover_oc_petri_net(ocel_pointer)
    pm4py.view_ocpn(discovered_df)
