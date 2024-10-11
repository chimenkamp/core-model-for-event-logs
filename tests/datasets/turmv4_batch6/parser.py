from datetime import datetime
from pprint import pprint

import pm4py
import yaml
from typing import Any, Dict, List, Union

from src.wrapper.ocel_wrapper import OCELWrapper

import json
from typing import List, Dict, Any

def group_by_observation_id(dicts: List[Dict[str, any]]) -> List[List[Dict[str, any]]]:
    """
    Groups a list of dictionaries by their 'observation_id' field.

    :param dicts: A list of dictionaries, each containing an 'observation_id' field.
    :return: A list of lists, where each sublist contains dictionaries with the same 'observation_id'.
    """
    groups = {}
    for d in dicts:
        obs_id = d.get('observation_id')
        if obs_id in groups:
            groups[obs_id].append(d)
        else:
            groups[obs_id] = [d]

    return list(groups.values())

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

    def parse_sensorstream_log(self, sensorstream_log: List[Dict[str, Any]]) -> OCELWrapper:
        """
        Parses a SensorStream log and returns an OCELWrapper object.

        :param sensorstream_log: List of SensorStream events as dictionaries.
        :return: OCELWrapper object containing the parsed data.
        """
        for event in sensorstream_log:
            if "event" in event:
                event = event["event"]
                process_event_id: str | None = None
                if "concept:name" in event:
                    process_event_id = self._parse_process_event(event)
                if "stream:datastream" in event:
                    data_stream: List[Dict] = event["stream:datastream"]
                    self._parse_sensorstream_data(data_stream, process_event_id)

        # link observations with the same id to one iot event
        self.link_observation_to_iot_event()
        # After processing all data, pass it to the OCELWrapper constructor
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

    def link_observation_to_iot_event(self, process_event_id: str | None = None) -> None:
        """
        Link observations with the same id to one iot event
        """

        groups = group_by_observation_id(self.observations)
        for observations in groups:
            iot_event_id: str = f"iot_event_{observations[0]['observation_id']}"
            self.iot_events.append({
                "event_id": iot_event_id,
                "timestamp": observations[0]["timestamp"],
            })

            for i, observation in enumerate(observations):
                observation_id = observation["observation_id"] + f"_{i}"
                observation["observation_id"] = observation_id

                self.event_event_relationships.append({
                    "event_id": iot_event_id,
                    "derived_from_event_id": observation_id
                })

            if process_event_id:
                self.event_event_relationships.append({
                    "event_id": process_event_id,
                    "derived_from_event_id": iot_event_id
                })
    def _parse_process_event(self, process_event: Dict[str, Any]) -> str:
        # Create Process events
        # only add the event if it is not already in the list
        if not any(x["event_id"] == process_event["id:id"] for x in self.process_events):
            self.process_events.append({
                "event_id": process_event["id:id"],
                "timestamp": datetime.now(),
                "activity": {
                    "activity_type": process_event["concept:name"]
                }
            })
        return process_event["id:id"]

    def _parse_sensorstream_data(self, sensorstream_events: List[Dict[str, Any]], process_event_id: str | None) -> None:
        """
        Parses a SensorStream event and adds the corresponding IoT data and relationships to the parser's internal state.

        :param sensorstream_events: A dictionary representing a SensorStream event from the XES log.
        """

        points: List[Dict[str, Any]] = [x["stream:point"] for x in sensorstream_events if "stream:point" in x]

        iot_device_id = [x["stream:name"] for x in sensorstream_events if "stream:name" in x][0]
        iot_source = [x["stream:source"] for x in sensorstream_events if "stream:source" in x][0]

        # Add IoT device to the local list
        if iot_device_id not in self.iot_devices:
            self.iot_devices.append({
                "data_source_id": iot_device_id,
                "attributes": iot_source
            })


        # Process the sensor points and create IoT events (observations)
        for point in points:
            if type(point) is not dict:
                print(type(point))
                continue
            id_ = point.get("stream:id", "unknown_id")
            observation = {
                "observation_id": "obs_" + id_,
                "iot_device_id": iot_device_id,
                "timestamp": point.get("stream:timestamp", ""),
                "attributes": {
                    "value": point.get("stream:value", ""),
                    "source_details": point.get("stream:source", {})
                }
            }
            self.observations.append(observation)

            # Create event-object relationships (link IoT events to their data source)
            self.event_data_source_relationships.append({
                "event_id": observation["observation_id"],
                "data_source_id": iot_device_id
            })


def load_yaml(yaml_file: str) -> List[Any]:
    """
    Loads a YAML file, returning all documents as a list.

    :param yaml_file: The path to the YAML file.
    :return: A list containing the loaded YAML documents.
    """
    with open(yaml_file, 'r') as file:
        return list(yaml.safe_load_all(file))


def infer_schema(data: Any) -> Union[Dict[str, Any], List[Any], str]:
    """
    Infers the schema (types) of the data structure.

    :param data: The loaded YAML data.
    :return: A structure reflecting the types of the data.
    """
    if isinstance(data, dict):
        return {key: infer_schema(value) for key, value in data.items()}
    elif isinstance(data, list):
        if len(data) > 0:
            return [infer_schema(item) for item in data]
        else:
            return "EmptyList"
    else:
        return type(data).__name__


def extract_schema(yaml_file: str) -> List[Dict[str, Any]]:
    """
    Extracts the schema of a YAML file by identifying keys and their corresponding types,
    handling multiple documents.

    :param yaml_file: The path to the YAML file.
    :return: A list of dictionaries, each containing the keys and types of one document.
    """
    yaml_docs = load_yaml(yaml_file)
    return [infer_schema(doc) for doc in yaml_docs]


if __name__ == "__main__":
    # Example usage: extracting the schema from the YAML file
    # schema = extract_schema("file.yaml")
    # for i, doc_schema in enumerate(schema, 1):
    #     print(f"Document {i} Schema:")
    #     print(doc_schema)
    #     print("\n")

    # Example usage: loading and printing the YAML documents

    scheme = load_yaml("file.yaml")
    parser = SensorStreamParser()
    res: OCELWrapper = parser.parse_sensorstream_log(scheme)
    tabel = res.get_extended_table()
    print(tabel)
    ocel_pointer: pm4py.OCEL = res.get_ocel()
    res.save_ocel("v2_output.jsonocel")
    # print("Schema Len:", len(scheme))
    # all_keys = [list(e.keys())[0] for e in scheme]
    # pprint(scheme[0:5])
    # print("All Keys:", all_keys)
    # # get all events with a data attribute
    # data_events = [e for e in scheme if "data" in e.get("event", {})]
    # print("Data Events:", len(data_events))
