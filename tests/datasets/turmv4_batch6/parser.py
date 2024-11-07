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

    def parse_sensor_stream_log(self, sensorstream_log: List[Dict[str, Any]]) -> OCELWrapper:
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
                    self._parse_sensor_stream_data(data_stream, process_event_id)

        # link observations with the same id to one iot event
        self.link_observation_to_iot_event()

        # link iot event to objects
        self.link_iot_events_to_objects()

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
                "event_type": observations[0]['observation_id'],
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

    def _parse_sensor_stream_data(self, sensorstream_events: List[Dict[str, Any]],
                                  process_event_id: str | None) -> None:
        """
        Parses a SensorStream event and adds the corresponding IoT data and relationships to the parser's internal state.

        :param sensorstream_events: A dictionary representing a SensorStream event from the XES log.
        """

        points: List[Dict[str, Any]] = [x["stream:point"] for x in sensorstream_events if "stream:point" in x]

        iot_device_id = [x["stream:name"] for x in sensorstream_events if "stream:name" in x][0]
        iot_source = [x["stream:source"] for x in sensorstream_events if "stream:source" in x][0]

        # Add IoT device to the local list or find the existing object
        iot_device_id_with_prefix: str = "iot_device_" + iot_device_id
        iot_device = next((dev for dev in self.iot_devices if dev["data_source_id"] == iot_device_id_with_prefix), None)

        if iot_device is None:
            # If not already stored, add a new IoT device
            iot_device = {
                "data_source_id": iot_device_id_with_prefix,
                "attributes": iot_source  # Add all relevant attributes here
            }
            self.iot_devices.append(iot_device)

        # Process the sensor points and create observations
        for point in points:
            if type(point) is not dict:
                print(type(point))
                continue
            id_ = point.get("stream:id", "unknown_id")
            observation = {
                "observation_id": "obs_" + id_,
                "iot_device_link": iot_device_id_with_prefix,  # Link to the sensor object
                "timestamp": point.get("stream:timestamp", ""),
                "attributes": {}
            }
            # Add all attributes to the observation
            for key in point.keys():
                if key not in observation and key not in observation["attributes"]:
                    observation["attributes"][key] = point[key]

            self.observations.append(observation)

            # Create event-object relationships (link IoT events to their data source)
            self.event_data_source_relationships.append({
                "event_id": observation["observation_id"],
                "data_source_id": iot_device_id_with_prefix  # Link to the sensor object
            })

    def link_iot_events_to_objects(self):
        """
        Link iot events to objects
        """
        for i, iot_event in enumerate(self.iot_events):
            iot_event_id = iot_event["event_id"]
            self.event_object_relationships.append({
                "event_id": iot_event_id,
                "object_id":  self.iot_devices[0]["data_source_id"] if i%2==0 else self.iot_devices[1]["data_source_id"] # Todo: This is for testing only and should be replaced with a proper mapping
            })


def load_yaml(yaml_file: str) -> List[Any]:
    """
    Loads a YAML file, returning all documents as a list.

    :param yaml_file: The path to the YAML file.
    :return: A list containing the loaded YAML documents.
    """
    with open(yaml_file, 'r') as file:
        return list(yaml.safe_load_all(file))


if __name__ == "__main__":
    scheme_1 = load_yaml("file.yaml")
    scheme_2 = load_yaml("file_2.yaml")
    full_scheme = scheme_1 + scheme_2
    parser = SensorStreamParser()
    res: OCELWrapper = parser.parse_sensor_stream_log(full_scheme)
    ocel_pointer: pm4py.OCEL = res.get_ocel()
    res.save_ocel("v3_output.jsonocel")
    print(ocel_pointer.get_summary())
    discovered_df = pm4py.discover_oc_petri_net(ocel_pointer)
    pm4py.view_ocpn(discovered_df)

    # Discover an Object-Centric Petri Net (OC-PN) from the sampled OCEL
    # ocpn = pm4py.discover_oc_petri_net(ocel_pointer)

    # Get a visualization of the OC-PN (Returns a Graphviz digraph)
    # gph = pm4py.visualization.ocel.ocpn.visualizer.apply(ocpn)

    # View the diagram using matplotlib
    # pm4py.visualization.ocel.ocpn.visualizer.matplotlib_view(gph)

    # with open("/Users/christianimenkamp/Downloads/payment.json") as f:
    #     json_ref = json.load(f)
    #     print(json_ref)
