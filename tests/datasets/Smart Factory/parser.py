from typing import Any, Dict, List, Optional
from uuid import uuid4

import pandas as pd
import pm4py
import xmltodict

from src.wrapper.ocel_wrapper import OCELWrapper


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

    def parse_sensor_stream_log(self, sensorstream_log: Dict[str, Any]) -> OCELWrapper:
        """
        Parses a SensorStream log and returns an OCELWrapper object.

        :param sensorstream_log: List of SensorStream events as dictionaries.
        :return: OCELWrapper object containing the parsed data.
        """

        # xml_dict["log"]["trace"](dict_2)["event"](list_5)[0](dict_3)["string"] <- concept:name and org:resource
        # xml_dict["log"]["trace"](dict_2)["event"](list_5)[0](dict_3)["date"] <- timestamp and operation end time
        # xml_dict["log"]["trace"](dict_2)["event"](list_5)[0](dict_3)["list"] <- stream:datastream

        # stream:datastream(list_178)[0](dict_8) <- stream:point

        process_events: List[dict] = sensorstream_log["event"]

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
                attr_system: str = stream_point["@stream:system"]
                attr_system_type: str = stream_point["@stream:system_type"]
                attr_observation: str = stream_point["@stream:observation"]
                attr_procedure_type: str = stream_point["@stream:procedure_type"]
                attr_interaction_type: str = stream_point["@stream:interaction_type"]
                point_time_stamp: str = stream_point["date"]["@stream:timestamp"]
                point_value: str = stream_point["string"]["@stream:value"]

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


FILE_NAME: str = "771e7f75-e1e6-4d0c-856c-d65a003159b4.xes"

if __name__ == "__main__":
    # Load xml as string
    with open(FILE_NAME, 'r') as file:
        xml_string = file.read()

    # Parse xml string to dict
    xml_dict = xmltodict.parse(xml_string)
    # Parse the log
    parser = SensorStreamParser()
    ocel_wrapper = parser.parse_sensor_stream_log(xml_dict["log"]["trace"])

    ocel_pointer: pm4py.OCEL = ocel_wrapper.get_ocel()
    ocel_wrapper.save_ocel("v1_output.jsonocel")
    print(ocel_pointer.get_summary())
    discovered_df = pm4py.discover_oc_petri_net(ocel_pointer)
    pm4py.view_ocpn(discovered_df)

