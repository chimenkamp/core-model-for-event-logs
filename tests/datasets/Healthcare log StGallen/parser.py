import xml.etree.ElementTree as ET
from uuid import uuid4

import pm4py
import yaml
from typing import Union, Dict, List, Any
import xmltodict

from src.wrapper.ocel_wrapper import OCELWrapper


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

    def parse_sensor_stream_log(self, sensorstream_log: dict) -> OCELWrapper:
        """
        Parses a SensorStream log and returns an OCELWrapper object.

        :param sensorstream_log: List of SensorStream events as dictionaries.
        :return: OCELWrapper object containing the parsed data.
        """

        for trace in xml_dict['log']['trace']:
            concept_name: str = trace['string']['@value']
            stream_points: List[dict] = trace["list"]["list"]["list"]

            self.objects.append({
                "object_id": concept_name,
                "object_type": "case_object",
                "attributes": {
                    "concept:name": concept_name,
                    "lifecycle:start": stream_points[0]["date"]["@value"],
                    "lifecycle:end": stream_points[-1]["date"]["@value"]
                }
            })

            for elem in stream_points:
                # StreamPoint
                timestamp = elem["date"]
                event: List[dict] = elem["string"]

                point_value: dict = {}

                for p in event:
                    key = p["@key"]
                    value = p["@value"]
                    point_value[key] = value

                event_id: str = f"{str(uuid4())[:8]} - {point_value['stream:id']}"

                self.iot_events.append({
                    "event_id": event_id,
                    "event_type": point_value["stream:id"],
                    "timestamp": timestamp["@value"],
                    "attributes": {
                        "stream:source": point_value["stream:source"],
                        "stream:value": point_value["stream:value"],
                    }
                })

                self.event_object_relationships.append({
                    "event_id": event_id,
                    "object_id": concept_name
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


if __name__ == "__main__":
    sample_xml_path = "full_log.txt"

    # Load xml as string
    with open(sample_xml_path, 'r') as file:
        xml_string = file.read()

    # Parse xml string to dict
    xml_dict = xmltodict.parse(xml_string)

    # Parse xml dict to OCELWrapper
    parser = SensorStreamParser()
    ocel_wrapper = parser.parse_sensor_stream_log(xml_dict)

    ocel_pointer: pm4py.OCEL = ocel_wrapper.get_ocel()
    ocel_wrapper.save_ocel("v1_output.jsonocel")
    print(ocel_pointer.get_summary())
    discovered_df = pm4py.discover_oc_petri_net(ocel_pointer)
    pm4py.view_ocpn(discovered_df)
