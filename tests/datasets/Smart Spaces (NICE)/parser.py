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
        event_log: dict = sensorstream_log["EventLog"]

        object_list: list[dict] = event_log["ObjectsList"]["FeatureOfInterest"]
        sensors: list[dict] = event_log["DataSourcesList"]["Sensor"]

        iot_events = event_log["EventsList"]["IoTEvent"]
        process_events = event_log["EventsList"]["ProcessEvent"]
        context_events = event_log["EventsList"]["ContextEvent"]

        for obj in object_list:

            self.objects.append({
                "object_id": obj["@ID"],
                "object_type": obj["@objectType"],
                # "attributes": obj.get("DigitalProperty", {}),
            })
            ob_prop: dict | None = obj.get("ObservableProperty", None)

            if ob_prop:
                # If the object has an observable property link obj to sensor
                # Create Observation
                ...

        for sensor in sensors:
            self.iot_devices.append({
                "data_source_id": sensor["@ID"],
                "device_type": "Sensor",
            })

            # Link iot device to object
            sensor_id: str = sensor["@ID"]

            if "@location" in sensor:
                object_id: str = sensor["@location"]

                self.object_object_relationships.append({
                    "object_id": sensor_id,
                    "related_object_id": object_id,
                })

        for event in iot_events:
            self.iot_events.append({
                "event_id": event["@ID"],
                "event_type": f"IoTEvent+{str(uuid4())[:8]}",
                "timestamp": event["@timestamp"],
            })
            # Link iot event to object
            e_o_relationship: List[Dict[str, str]] = event.get("EventObjectRelationship", [])
            for rel in e_o_relationship:
                self.event_object_relationships.append({
                    "event_id": event["@ID"],
                    "object_id": rel["@objectID"],
                })

            # Link iot event to object
            if "FeatureOfInterest" in event:
                linking_object_id: str = str(uuid4())

                self.objects.append({
                    "object_id": linking_object_id,
                    "object_type": "FeatureOfInterest",
                })

                self.event_object_relationships.append({
                    "event_id": event["@ID"],
                    "object_id": linking_object_id,
                })

                self.object_object_relationships.append({
                    "object_id": linking_object_id,
                    "related_object_id": event["FeatureOfInterest"],
                })

        for event in process_events:
            self.process_events.append({
                "event_id": event["@ID"],
                "event_type": event["@label"],
                "timestamp": event["@timestamp"],
                "attributes": event.get("Method", {}),
                "activity": {
                    "activity_type": event["@label"],
                }
            })
            # Link process event to object
            e_o_relationship: List[Dict[str, str]] = event.get("EventObjectRelationship", [])
            for rel in e_o_relationship:
                self.event_object_relationships.append({
                    "event_id": event["@ID"],
                    "object_id": rel["@objectID"],
                })

            # Create Analysis Object
            analysis_object_id: str = str(uuid4())
            self.objects.append({
                "object_id": analysis_object_id,
                "object_type": "Analysis",
            })

            self.event_object_relationships.append({
                "event_id": event["@ID"],
                "object_id": analysis_object_id,
            })

            analysis_event_ids: List[str] = event.get("Analytics", [])
            for analysis_event_id in analysis_event_ids:
                self.event_object_relationships.append({
                    "event_id": analysis_event_id,
                    "object_id": analysis_object_id,
                })

        for event in context_events:
            event_id: str = event["@ID"]
            event_type: str = "ContextEvent"
            event_timestamp: str = event["@timestamp"]

            self.iot_events.append({
                "event_id": event_id,
                "event_type": event_type,
                "timestamp": event_timestamp,
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
    sample_xml_path = "smart spaces nice log.xml"

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
