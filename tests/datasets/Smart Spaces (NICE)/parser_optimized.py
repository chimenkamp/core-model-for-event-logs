import xml.etree.ElementTree as ET
from uuid import uuid4
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, wait
import pm4py
import yaml
from typing import Union, Dict, List, Any, Literal
import xmltodict

from src.types_defintion.event_definition import Event, ProcessEvent, Observation, IotEvent
from src.types_defintion.object_definition import Object, ObjectClassEnum
from src.types_defintion.relationship_definitions import ObjectObjectRelationship, EventObjectRelationship, \
    EventEventRelationship
from src.wrapper.ocel_wrapper import COREMetamodel


def get_object_class_for_object_type(o_type: Literal["location", "date", "user"]) -> ObjectClassEnum:
    if o_type == "location":
        return ObjectClassEnum.BUSINESS_OBJECT
    elif o_type == "date":
        return ObjectClassEnum.BUSINESS_OBJECT
    elif o_type == "user":
        return ObjectClassEnum.RESOURCE


class SensorStreamParser:
    def __init__(self) -> None:
        """
        Initializes the SensorStreamParser class.
        """
        self.objects: List[Object] = []
        self.iot_events: List[Event] = []
        self.process_events: List[Event] = []
        self.observations: List[Event] = []
        self.object_object_relationships: List[ObjectObjectRelationship] = []
        self.event_object_relationships: List[EventObjectRelationship] = []
        self.event_event_relationships: List[EventEventRelationship] = []
        # Add locks for thread safety
        self.objects_lock = threading.Lock()
        self.iot_events_lock = threading.Lock()
        self.process_events_lock = threading.Lock()
        self.observations_lock = threading.Lock()
        self.object_object_relationships_lock = threading.Lock()
        self.event_object_relationships_lock = threading.Lock()
        self.event_event_relationships_lock = threading.Lock()

    def process_objects(self, object_list: List[dict]) -> None:
        """Process objects in a separate thread"""
        print("Processing objects started")
        for obj in object_list:
            dp: dict | list = obj.get("DigitalProperty", {})
            dp_dict = dp if isinstance(dp, dict) else {str(i): n for i, n in enumerate(dp)}

            object_ref: Object = Object(
                object_id=obj["@ID"],
                object_type=obj["@objectType"],
                object_class=get_object_class_for_object_type(obj["@objectType"]),
                attributes=dp_dict
            )

            with self.objects_lock:
                self.objects.append(object_ref)
        print("Processing objects completed")

    def process_sensors(self, sensors: List[dict]) -> None:
        """Process sensors in a separate thread"""
        print("Processing sensors started")
        for sensor in sensors:
            object_ref: Object = Object(
                object_id=sensor["@ID"],
                object_type="Sensor",
                object_class=ObjectClassEnum.SENSOR,
                attributes={
                    "location": sensor.get("@location", None),
                    "metadata": sensor.get("metadata", {}),
                },
            )

            with self.objects_lock:
                self.objects.append(object_ref)

            sensor_id: str = sensor["@ID"]

            if "@location" in sensor:
                object_id: str = sensor["@location"]
                o2o_ref: ObjectObjectRelationship = ObjectObjectRelationship(
                    object_id=sensor_id,
                    related_object_id=object_id,
                    qualifier="located_at",
                )
                with self.object_object_relationships_lock:
                    self.object_object_relationships.append(o2o_ref)
        print("Processing sensors completed")

    def process_iot_events(self, iot_events: List[dict]) -> None:
        print("Processing IoT events started")
        """Process IoT events in a separate thread"""
        for event in iot_events:
            iot_event_ref: Event = IotEvent(
                event_id=event["@ID"],
                event_type=f"FeatureOfInterest",
                timestamp=event["@timestamp"],
                attributes={
                    "feature_of_interest": event.get("FeatureOfInterest", None),
                }
            )

            with self.iot_events_lock:
                self.iot_events.append(iot_event_ref)

            e_o_relationship: List[Dict[str, str]] = event.get("EventObjectRelationship", [])
            for rel in e_o_relationship:
                e2o_ref: EventObjectRelationship = EventObjectRelationship(
                    event_id=event["@ID"],
                    object_id=rel["@objectID"],
                )
                with self.event_object_relationships_lock:
                    self.event_object_relationships.append(e2o_ref)

            if "Observation" in event:
                observation: dict = event["Observation"]
                observation_ref: Event = Observation(
                    event_id=str(uuid4()),
                    event_type=f"Observation+{str(uuid4())[:8]}",
                    timestamp=event["@timestamp"],
                    attributes={
                        "value": observation.get("@value", None),
                        "sensor": observation.get("@sensor", None),
                    }
                )

                with self.observations_lock:
                    self.observations.append(observation_ref)

                e2e_ref: EventEventRelationship = EventEventRelationship(
                    event_id=iot_event_ref.event_id,
                    derived_from_event_id=observation_ref.event_id,
                    qualifier="observe_by",
                )
                with self.event_event_relationships_lock:
                    self.event_event_relationships.append(e2e_ref)

                if observation.get("@sensor", None):
                    e2o_ref: EventObjectRelationship = EventObjectRelationship(
                        event_id=iot_event_ref.event_id,
                        object_id=observation["@sensor"],
                        qualifier="observe_by",
                    )
                    with self.event_object_relationships_lock:
                        self.event_object_relationships.append(e2o_ref)
        print("Processing IoT events completed")

    def process_process_events(self, process_events: List[dict]) -> None:
        """Process process events in a separate thread"""
        print("Processing process events started")
        for event in process_events:
            process_event_ref: Event = ProcessEvent(
                event_id=event["@ID"],
                event_type=f"ProcessEvent+{str(uuid4())[:8]}",
                timestamp=event["@timestamp"],
                activity=event["@label"],
                attributes={
                    "method": event.get("Method", {}),
                    "analytics": event.get("Analytics", {}),
                    "value": event.get("@value", None),
                },
            )

            with self.process_events_lock:
                self.process_events.append(process_event_ref)

            e_o_relationship: List[Dict[str, str]] = event.get("EventObjectRelationship", [])
            for rel in e_o_relationship:
                e2o_ref: EventObjectRelationship = EventObjectRelationship(
                    event_id=event["@ID"],
                    object_id=rel["@objectID"],
                )
                with self.event_object_relationships_lock:
                    self.event_object_relationships.append(e2o_ref)

            if "Analytics" in event:
                analytics: List[str] = event["Analytics"]["AnalysesEvent"]
                for id_ref in analytics:
                    with self.event_event_relationships_lock:
                        self.event_event_relationships.append(
                            EventEventRelationship(
                                event_id=process_event_ref.event_id,
                                derived_from_event_id=id_ref,
                                qualifier="analyzed_by",
                            )
                        )
        print("Processing process events completed")

    def process_context_events(self, context_events: List[dict]) -> None:
        """Process context events in a separate thread"""
        print("Processing context events started")
        for event in context_events:
            event_id: str = event["@ID"]
            event_type: str = f"ContextEvent+{str(uuid4())[:8]}"
            event_timestamp: str = event["@timestamp"]

            context_event_ref: Event = Observation(
                event_id=event_id,
                event_type=event_type,
                timestamp=event_timestamp,
                attributes={
                    "value": event.get("@value", None),
                }
            )
            with self.observations_lock:
                self.observations.append(context_event_ref)

            e_o_relationship: List[Dict[str, str]] = event.get("EventObjectRelationship", [])
            for rel in e_o_relationship:
                e2o_ref: EventObjectRelationship = EventObjectRelationship(
                    event_id=event_id,
                    object_id=rel["@objectID"],
                )
                with self.event_object_relationships_lock:
                    self.event_object_relationships.append(e2o_ref)
        print("Processing context events completed")

    def parse_sensor_stream_log(self, sensorstream_log: dict) -> COREMetamodel:
        """
        Parses a SensorStream log and returns an OCELWrapper object.

        :param sensorstream_log: List of SensorStream events as dictionaries.
        :return: OCELWrapper object containing the parsed data.
        """
        event_log: dict = sensorstream_log["EventLog"]

        # Get all the data that needs to be processed
        object_list: list[dict] = event_log["ObjectsList"]["FeatureOfInterest"]
        sensors: list[dict] = event_log["DataSourcesList"]["Sensor"]
        iot_events = event_log["EventsList"]["IoTEvent"]
        process_events = event_log["EventsList"]["ProcessEvent"]
        context_events = event_log["EventsList"]["ContextEvent"]

        # Create a ThreadPoolExecutor to manage the threads
        with ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all tasks to the executor
            futures = [
                executor.submit(self.process_objects, object_list),
                executor.submit(self.process_sensors, sensors),
                executor.submit(self.process_iot_events, iot_events),
                executor.submit(self.process_process_events, process_events),
                executor.submit(self.process_context_events, context_events)
            ]

            # Wait for all tasks to complete
            wait(futures)

        # print stats
        print("Objects:", len(self.objects))
        print("Iot Events:", len(self.iot_events))
        print("Process Events:", len(self.process_events))
        print("Observations:", len(self.observations))
        print("Object Object Relationships:", len(self.object_object_relationships))
        print("Event Object Relationships:", len(self.event_object_relationships))
        print("Event Event Relationships:", len(self.event_event_relationships))

        return COREMetamodel(
            objects=self.objects,
            iot_events=self.iot_events,
            process_events=self.process_events,
            observations=self.observations,
            object_object_relationships=self.object_object_relationships,
            event_object_relationships=self.event_object_relationships,
            event_event_relationships=self.event_event_relationships,
        )


if __name__ == "__main__":
    sample_xml_path = "smart spaces nice log.xml"

    # Load xml as string
    with open(sample_xml_path, 'r') as file:
        xml_string = file.read()
    print("After reading the file")

    # Parse xml string to dict
    xml_dict = xmltodict.parse(xml_string)

    # Parse xml dict to OCELWrapper
    parser = SensorStreamParser()
    ocel_wrapper = parser.parse_sensor_stream_log(xml_dict)
    print("After parsing the xml")

    ocel_pointer: pm4py.OCEL = ocel_wrapper.get_ocel()
    ocel_wrapper.save_ocel("v1_output.jsonocel")
    print(ocel_pointer.get_summary())
    discovered_df = pm4py.discover_oc_petri_net(ocel_pointer)
    pm4py.view_ocpn(discovered_df)