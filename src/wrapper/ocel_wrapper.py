import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Literal, Self

import pandas as pd
import pm4py
import pylab as p
from pm4py import OCEL
from pm4py.util import constants

from src.validation.base import JsonValidator

ATTRIBUTE_KEY_PREFIX = "ocel:attr:"


def get_event_by_id(events: pd.DataFrame, event_id: str, ocel_string: str) -> Dict[str, Any]:
    """
    Retrieves an event by its ID from a list of events.

    :param events: Dataframe of events.
    :param event_id: ID of the event to retrieve.
    :return: The event with the specified ID.
    """
    return events.loc[events[ocel_string] == event_id].to_dict()


class OCELWrapper:
    def __init__(self,
                 objects: Optional[List[Dict[str, Any]]] = None,
                 iot_events: Optional[List[Dict[str, Any]]] = None,
                 process_events: Optional[List[Dict[str, Any]]] = None,
                 iot_devices: Optional[List[Dict[str, Any]]] = None,
                 observations: Optional[List[Dict[str, Any]]] = None,
                 information_systems: Optional[List[Dict[str, Any]]] = None,
                 object_object_relationships: Optional[List[Dict[str, Any]]] = None,
                 event_object_relationships: Optional[List[Dict[str, Any]]] = None,
                 event_event_relationships: Optional[List[Dict[str, Any]]] = None,
                 event_data_source_relationships: Optional[List[Dict[str, Any]]] = None
                 ) -> None:
        """
        Initialize the OCELWrapper with lists of dicts representing different aspects of the OCEL model.
        """
        self.ocel = OCEL()

        self.JSON_SCHEMA_PATH = "../../schemas/json_schema.json"
        self.objects: List[Dict[str, Any]] = objects if objects is not None else []
        self.iot_events: List[Dict[str, Any]] = iot_events if iot_events is not None else []
        self.process_events: List[Dict[str, Any]] = process_events if process_events is not None else []
        self.iot_devices: List[Dict[str, Any]] = iot_devices if iot_devices is not None else []
        self.observations: List[Dict[str, Any]] = observations if observations is not None else []
        self.information_systems: List[Dict[str, Any]] = information_systems if information_systems is not None else []
        self.object_object_relationships: List[
            Dict[str, Any]] = object_object_relationships if object_object_relationships is not None else []
        self.event_object_relationships: List[
            Dict[str, Any]] = event_object_relationships if event_object_relationships is not None else []
        self.event_event_relationships: List[
            Dict[str, Any]] = event_event_relationships if event_event_relationships is not None else []
        self.event_data_source_relationships: List[
            Dict[str, Any]] = event_data_source_relationships if event_data_source_relationships is not None else []

        # Process the data
        self._process_data()

    def _process_data(self) -> None:
        """
        Processes the data by adding objects, events, and relationships to the OCEL.
        """

        self._add_objects(self.objects, "generic")
        self._add_objects(self._format_iot_devices(self.iot_devices), "iot_device")
        self._add_objects(self._format_information_systems(self.information_systems), "information_system")
        self._add_events(self._format_observations(self.observations), "observation")
        self._add_events(self.iot_events, "iot_event")
        self._add_events(self.process_events, "process_event")
        self._add_object_relationships(self.object_object_relationships)
        self._add_event_object_relationships(self.event_object_relationships)
        self._add_event_event_relationships(self.event_event_relationships)
        self._add_event_data_source_relationships(self.event_data_source_relationships)

        t = [list(
            self.ocel.objects.loc[self.ocel.objects["ocel:oid"] == x["object_id"]].to_dict()["ocel:type"].values())[0]
             for x in self.event_object_relationships]

        e_to_o = lambda x: list(
            self.ocel.events.loc[self.ocel.events["ocel:eid"] == x["event_id"]]
            .to_dict()["ocel:activity"]
            .values()
        )

        l = [
            e_to_o(x)[0] if len(e_to_o(x)) > 0 else "undefined"
            for x in self.event_object_relationships
        ]

        relationships = {
            self.ocel.event_id_column: [x["event_id"] for x in self.event_object_relationships],
            self.ocel.object_id_column: [x["object_id"] for x in self.event_object_relationships],
            self.ocel.object_type_column: t,
            self.ocel.event_activity: l,
            "ocel:qualifier": ["related"] * len(self.event_object_relationships)
        }

        self.ocel.relations = pd.DataFrame(relationships)

    def _add_objects(self, objects: List[Dict[str, Any]],
                     object_class: Literal["iot_device", "information_system", "generic"]) -> None:
        """
        Adds objects to the OCEL.

        :param objects: List of dictionaries representing objects.
        """
        new_rows = []
        for obj in objects:
            obj_id = obj["object_id"]
            obj_type = obj["object_type"]
            attributes = obj.get("attributes", {})

            new_row = {self.ocel.object_id_column: obj_id, self.ocel.object_type_column: obj_type}
            for key, value in attributes.items():
                new_row[ATTRIBUTE_KEY_PREFIX + key] = value

            # only add object if it does not exist yet
            if not any(x[self.ocel.object_id_column] == obj_id for x in new_rows):
                new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.objects = pd.concat([self.ocel.objects, new_df], ignore_index=True)

    def _add_events(self, events: List[Dict[str, Any]],
                    event_type: Literal["iot_event", "process_event", "observation"]) -> None:
        """
        Adds events to the OCEL.

        :param events: List of dictionaries representing events.
        :param event_type: Type of the event.
        """
        new_rows = []
        for event in events:
            event_id = event["event_id"]
            timestamp = pd.to_datetime(event["timestamp"])
            attributes = event.get("attributes", {})
            activity = event.get("activity", {}).get("activity_type", "")

            # For process events: Set event type to the activity label (as in OCEL).
            # For bottom-level IoT events: Set event type to “observation.”
            # For intermediary IoT events: Use a meaningful label.
            event_sub_type_label: str = ""
            if event_type == "process_event":
                event_sub_type_label = activity
            elif event_type == "iot_event":
                event_sub_type_label = event["event_type"]
            elif event_type == "observation":
                event_sub_type_label = "observed"
            else:
                event_sub_type_label = "NO EVENT TYPE"

            new_row = {
                self.ocel.event_id_column: event_id,
                self.ocel.event_activity: event_sub_type_label,
                self.ocel.event_timestamp: timestamp,
                "ocel:event_type": event_sub_type_label,
                "ocel:event_class": event_type
            }

            for key, value in attributes.items():
                new_row[ATTRIBUTE_KEY_PREFIX + key] = value

            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.events = pd.concat([self.ocel.events, new_df], ignore_index=True)

    def _add_object_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Adds object-object relationships to the OCEL.

        :param relationships: List of dictionaries representing object relationships.
        """
        new_rows = []
        for relationship in relationships:
            obj1_id = relationship["object_id"]
            obj2_id = relationship["related_object_id"]

            new_row = {
                self.ocel.object_id_column: obj1_id,
                self.ocel.object_id_column + "_2": obj2_id,
                self.ocel.qualifier: "related"  # Assuming a generic qualifier
            }
            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.o2o = pd.concat([self.ocel.o2o, new_df], ignore_index=True)

    def _add_event_object_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Adds event-object relationships to the OCEL.

        :param relationships: List of dictionaries representing event-object relationships.
        """
        new_rows = []
        for relationship in relationships:
            obj_id = relationship["object_id"]
            event_id = relationship["event_id"]

            # Find the object
            obj = next((x for x in self.objects if x["object_id"] == obj_id), None)
            if obj is None:
                continue

            obj_type = obj.get("object_type", "undefined")

            # Find the event
            all_events = self.iot_events + self.process_events
            evt = next((x for x in all_events if x["event_id"] == event_id), None)
            if evt is None:
                continue

            event_activity = evt.get("activity", {}).get("activity_type", "")
            event_timestamp = pd.to_datetime(evt["timestamp"])

            new_row = {
                self.ocel.event_id_column: event_id,
                self.ocel.event_activity: event_activity,
                self.ocel.event_timestamp: event_timestamp,
                self.ocel.object_type_column: obj_type,
                self.ocel.object_id_column: obj_id
            }
            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.relations = pd.concat([self.ocel.relations, new_df], ignore_index=True)

    def _add_event_event_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Adds event-event relationships to the OCEL.

        :param relationships: List of dictionaries representing event-event relationships.
        """
        new_rows = []
        for relationship in relationships:
            event1_id = relationship["event_id"]
            event2_id = relationship["derived_from_event_id"]

            new_row = {
                self.ocel.event_id_column: event1_id,
                self.ocel.event_id_column + "_2": event2_id,
                self.ocel.qualifier: "derived_from"  # Assuming a generic qualifier
            }
            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            self.ocel.e2e = pd.concat([self.ocel.e2e, new_df], ignore_index=True)

    def _add_event_data_source_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Adds event-data source relationships to the OCEL.

        :param relationships: List of dictionaries representing event-data source relationships.
        """
        new_rows = []
        for relationship in relationships:
            data_source_id = relationship["data_source_id"]
            event_id = relationship["event_id"]

            # Find the data source
            data_sources = self.information_systems + self.iot_devices
            ds = next((x for x in data_sources if x.get("data_source_id") == data_source_id), None)
            if ds is None:
                continue

            obj_id = ds["data_source_id"]
            type_test = self.ocel.objects.loc[self.ocel.objects["ocel:oid"] == obj_id].to_dict()["ocel:type"]

            type_ = list(type_test.values())[0]

            obj_type = "information_system" if "name" in ds else type_

            # Find the event
            all_events = self.iot_events + self.process_events
            evt = next((x for x in all_events if x["event_id"] == event_id), None)
            if evt is None:
                continue

            event_activity = evt.get("activity", {}).get("activity_type", "")
            event_timestamp = pd.to_datetime(evt["timestamp"])

            new_row = {
                self.ocel.event_id_column: event_id,
                self.ocel.event_activity: event_activity,
                self.ocel.event_timestamp: event_timestamp,
                self.ocel.object_type_column: obj_type,
                self.ocel.object_id_column: obj_id
            }
            new_rows.append(new_row)

        if new_rows:
            new_df = pd.DataFrame(new_rows)
            if self.ocel.relations is None:
                self.ocel.relations = new_df
            else:
                self.ocel.relations = pd.concat([self.ocel.relations, new_df], ignore_index=True)

    def _format_iot_devices(self, iot_devices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Formats IoT devices as objects for the OCEL model.

        :param iot_devices: List of dictionaries representing IoT devices.
        :return: List of formatted dictionaries representing IoT devices as objects.
        """
        formatted_devices = []
        for device in iot_devices:
            # generate a random string
            device_type: str = "iot_device_" + str(p.randint(0, 1000000))

            formatted_devices.append({
                "object_id": device["data_source_id"],
                "object_type": device_type,
                "attributes": {}
            })

            # Add device attributes excluding data_source_id
            for key, value in device.items():
                if key != "data_source_id":
                    formatted_devices[-1]["attributes"][key] = value

        return formatted_devices

    def _format_observations(self, observations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Formats observations as events for the OCEL model.

        :param observations: List of dictionaries representing observations.
        :return: List of formatted dictionaries representing observations as events.
        """
        formatted_events = []
        for observation in observations:
            observation_ref = {
                "event_id": observation["observation_id"],
                "event_type": "observation",
                "timestamp": observation.get("timestamp", ""),
                "attributes": {}
            }

            # Add observation attributes excluding iot_device_id
            for key, value in observation.get("attributes", {}).items():
                observation_ref["attributes"][key] = value

            formatted_events.append(observation_ref)

            # Add relationship to link IoT event to the IoT device
            # self.event_data_source_relationships.append({
            #     "event_id": observation["observation_id"],
            #     "data_source_id": observation["iot_device_id"]
            # })

        return formatted_events

    def _format_information_systems(self, information_systems: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Formats information systems as objects for the OCEL model.

        :param information_systems: List of dictionaries representing information systems.
        :return: List of formatted dictionaries representing information systems as objects.
        """
        formatted_systems = []
        for system in information_systems:
            formatted_systems.append({
                "object_id": system["data_source_id"],
                "object_type": "information_system",
                "attributes": {
                    "system_name": system["name"]
                }
            })
        return formatted_systems

    def get_ocel(self) -> OCEL:
        """
        Returns the OCEL object.

        :return: OCEL object.
        """
        return self.ocel

    def get_extended_table(self):
        """
        Transforms the current OCEL data structure into a Pandas dataframe containing the events with their
        attributes and the related objects per object type.
        """
        return self.ocel.get_extended_table()

    def save_ocel(self, path: str) -> None:
        """
        Saves the OCEL object to a file.

        :param path: Path to the file.
        """
        pm4py.write_ocel(self.ocel, path)
