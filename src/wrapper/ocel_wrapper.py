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

        # self._add_objects(self.objects)
        self._add_objects(self._format_iot_devices(self.iot_devices), "iot_device")
        self._add_objects(self._format_information_systems(self.information_systems), "information_system")
        self._add_events(self._format_observations(self.observations), "observation")
        self._add_events(self.iot_events, "iot_event")
        self._add_events(self.process_events, "process_event")
        self._add_object_relationships(self.object_object_relationships)
        self._add_event_object_relationships(self.event_object_relationships)
        self._add_event_event_relationships(self.event_event_relationships)
        self._add_event_data_source_relationships(self.event_data_source_relationships)

        relationships = {
            self.ocel.event_id_column: [x["event_id"] for x in self.event_object_relationships],
            self.ocel.object_id_column: [x["object_id"] for x in self.event_object_relationships],
            self.ocel.object_type_column: ["iot_device" for _ in self.event_object_relationships],
            self.ocel.event_activity: [
                get_event_by_id(self.ocel.events, e.get("event_id", ""), self.ocel.event_activity)
                [self.ocel.event_activity]
                for e in self.event_object_relationships
            ],
        }

        self.ocel.relations = pd.DataFrame(relationships)

    def load_from_json_schema(self, json_file_path: str) -> Self:
        """
        Loads data from a JSON file, validates it against the schema, and assigns it to the wrapper's attributes.

        :param json_file_path: Path to the JSON file containing the data.
        :param schema_path: Path to the JSON schema file for validation.
        """
        validator = JsonValidator(self.JSON_SCHEMA_PATH)

        if not validator.validate(json_file_path):
            raise ValueError("JSON file does not conform to schema")

        with open(json_file_path, 'r') as f:
            data = json.load(f)

        self.objects = data.get('objects', [])
        self.iot_events = data.get('iot_events', [])
        self.process_events = data.get('process_events', [])
        self.iot_devices = data.get('iot_devices', [])
        self.observations = data.get('observations', [])
        self.information_systems = data.get('information_systems', [])
        self.object_object_relationships = data.get('object_object_relationships', [])
        self.event_object_relationships = data.get('event_object_relationships', [])
        self.event_event_relationships = data.get('event_event_relationships', [])
        self.event_data_source_relationships = data.get('event_data_source_relationships', [])

        self.ocel = OCEL()
        self._process_data()

        return self

    def load_from_ocel_schema(self, ocel_file_path: str) -> Self:
        return Self

    def _add_objects(self, objects: List[Dict[str, Any]],
                     object_class: Literal["iot_device", "information_system"]) -> None:
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
            obj_type = "information_system" if "name" in ds else "iot_device"

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
            formatted_devices.append({
                "object_id": device["data_source_id"],
                "object_type": "iot_device",
                "attributes": {}
            })
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
                "attributes": {}  # Moved IoT device linking to relationships
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

    def save_to_json(self, json_file_path: str, schema_path: Optional[str] = None) -> None:
        """
        Saves the data stored inside the OCEL instance to a JSON file following the defined schema.

        :param json_file_path: Path to the JSON file where data will be saved.
        :param schema_path: Optional path to the JSON schema file for validation.
        """
        data = self._reconstruct_data()

        if schema_path:
            validator = JsonValidator(schema_path)
            is_valid = validator.validate(data)
            if not is_valid:
                raise ValueError("Reconstructed data does not conform to schema")

        # Save data to JSON file
        with open(json_file_path, 'w') as f:
            json.dump(data, f, indent=2, default=str)  # default=str to handle datetime serialization

    def _reconstruct_data(self) -> Dict[str, Any]:
        """
        Reconstructs the data from the OCEL instance into the JSON structure defined by the schema.

        :return: A dictionary containing the reconstructed data.
        """
        data = {
            "objects": [],
            "iot_events": [],
            "process_events": [],
            "iot_devices": [],
            "observations": [],
            "information_systems": [],
            "object_object_relationships": [],
            "event_object_relationships": [],
            "event_event_relationships": [],
            "event_data_source_relationships": []
        }

        # Extract objects
        for _, obj_row in self.ocel.objects.iterrows():
            obj = {
                "object_id": obj_row[self.ocel.object_id_column],
                "object_type": obj_row[self.ocel.object_type_column],
                "attributes": {k.replace(ATTRIBUTE_KEY_PREFIX, ""): v for k, v in obj_row.items() if
                               k.startswith(ATTRIBUTE_KEY_PREFIX)}
            }
            obj_type = obj["object_type"]
            if obj_type == "iot_device":
                data["iot_devices"].append({
                    "data_source_id": obj["object_id"]
                })
            elif obj_type == "information_system":
                data["information_systems"].append({
                    "data_source_id": obj["object_id"],
                    "name": obj["attributes"].get("system_name", "")
                })
            else:
                data["objects"].append(obj)

        # Extract events
        for _, event_row in self.ocel.events.iterrows():
            event = {
                "event_id": event_row[self.ocel.event_id_column],
                "timestamp": event_row[self.ocel.event_timestamp].isoformat(),
                "attributes": {k.replace(ATTRIBUTE_KEY_PREFIX, ""): v for k, v in event_row.items() if
                               k.startswith(ATTRIBUTE_KEY_PREFIX)},
                "activity": {
                    "activity_type": event_row.get("ocel:event_type", "")
                }
            }
            event_class = event_row.get("ocel:event_class", "")
            if event_class == "iot_event":
                data["iot_events"].append(event)
            elif event_class == "process_event":
                data["process_events"].append(event)
            elif event_class == "observation":
                observation = {
                    "observation_id": event["event_id"],
                    "timestamp": event["timestamp"],
                    "iot_device_id": event["attributes"].get("iot_device_id", "")
                }
                data["observations"].append(observation)
            else:
                print(f"Unknown event class: {event_class}")
                pass

        # Extract object-object relationships
        if self.ocel.o2o is not None and not self.ocel.o2o.empty:
            for _, rel_row in self.ocel.o2o.iterrows():
                rel = {
                    "object_id": rel_row[self.ocel.object_id_column],
                    "related_object_id": rel_row[self.ocel.object_id_column + "_2"]
                }
                data["object_object_relationships"].append(rel)

        # Extract event-object relationships
        if self.ocel.relations is not None and not self.ocel.relations.empty:
            for _, rel_row in self.ocel.relations.iterrows():
                rel = {
                    "event_id": rel_row[self.ocel.event_id_column],
                    "object_id": rel_row[self.ocel.object_id_column]
                }
                obj_type = rel_row[self.ocel.object_type_column]
                if obj_type in ["iot_device", "information_system"]:
                    data["event_data_source_relationships"].append({
                        "event_id": rel["event_id"],
                        "data_source_id": rel["object_id"]
                    })
                else:
                    data["event_object_relationships"].append(rel)

        # Extract event-event relationships
        if self.ocel.e2e is not None and not self.ocel.e2e.empty:
            for _, rel_row in self.ocel.e2e.iterrows():
                rel = {
                    "event_id": rel_row[self.ocel.event_id_column],
                    "derived_from_event_id": rel_row[self.ocel.event_id_column + "_2"]
                }
                data["event_event_relationships"].append(rel)

        return data
