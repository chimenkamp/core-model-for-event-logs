from datetime import datetime
from typing import List, Dict, Any, Optional, Literal

import pandas as pd
from pm4py import OCEL


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

         :param objects: List of dictionaries representing objects.
         :param iot_events: List of dictionaries representing IoT events.
         :param process_events: List of dictionaries representing process events.
         :param iot_devices: List of dictionaries representing IoT devices.
         :param observations: List of dictionaries representing observations.
         :param information_systems: List of dictionaries representing information systems.

         :param object_object_relationships: List of dictionaries representing object relationships.
         :param event_object_relationships: List of dictionaries representing event-object relationships.
         :param event_event_relationships: List of dictionaries representing event-event relationships.
         :param event_data_source_relationships: List of dictionaries representing event-data source relationships.
         """

        self.ocel = OCEL()

        if objects:
            self._add_objects(objects)

        if iot_devices:
            self._add_objects(self._format_iot_devices(iot_devices))

        if observations:
            self._add_events(self._format_observations(observations), "")

        if information_systems:
            self._add_objects(self._format_information_systems(information_systems))

        if iot_events:
            self._add_events(iot_events, "iot_event")

        if process_events:
            self._add_events(process_events, "process_event")

        if object_object_relationships:
            self._add_object_relationships(object_object_relationships)

        if event_object_relationships:
            self._add_event_object_relationships(event_object_relationships)

        if event_event_relationships:
            self._add_event_event_relationships(event_event_relationships)

        if event_data_source_relationships:
            self._add_event_data_source_relationships(event_data_source_relationships)

    def _add_objects(self, objects: List[Dict[str, Any]]) -> None:
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
                new_row["ocel:attr" + key] = value

            new_rows.append(new_row)

        new_df = pd.DataFrame(new_rows)
        self.ocel.objects = pd.concat([self.ocel.objects, new_df], ignore_index=True)

    def _add_events(self, events: List[Dict[str, Any]], event_type: Literal["iot_event", "process_event", "observation"]) -> None:
        """
        Adds events to the OCEL.

        :param events: List of dictionaries representing events.
        """
        new_rows = []
        for event in events:
            event_id = event["event_id"]
            timestamp = event["timestamp"]
            attributes = event.get("attributes", {})
            activity = event.get("activity", {}).get("activity_type", "")

            new_row = {
                self.ocel.event_id_column: event_id,
                self.ocel.event_activity: activity,
                self.ocel.event_timestamp: timestamp,
                "ocel:event_type": event_type
            }
            for key, value in attributes.items():
                new_row["ocel:attr:"+key] = value

            new_rows.append(new_row)

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

        new_df = pd.DataFrame(new_rows)
        self.ocel.o2o = pd.concat([self.ocel.o2o, new_df], ignore_index=True)

    def _add_event_object_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Adds event-object relationships to the OCEL.

        :param relationships: List of dictionaries representing event-object relationships.
        """
        new_rows = []
        for relationship in relationships:
            event_id = relationship["event_id"]
            obj_id = relationship["object_id"]

            new_row = {
                self.ocel.event_id_column: event_id,
                self.ocel.object_id_column: obj_id
            }
            new_rows.append(new_row)

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

        new_df = pd.DataFrame(new_rows)
        self.ocel.e2e = pd.concat([self.ocel.e2e, new_df], ignore_index=True)

    def _add_event_data_source_relationships(self, relationships: List[Dict[str, Any]]) -> None:
        """
        Adds event-data source relationships to the OCEL.

        :param relationships: List of dictionaries representing event-data source relationships.
        """
        new_rows = []
        for relationship in relationships:
            event_id = relationship["event_id"]
            data_source_id = relationship["data_source_id"]

            new_row = {
                self.ocel.event_id_column: event_id,
                "ocel:data_source": data_source_id
            }
            new_rows.append(new_row)

        new_df = pd.DataFrame(new_rows)
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
                "object_id": device["device_id"],
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
            formatted_events.append({
                "event_id": observation["observation_id"],
                "timestamp": observation.get("timestamp", ""),  # Placeholder, assuming real timestamp should be used
                "attributes": {
                    "iot_device_id": observation["iot_device_id"]
                }
            })
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
                "object_id": system["is_id"],
                "object_type": "information_system",
                "attributes": {
                    "system_name": system["system_name"]
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