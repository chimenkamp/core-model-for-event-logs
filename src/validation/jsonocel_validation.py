from typing import Dict, Any, Tuple, List

from src.validation.base import BaseValidator


class JsonOCEL(BaseValidator):
    def __init__(self, jsonocel: Dict[str, Any]) -> None:
        """
        Initialize the OCELValidator with the JSON OCEL data.

        :param jsonocel: JSON OCEL data as a dictionary.
        """
        self.jsonocel = jsonocel

    def validate(self) -> Tuple[bool, List[str]]:
        """
        Validate the JSON OCEL data against the rule set.

        :return: Tuple of True if the JSON OCEL data is valid, otherwise False, and a list of error messages.
        """
        errors = []
        if not self._validate_object_types(errors):
            errors.append("Object types validation failed.")
        if not self._validate_event_types(errors):
            errors.append("Event types validation failed.")
        if not self._validate_objects(errors):
            errors.append("Objects validation failed.")
        if not self._validate_events(errors):
            errors.append("Events validation failed.")
        if not self._validate_relationships(errors):
            errors.append("Relationships validation failed.")

        return len(errors) == 0, errors

    def _validate_object_types(self, errors: List[str]) -> bool:
        """
        Validate the object types in the JSON OCEL data.

        :param errors: List to collect error messages.
        :return: True if the object types are valid, otherwise False.
        """
        object_types = {obj_type["name"] for obj_type in self.jsonocel.get("objectTypes", [])}
        if not object_types:
            errors.append("No object types found.")
            return False
        return True

    def _validate_event_types(self, errors: List[str]) -> bool:
        """
        Validate the event types in the JSON OCEL data.

        :param errors: List to collect error messages.
        :return: True if the event types are valid, otherwise False.
        """
        event_types = {event_type["name"] for event_type in self.jsonocel.get("eventTypes", [])}
        if not event_types:
            errors.append("No event types found.")
            return False
        return True

    def _validate_objects(self, errors: List[str]) -> bool:
        """
        Validate the objects in the JSON OCEL data.

        :param errors: List to collect error messages.
        :return: True if the objects are valid, otherwise False.
        """
        objects = self.jsonocel.get("objects", [])
        valid = True
        for obj in objects:
            if "id" not in obj:
                errors.append("Object missing 'id'.")
                valid = False
            if "type" not in obj:
                errors.append(f"Object {obj.get('id', 'unknown')} missing 'type'.")
                valid = False
            if "relationships" in obj:
                for rel in obj["relationships"]:
                    if "objectId" not in rel:
                        errors.append(f"Object {obj['id']} relationship missing 'objectId'.")
                        valid = False
                    if "qualifier" not in rel:
                        errors.append(f"Object {obj['id']} relationship missing 'qualifier'.")
                        valid = False
        return valid

    def _validate_events(self, errors: List[str]) -> bool:
        """
        Validate the events in the JSON OCEL data.

        :param errors: List to collect error messages.
        :return: True if the events are valid, otherwise False.
        """
        events = self.jsonocel.get("events", [])
        valid = True
        for event in events:
            if "id" not in event:
                errors.append("Event missing 'id'.")
                valid = False
            if "type" not in event:
                errors.append(f"Event {event.get('id', 'unknown')} missing 'type'.")
                valid = False
            if "time" not in event:
                errors.append(f"Event {event.get('id', 'unknown')} missing 'time'.")
                valid = False
        return valid

    def _validate_relationships(self, errors: List[str]) -> bool:
        """
        Validate the relationships in the JSON OCEL data.

        :param errors: List to collect error messages.
        :return: True if the relationships are valid, otherwise False.
        """
        valid = True
        object_ids = {obj["id"] for obj in self.jsonocel.get("objects", [])}
        event_ids = {event["id"] for event in self.jsonocel.get("events", [])}

        # Validate object-object relationships (o2o)
        for obj in self.jsonocel.get("objects", []):
            if "relationships" in obj:
                for rel in obj["relationships"]:
                    if rel["objectId"] not in object_ids:
                        errors.append(
                            f"Object {obj['id']} has relationship with non-existing object {rel['objectId']}.")
                        valid = False

        # Validate event-event relationships (e2e)
        for event in self.jsonocel.get("events", []):
            if "derivedFrom" in event:
                if event["derivedFrom"] not in event_ids:
                    errors.append(f"Event {event['id']} is derived from non-existing event {event['derivedFrom']}.")
                    valid = False
            if "recordedIn" in event:
                if event["recordedIn"] not in object_ids:
                    errors.append(f"Event {event['id']} is recorded in non-existing object {event['recordedIn']}.")
                    valid = False

        return valid