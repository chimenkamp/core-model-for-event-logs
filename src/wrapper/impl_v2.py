from enum import Enum
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
import pandas as pd
import numpy as np
from pm4py.objects.ocel import constants
from pm4py.objects.ocel.obj import OCEL as BaseOCEL
from pm4py.util import exec_utils, pandas_utils


class IoTParameters(Enum):
    """Extension of OCEL parameters for IoT specific configurations"""
    EVENT_SUBTYPE = "event_subtype"
    DATA_SOURCE_TYPE = "data_source_type"
    BUSINESS_OBJECT_TYPE = "business_object_type"
    IOT_OBSERVATION = "iot_observation"
    PROCESS_ACTIVITY = "process_activity"


@dataclass
class ObjectTypeDefinition:
    """Definition of an object type and its constraints"""
    name: str
    parent_type: Optional[str] = None
    required_attributes: Set[str] = field(default_factory=set)
    optional_attributes: Set[str] = field(default_factory=set)
    allowed_relationships: Set[str] = field(default_factory=set)


@dataclass
class EventTypeDefinition:
    """Definition of an event type and its constraints"""
    name: str
    subtype: str  # 'IOT' or 'PROCESS'
    required_object_types: Set[str] = field(default_factory=set)
    optional_object_types: Set[str] = field(default_factory=set)
    required_attributes: Set[str] = field(default_factory=set)
    optional_attributes: Set[str] = field(default_factory=set)


class OCELIoT(BaseOCEL):
    """Extended OCEL implementation with IoT support"""

    def __init__(self, events=None, objects=None, relations=None, globals=None, parameters=None,
                 o2o=None, e2e=None, object_changes=None):
        super().__init__(events, objects, relations, globals, parameters, o2o, e2e, object_changes)

        # Initialize IoT-specific metadata structures
        self.object_type_definitions: Dict[str, ObjectTypeDefinition] = {}
        self.event_type_definitions: Dict[str, EventTypeDefinition] = {}

        # Initialize default IoT structure if not provided
        self._initialize_iot_structure()

    def _initialize_iot_structure(self):
        """Initialize the basic IoT structure with default types"""
        # Define base object types
        self.register_object_type(ObjectTypeDefinition(
            name="DataSource",
            required_attributes={"name"},
            optional_attributes={"description"}
        ))

        self.register_object_type(ObjectTypeDefinition(
            name="Sensor",
            parent_type="DataSource",
            required_attributes={"measurement_unit"},
            optional_attributes={"location", "accuracy"}
        ))

        self.register_object_type(ObjectTypeDefinition(
            name="BusinessObject",
            required_attributes={"name"},
            optional_attributes={"description"}
        ))

        # Define base event types
        self.register_event_type(EventTypeDefinition(
            name="IoTObservation",
            subtype="IOT",
            required_object_types={"DataSource"},
            optional_object_types={"BusinessObject"},
            required_attributes={"value"},
            optional_attributes={"quality"}
        ))

        self.register_event_type(EventTypeDefinition(
            name="ProcessEvent",
            subtype="PROCESS",
            required_object_types={"BusinessObject"},
            optional_object_types={"DataSource"},
            required_attributes={"activity_name"},
            optional_attributes={"duration"}
        ))

    def register_object_type(self, definition: ObjectTypeDefinition):
        """Register a new object type definition"""
        # Validate parent type if specified
        if definition.parent_type and definition.parent_type not in self.object_type_definitions:
            raise ValueError(f"Parent type {definition.parent_type} not found")

        self.object_type_definitions[definition.name] = definition

    def register_event_type(self, definition: EventTypeDefinition):
        """Register a new event type definition"""
        # Validate that required object types exist
        for ot in definition.required_object_types:
            if ot not in self.object_type_definitions:
                raise ValueError(f"Required object type {ot} not found")

        self.event_type_definitions[definition.name] = definition

    def add_object(self, object_type: str, object_id: str, attributes: Dict) -> None:
        """Add a new object with validation against type definition"""
        if object_type not in self.object_type_definitions:
            raise ValueError(f"Unknown object type: {object_type}")

        definition = self.object_type_definitions[object_type]

        # Validate required attributes
        missing_attrs = definition.required_attributes - set(attributes.keys())
        if missing_attrs:
            raise ValueError(f"Missing required attributes for {object_type}: {missing_attrs}")

        # Create object entry
        obj_data = {
            self.object_id_column: [object_id],
            self.object_type_column: [object_type]
        }

        # Add attributes as columns
        for attr, value in attributes.items():
            if attr not in definition.required_attributes and attr not in definition.optional_attributes:
                print(f"Unknown attribute {attr} for type {object_type}")
            obj_data[attr] = [value]

        # Add to objects dataframe
        self.objects = pd.concat([self.objects, pd.DataFrame(obj_data)], ignore_index=True)

    def add_event(self, event_type: str, event_id: str, timestamp: datetime,
                  objects: Dict[str, List[str]], attributes: Dict) -> None:
        """Add a new event with validation against type definition"""
        if event_type not in self.event_type_definitions:
            print(f"Unknown event type: {event_type}")

        definition = self.event_type_definitions[event_type]

        # Validate required object types
        missing_obj_types = definition.required_object_types - set(objects.keys())
        if missing_obj_types:
            print(f"Missing required object types: {missing_obj_types}")

        # Validate required attributes
        missing_attrs = definition.required_attributes - set(attributes.keys())
        if missing_attrs:
            print(f"Missing required attributes: {missing_attrs}")

        # Create event entry
        event_data = {
            self.event_id_column: [event_id],
            self.event_timestamp: [timestamp],
            "event_type": [event_type]
        }

        # Add attributes
        for attr, value in attributes.items():
            if attr not in definition.required_attributes and attr not in definition.optional_attributes:
                print(f"Unknown attribute {attr} for type {event_type}")
            event_data[attr] = [value]

        # Add to events dataframe
        self.events = pd.concat([self.events, pd.DataFrame(event_data)], ignore_index=True)

        # Create relations
        relations_data = []
        for obj_type, obj_ids in objects.items():
            for obj_id in obj_ids:
                relations_data.append({
                    self.event_id_column: event_id,
                    self.object_id_column: obj_id,
                    self.object_type_column: obj_type
                })

        if relations_data:
            self.relations = pd.concat([self.relations, pd.DataFrame(relations_data)], ignore_index=True)

    def validate_consistency(self) -> List[str]:
        """Validate the consistency of the entire log against defined constraints"""
        issues = []

        # Validate objects
        for _, obj in self.objects.iterrows():
            obj_type = obj[self.object_type_column]
            if obj_type in self.object_type_definitions:
                definition = self.object_type_definitions[obj_type]
                # Check required attributes
                for attr in definition.required_attributes:
                    if attr not in obj or pd.isna(obj[attr]):
                        issues.append(f"Missing required attribute {attr} for object {obj[self.object_id_column]}")

        # Validate events and their relationships
        for _, event in self.events.iterrows():
            if "event_type" in event and event["event_type"] in self.event_type_definitions:
                definition = self.event_type_definitions[event["event_type"]]

                # Get related objects for this event
                related_objects = self.relations[self.relations[self.event_id_column] == event[self.event_id_column]]
                related_types = set(related_objects[self.object_type_column].unique())

                # Check required object types
                missing_types = definition.required_object_types - related_types
                if missing_types:
                    issues.append(f"Event {event[self.event_id_column]} missing required object types: {missing_types}")

        return issues


# Example usage
def create_iot_manufacturing_log():
    log = OCELIoT()

    # Register custom types
    log.register_object_type(ObjectTypeDefinition(
        name="Machine",
        parent_type="BusinessObject",
        required_attributes={"serial_number"},
        optional_attributes={"manufacturer", "model"}
    ))

    # Add objects
    log.add_object(
        object_type="Sensor",
        object_id="TEMP001",
        attributes={
            "name": "Temperature Sensor 1",
            "measurement_unit": "Celsius",
            "location": "Assembly Line 1"
        }
    )

    log.add_object(
        object_type="Machine",
        object_id="MCH001",
        attributes={
            "name": "Assembly Machine 1",
            "serial_number": "ASM123",
            "manufacturer": "ACME"
        }
    )

    # Add events
    log.add_event(
        event_type="IoTObservation",
        event_id="E001",
        timestamp=datetime.now(),
        objects={
            "Sensor": ["TEMP001"],
            "Machine": ["MCH001"]
        },
        attributes={
            "value": 23.5,
            "quality": 0.99
        }
    )

    return log

if __name__ == "__main__":
    log = create_iot_manufacturing_log()
    print(log.get_extended_table())
    issues = log.validate_consistency()
    if issues:
        print("Validation issues found:")
        for issue in issues:
            print(f" - {issue}")
    else:
        print("Log is consistent")