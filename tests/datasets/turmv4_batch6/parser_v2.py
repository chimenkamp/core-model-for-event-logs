from datetime import datetime
from typing import Dict, List, Any, Optional
import yaml
from uuid import uuid4

from src.wrapper.impl_v2 import OCELIoT, ObjectTypeDefinition, EventTypeDefinition


class SensorStreamParser:
    def __init__(self, ocel_iot: Optional['OCELIoT'] = None) -> None:
        """
        Initializes the SensorStreamParser class.

        :param ocel_iot: Optional OCELIoT instance. If None, creates a new one.
        """
        if ocel_iot is None:
            ocel_iot = OCELIoT()

        self.ocel = ocel_iot
        self._initialize_iot_types()

    def _initialize_iot_types(self):
        """Initialize the IoT-specific type definitions"""
        # Register IoT Device type
        self.ocel.register_object_type(ObjectTypeDefinition(
            name="IoTDevice",
            parent_type="DataSource",
            required_attributes={"device_id", "source_type"},
            optional_attributes={"location", "description"}
        ))

        # Register Observation event type
        self.ocel.register_event_type(EventTypeDefinition(
            name="Observation",
            subtype="IOT",
            required_object_types={"IoTDevice"},
            optional_object_types={"BusinessObject"},
            required_attributes={"value_type", "value"},
            optional_attributes={"quality", "unit"}
        ))

        # Register aggregated IoT event type
        self.ocel.register_event_type(EventTypeDefinition(
            name="AggregatedIoTEvent",
            subtype="IOT",
            required_object_types={"IoTDevice"},
            optional_object_types={"BusinessObject"},
            required_attributes={"observation_ids"},
            optional_attributes={"summary_statistics"}
        ))

    def parse_sensor_stream_log(self, sensorstream_log: List[Dict[str, Any]]) -> 'OCELIoT':
        """
        Parses a SensorStream log into the OCELIoT format.

        :param sensorstream_log: List of SensorStream events as dictionaries
        :return: OCELIoT object containing the parsed data
        """
        for event in sensorstream_log:
            if "event" not in event:
                continue

            event_data = event["event"]
            process_event_id: Optional[str] = None

            # Handle process event if present
            if "concept:name" in event_data:
                process_event_id = self._parse_process_event(event_data)

            # Handle sensor stream data if present
            if "stream:datastream" in event_data:
                data_stream: List[Dict] = event_data["stream:datastream"]
                self._parse_sensor_stream_data(data_stream, process_event_id)

        return self.ocel

    def _parse_process_event(self, process_event: Dict[str, Any]) -> str:
        """Parse process event and add it to OCEL"""
        event_id = process_event["id:id"]

        # Add process event if not already exists
        if not any(event_id == e_id for e_id in self.ocel.events[self.ocel.event_id_column]):
            self.ocel.add_event(
                event_type="ProcessEvent",
                event_id=event_id,
                timestamp=datetime.now(),
                objects={},  # Will be linked later if needed
                attributes={
                    "activity_name": process_event["concept:name"]
                }
            )

        return event_id

    def _parse_sensor_stream_data(self, sensorstream_events: List[Dict[str, Any]],
                                  process_event_id: Optional[str]) -> None:
        """Parse sensor stream data and add it to OCEL"""
        # Extract points and device information
        points = [x["stream:point"] for x in sensorstream_events if "stream:point" in x]
        iot_device_id = next(x["stream:name"] for x in sensorstream_events if "stream:name" in x)
        iot_source = next(x["stream:source"] for x in sensorstream_events if "stream:source" in x)

        # Create or get IoT device
        device_id = f"iot_device_{iot_device_id}"
        if device_id not in self.ocel.objects[self.ocel.object_id_column].values:
            self.ocel.add_object(
                object_type="IoTDevice",
                object_id=device_id,
                attributes={
                    "device_id": iot_device_id,
                    "source_type": iot_source
                }
            )

        # Process points into observations
        observation_groups = {}

        for point in points:
            if not isinstance(point, dict):
                continue

            obs_id = f"obs_{point.get('stream:id', str(uuid4()))}"
            timestamp = point.get("stream:timestamp", datetime.now())

            # Create observation event
            self.ocel.add_event(
                event_type="Observation",
                event_id=obs_id,
                timestamp=timestamp,
                objects={"IoTDevice": [device_id]},
                attributes={
                    "value_type": point.get("stream:type", "unknown"),
                    "value": point.get("stream:value", None),
                    "unit": point.get("stream:unit", None),
                    "quality": point.get("stream:quality", None)
                }
            )

            # Group observations for aggregation
            group_key = point.get("stream:group", "default")
            if group_key not in observation_groups:
                observation_groups[group_key] = []
            observation_groups[group_key].append(obs_id)

        # Create aggregated IoT events
        for group_key, obs_ids in observation_groups.items():
            if not obs_ids:
                continue

            # Get timestamp of first observation in group
            first_obs = self.ocel.events[
                self.ocel.events[self.ocel.event_id_column].isin(obs_ids)
            ].iloc[0]

            iot_event_id = f"iot_event_{group_key}_{str(uuid4())[:8]}"

            # Create aggregated IoT event
            self.ocel.add_event(
                event_type="AggregatedIoTEvent",
                event_id=iot_event_id,
                timestamp=first_obs[self.ocel.event_timestamp],
                objects={"IoTDevice": [device_id]},
                attributes={
                    "observation_ids": obs_ids,
                    "summary_statistics": self._calculate_summary_statistics(obs_ids)
                }
            )

            # Link to process event if exists
            if process_event_id:
                self.ocel.e2e = self.ocel.e2e.append({
                    self.ocel.event_id_column: process_event_id,
                    f"{self.ocel.event_id_column}_2": iot_event_id,
                    self.ocel.qualifier: "TRIGGERS"
                }, ignore_index=True)

    def _calculate_summary_statistics(self, observation_ids: List[str]) -> Dict[str, Any]:
        """Calculate summary statistics for a group of observations"""
        observations = self.ocel.events[
            self.ocel.events[self.ocel.event_id_column].isin(observation_ids)
        ]

        if len(observations) == 0:
            return {}

        values = observations.get("value", [])
        if not values or len(values) == 0:
            return {}

        return {
            "count": len(values),
            "mean": sum(values) / len(values) if all(isinstance(v, (int, float)) for v in values) else None,
            "min": min(values) if all(isinstance(v, (int, float)) for v in values) else None,
            "max": max(values) if all(isinstance(v, (int, float)) for v in values) else None
        }


def load_yaml(yaml_file: str) -> List[Any]:
    """Load YAML file and return all documents"""
    with open(yaml_file, 'r') as file:
        return list(yaml.safe_load_all(file))


# Example usage
def parse_sensor_stream_file(file_path: str) -> OCELIoT:
    """Parse a sensor stream YAML file into OCELIoT format"""
    yaml_data = load_yaml(file_path)
    parser = SensorStreamParser()
    return parser.parse_sensor_stream_log(yaml_data)


if __name__ == "__main__":
    ocel_iot = parse_sensor_stream_file("file.yaml")

