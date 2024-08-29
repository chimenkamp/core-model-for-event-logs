import datetime
from random import randint
from typing import List, Dict, Any

import pandas as pd

from src.wrapper.ocel_wrapper import OCELWrapper

objects: List[Dict[str, Any]] = [
    {
        "object_id": "1",
        "object_type": "shipment",
        "attributes": {
            "status": "in transit",
            "environmental_conditions": "stable",
            "location": "Supplier Warehouse"
        }
    },
    {
        "object_id": "2",
        "object_type": "sensor",
        "attributes": {
            "precision": "0.1",
            "unit": "cm",
            "location": "Supplier Warehouse"
        }
    },
    {
        "object_id": "3",
        "object_type": "sensor",
        "attributes": {
            "precision": "0.1",
            "unit": "cm",
            "location": "Supplier Warehouse"
        }
    }
]

object_relationships: List[Dict[str, str]] = [
    {
        "object_id": "1",
        "related_object_id": "3"
    }
]

iot_events: List[Dict[str, Any]] = [
    {
        "event_id": "1",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "shipment departure",
            "location": "Supplier Warehouse"
        }
    },
    {
        "event_id": "2",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "shipment arrival",
            "location": "Main Manufacturing Plant"
        }
    },
    {
        "event_id": "5",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "temperature check",
            "location": "Supplier Warehouse",
            "temperature": "22C"
        }
    },
    {
        "event_id": "6",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "humidity check",
            "location": "Supplier Warehouse",
            "humidity": "50%"
        }
    },
    {
        "event_id": "7",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "vibration check",
            "location": "Supplier Warehouse",
            "vibration": "normal"
        }
    },
    {
        "event_id": "8",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "light check",
            "location": "Main Manufacturing Plant",
            "light": "bright"
        }
    },
    {
        "event_id": "9",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "pressure check",
            "location": "Main Manufacturing Plant",
            "pressure": "1 atm"
        }
    }
]

process_events: List[Dict[str, Any]] = [
    {
        "event_id": "3",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "assembly start",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "assembly started"
        }
    },
    {
        "event_id": "4",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "assembly complete",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "assembly completed"
        }
    },
    {
        "event_id": "10",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "quality check",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "quality check performed"
        }
    },
    {
        "event_id": "11",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "packaging start",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "packaging started"
        }
    },
    {
        "event_id": "12",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "packaging complete",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "packaging completed"
        }
    },
    {
        "event_id": "13",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "dispatch ready",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "dispatch ready"
        }
    },
    {
        "event_id": "14",
        "timestamp": datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        "attributes": {
            "label": "dispatch complete",
            "location": "Main Manufacturing Plant"
        },
        "activity": {
            "activity_type": "dispatch completed"
        }
    }
]

event_object_relationships: List[Dict[str, str]] = [
    {
        "event_id": "1",
        "object_id": "2"
    },
    {
        "event_id": "2",
        "object_id": "2"
    },
    {
        "event_id": "5",
        "object_id": "2"
    },
    {
        "event_id": "6",
        "object_id": "1"
    },
    {
        "event_id": "7",
        "object_id": "1"
    },
    {
        "event_id": "8",
        "object_id": "2"
    },
    {
        "event_id": "9",
        "object_id": "2"
    }
]

event_event_relationships: List[Dict[str, str]] = [
    {
        "event_id": "1",
        "derived_from_event_id": "8"
    }
]

iot_devices: List[Dict[str, str]] = [
    {
        "device_id": "iot_device_temperature"
    },
    {
        "device_id": "iot_device_humidity"
    },
    {
        "device_id": "iot_device_vibration"
    },
    {
        "device_id": "iot_device_light"
    },
    {
        "device_id": "iot_device_pressure"
    }
]

observations: List[Dict[str, str]] = [
    {
        "observation_id": "3",
        "iot_device_id": "iot_device_temperature"
    },
    {
        "observation_id": "4",
        "iot_device_id": "iot_device_humidity"
    },
    {
        "observation_id": "5",
        "iot_device_id": "iot_device_vibration"
    },
    {
        "observation_id": "6",
        "iot_device_id": "iot_device_light"
    },
    {
        "observation_id": "7",
        "iot_device_id": "iot_device_pressure"
    }
]

event_data_source_relationships: List[Dict[str, str]] = [
    {
        "event_id": "5",
        "data_source_id": "iot_device_temperature"
    },
    {
        "event_id": "6",
        "data_source_id": "iot_device_humidity"
    },
    {
        "event_id": "7",
        "data_source_id": "iot_device_vibration"
    },
    {
        "event_id": "8",
        "data_source_id": "iot_device_light"
    },
    {
        "event_id": "9",
        "data_source_id": "iot_device_pressure"
    },
    {
        "event_id": "3",
        "data_source_id": "is_system"
    },
    {
        "event_id": "4",
        "data_source_id": "is_system"
    },
    {
        "event_id": "10",
        "data_source_id": "is_system"
    },
    {
        "event_id": "11",
        "data_source_id": "is_system"
    },
    {
        "event_id": "12",
        "data_source_id": "is_system"
    },
    {
        "event_id": "13",
        "data_source_id": "is_system"
    },
    {
        "event_id": "14",
        "data_source_id": "is_system"
    }
]

information_systems: List[Dict[str, str]] = [
    {
        "is_id": "1",
        "system_name": "is_system"
    }
]


wrapper = OCELWrapper(objects=objects,
                      object_object_relationships=object_relationships,
                      iot_events=iot_events,
                      process_events=process_events,
                      event_object_relationships=event_object_relationships,
                      event_event_relationships=event_event_relationships,
                      iot_devices=iot_devices,
                      observations=observations,
                      event_data_source_relationships=event_data_source_relationships,
                      information_systems=information_systems)

extended_table: pd.DataFrame = wrapper.get_extended_table()
print(extended_table)