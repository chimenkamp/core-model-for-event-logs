from random import randint

import pm4py

from src.classes_ import CCM, Object, Attribute, IoTEvent, ProcessEvent, Activity, SOSA, IS

import datetime
import pandas as pd

from src.mapping.ccm_to_ocel import CCMToOcelMapper


def create_ccm_env() -> CCM:
    ccm = CCM()

    # Define objects with attributes for raw material procurement and supply chain management
    object_shipment = Object(object_id="1", object_type="shipment")
    object_shipment.add_attribute(Attribute(key="status", value="in transit"))
    object_shipment.add_attribute(Attribute(key="environmental_conditions", value="stable"))
    object_shipment.add_attribute(Attribute(key="location", value="Supplier Warehouse"))

    object_sensor_rfid = Object(object_id="2", object_type="sensor")
    object_sensor_rfid.add_attribute(Attribute(key="precision", value="0.1"))
    object_sensor_rfid.add_attribute(Attribute(key="unit", value="cm"))
    object_sensor_rfid.add_attribute(Attribute(key="location", value="Supplier Warehouse"))

    # Define IoT Events for raw material procurement and supply chain management
    event_shipment_departure = IoTEvent(event_id="1",
                                        timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_shipment_departure.add_attribute(Attribute(key="label", value="shipment departure"))
    event_shipment_departure.add_attribute(Attribute(key="location", value="Supplier Warehouse"))

    event_shipment_arrival = IoTEvent(event_id="2",
                                      timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_shipment_arrival.add_attribute(Attribute(key="label", value="shipment arrival"))
    event_shipment_arrival.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    # Additional IoT Events
    event_temperature_check = IoTEvent(event_id="5",
                                       timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_temperature_check.add_attribute(Attribute(key="label", value="temperature check"))
    event_temperature_check.add_attribute(Attribute(key="location", value="Supplier Warehouse"))

    event_humidity_check = IoTEvent(event_id="6",
                                    timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_humidity_check.add_attribute(Attribute(key="label", value="humidity check"))
    event_humidity_check.add_attribute(Attribute(key="location", value="Supplier Warehouse"))

    event_vibration_check = IoTEvent(event_id="7",
                                     timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_vibration_check.add_attribute(Attribute(key="label", value="vibration check"))
    event_vibration_check.add_attribute(Attribute(key="location", value="Supplier Warehouse"))

    event_light_check = IoTEvent(event_id="8",
                                 timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_light_check.add_attribute(Attribute(key="label", value="light check"))
    event_light_check.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    event_pressure_check = IoTEvent(event_id="9",
                                    timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)))
    event_pressure_check.add_attribute(Attribute(key="label", value="pressure check"))
    event_pressure_check.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    # Define Process Events derived from IoT Events for assembly line operations
    event_assembly_start = ProcessEvent(event_id="3",
                                        timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
                                        activity=Activity(activity_type="assembly started")
                                        )
    event_assembly_start.add_attribute(Attribute(key="label", value="assembly start"))
    event_assembly_start.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    event_assembly_start.add_activity(Activity(activity_type="assembly started"))

    event_assembly_complete = ProcessEvent(
        event_id="4",
        timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
        activity=Activity(activity_type="assembly completed")
    )
    event_assembly_complete.add_attribute(Attribute(key="label", value="assembly complete"))
    event_assembly_complete.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    # Additional Process Events
    event_quality_check = ProcessEvent(event_id="10",
                                       timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
                                       activity=Activity(activity_type="quality check performed"))

    event_quality_check.add_attribute(Attribute(key="label", value="quality check"))
    event_quality_check.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    event_packaging_start = ProcessEvent(event_id="11",
                                         timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
                                         activity=Activity(activity_type="packaging started")
                                         )
    event_packaging_start.add_attribute(Attribute(key="label", value="packaging start"))
    event_packaging_start.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    event_packaging_complete = ProcessEvent(event_id="12",
                                            timestamp=datetime.datetime.now() + datetime.timedelta(
                                                minutes=randint(1, 60)),
                                            activity=Activity(activity_type="packaging completed")
                                            )
    event_packaging_complete.add_attribute(Attribute(key="label", value="packaging complete"))
    event_packaging_complete.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    event_dispatch_ready = ProcessEvent(event_id="13",
                                        timestamp=datetime.datetime.now() + datetime.timedelta(minutes=randint(1, 60)),
                                        activity=Activity(activity_type="dispatch ready")
                                        )
    event_dispatch_ready.add_attribute(Attribute(key="label", value="dispatch ready"))
    event_dispatch_ready.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    event_dispatch_complete = ProcessEvent(event_id="14",
                                           timestamp=datetime.datetime.now() + datetime.timedelta(
                                               minutes=randint(1, 60)),
                                           activity=Activity(activity_type="dispatch completed")
                                           )
    event_dispatch_complete.add_attribute(Attribute(key="label", value="dispatch complete"))
    event_dispatch_complete.add_attribute(Attribute(key="location", value="Main Manufacturing Plant"))

    # Add events to objects
    event_shipment_departure.add_object(object_sensor_rfid)
    event_shipment_arrival.add_object(object_sensor_rfid)
    event_temperature_check.add_object(object_sensor_rfid)
    event_humidity_check.add_object(object_shipment)
    event_vibration_check.add_object(object_shipment)

    event_shipment_departure.add_object(object_sensor_rfid)
    event_shipment_arrival.add_object(object_sensor_rfid)
    event_light_check.add_object(object_sensor_rfid)
    event_pressure_check.add_object(object_sensor_rfid)

    # Create IoT Devices and Observations for the new IoT Events
    iot_device_temperature = SOSA.IoTDevice()
    observation_temperature = SOSA.Observation(observation_id="3", iot_device=iot_device_temperature)
    event_temperature_check.add_attribute(Attribute(key="temperature", value="22C"))
    event_temperature_check.add_data_source(iot_device_temperature)

    iot_device_humidity = SOSA.IoTDevice()
    observation_humidity = SOSA.Observation(observation_id="4", iot_device=iot_device_humidity)
    event_humidity_check.add_attribute(Attribute(key="humidity", value="50%"))
    event_humidity_check.add_data_source(iot_device_humidity)

    iot_device_vibration = SOSA.IoTDevice()
    observation_vibration = SOSA.Observation(observation_id="5", iot_device=iot_device_vibration)
    event_vibration_check.add_attribute(Attribute(key="vibration", value="normal"))
    event_vibration_check.add_data_source(iot_device_vibration)

    iot_device_light = SOSA.IoTDevice()
    observation_light = SOSA.Observation(observation_id="6", iot_device=iot_device_light)
    event_light_check.add_attribute(Attribute(key="light", value="bright"))
    event_light_check.add_data_source(iot_device_light)

    iot_device_pressure = SOSA.IoTDevice()
    observation_pressure = SOSA.Observation(observation_id="7", iot_device=iot_device_pressure)
    event_pressure_check.add_attribute(Attribute(key="pressure", value="1 atm"))
    event_pressure_check.add_data_source(iot_device_pressure)

    # Add IoT devices to CCM
    ccm.add_iot_device(iot_device_temperature)
    ccm.add_iot_device(iot_device_humidity)
    ccm.add_iot_device(iot_device_vibration)
    ccm.add_iot_device(iot_device_light)
    ccm.add_iot_device(iot_device_pressure)

    # Define Information System and add events
    is_system = IS(is_id="1")
    event_assembly_start.add_data_source(is_system)
    event_assembly_complete.add_data_source(is_system)
    event_quality_check.add_data_source(is_system)
    event_temperature_check.add_data_source(is_system)
    event_packaging_start.add_data_source(is_system)
    event_packaging_complete.add_data_source(is_system)
    event_dispatch_ready.add_data_source(is_system)
    event_dispatch_complete.add_data_source(is_system)

    # Add information system to CCM
    ccm.add_information_system(is_system)

    # Add objects to CCM
    ccm.add_object(object_shipment)
    ccm.add_object(object_sensor_rfid)

    # Add events to CCM
    ccm.add_event(event_shipment_departure)
    ccm.add_event(event_shipment_arrival)
    ccm.add_event(event_assembly_start)
    ccm.add_event(event_assembly_complete)
    ccm.add_event(event_temperature_check)
    ccm.add_event(event_humidity_check)
    ccm.add_event(event_vibration_check)
    ccm.add_event(event_light_check)
    ccm.add_event(event_pressure_check)
    ccm.add_event(event_quality_check)
    ccm.add_event(event_packaging_start)
    ccm.add_event(event_packaging_complete)
    ccm.add_event(event_dispatch_ready)
    ccm.add_event(event_dispatch_complete)

    ccm.add_observation(observation_temperature)
    ccm.add_observation(observation_humidity)
    ccm.add_observation(observation_vibration)
    ccm.add_observation(observation_light)
    ccm.add_observation(observation_pressure)

    return ccm


if __name__ == "__main__":
    ccm: CCM = create_ccm_env()

    ccm.visualize("use_case_init_long.png")

    table: pd.DataFrame = ccm.get_extended_table()
    print(table)

    # Example query 1: Select all IoT events where the label is 'temperature check' or 'humidity check' query1 =
    # "SELECT event_id, event_type FROM Event WHERE (Event.event_type = 'iot event' AND Object.object_id = '2')"
    query1 = "SELECT * FROM Event WHERE (Event.event_type = 'iot event' AND Object.object_id = '2')"
    result1: pd.DataFrame = ccm.query(query1, "extended_table")
    print("Query 1 Result:")
    print(len(result1))

    ocel_mapping: CCMToOcelMapper = CCMToOcelMapper(ccm)

    print(ocel_mapping.ocel)
