import pandas as pd

from src.classes_ import CCM, Object, Attribute, IoTEvent, ProcessEvent, Activity, SOSA, IS


def create_ccm_env() -> CCM:
    ccm = CCM()

    # Define objects with attributes for raw material procurement and supply chain management
    object_shipment = Object(object_id="shipment1", object_type="shipment", attributes=[
        Attribute(key="status", value="in transit"),
        Attribute(key="environmental_conditions", value="stable"),
        Attribute(key="location", value="Supplier Warehouse")
    ])

    object_sensor_rfid = Object(object_id="rfid1", object_type="rfid", attributes=[
        Attribute(key="precision", value="0.1"),
        Attribute(key="unit", value="cm"),
        Attribute(key="location", value="Supplier Warehouse")
    ])

    # Define IoT Events for raw material procurement and supply chain management
    event_shipment_departure = IoTEvent(event_id="event1", attributes=[
        Attribute(key="label", value="shipment departure"),
        Attribute(key="location", value="Supplier Warehouse")
    ])

    event_shipment_arrival = IoTEvent(event_id="event2", attributes=[
        Attribute(key="label", value="shipment arrival"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    # Additional IoT Events
    event_temperature_check = IoTEvent(event_id="event5", attributes=[
        Attribute(key="label", value="temperature check"),
        Attribute(key="location", value="Supplier Warehouse")
    ])

    event_humidity_check = IoTEvent(event_id="event6", attributes=[
        Attribute(key="label", value="humidity check"),
        Attribute(key="location", value="Supplier Warehouse")
    ])

    event_vibration_check = IoTEvent(event_id="event7", attributes=[
        Attribute(key="label", value="vibration check"),
        Attribute(key="location", value="Supplier Warehouse")
    ])

    event_light_check = IoTEvent(event_id="event8", attributes=[
        Attribute(key="label", value="light check"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_pressure_check = IoTEvent(event_id="event9", attributes=[
        Attribute(key="label", value="pressure check"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    # Define Process Events derived from IoT Events for assembly line operations
    event_assembly_start = ProcessEvent(event_id="event3", attributes=[
        Attribute(key="label", value="assembly start"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_assembly_complete = ProcessEvent(event_id="event4", attributes=[
        Attribute(key="label", value="assembly complete"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    # Additional Process Events
    event_quality_check = ProcessEvent(event_id="event10", attributes=[
        Attribute(key="label", value="quality check"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_packaging_start = ProcessEvent(event_id="event11", attributes=[
        Attribute(key="label", value="packaging start"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_packaging_complete = ProcessEvent(event_id="event12", attributes=[
        Attribute(key="label", value="packaging complete"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_dispatch_ready = ProcessEvent(event_id="event13", attributes=[
        Attribute(key="label", value="dispatch ready"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_dispatch_complete = ProcessEvent(event_id="event14", attributes=[
        Attribute(key="label", value="dispatch complete"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    # Add activities to Process Events
    event_assembly_start.add_activity(Activity(activity_id="activity1", activity_type="start assembly"))
    event_assembly_complete.add_activity(Activity(activity_id="activity2", activity_type="complete assembly"))

    # Assign events to objects
    object_shipment.add_event(event_shipment_departure)
    object_shipment.add_event(event_shipment_arrival)
    object_shipment.add_event(event_temperature_check)
    object_shipment.add_event(event_humidity_check)
    object_shipment.add_event(event_vibration_check)
    object_sensor_rfid.add_event(event_shipment_departure)
    object_sensor_rfid.add_event(event_shipment_arrival)
    object_sensor_rfid.add_event(event_light_check)
    object_sensor_rfid.add_event(event_pressure_check)

    # Create IoT Devices and Observations for the new IoT Events
    iot_device_temperature = SOSA.IoTDevice(device_id="device2")
    observation_temperature = SOSA.Observation(observation_id="obs3", observed_property="temperature", value="22C")
    iot_device_temperature.add_observation(observation_temperature)
    event_temperature_check.attributes.append(Attribute(key="temperature", value="22C"))

    iot_device_humidity = SOSA.IoTDevice(device_id="device3")
    observation_humidity = SOSA.Observation(observation_id="obs4", observed_property="humidity", value="50%")
    iot_device_humidity.add_observation(observation_humidity)
    event_humidity_check.attributes.append(Attribute(key="humidity", value="50%"))

    iot_device_vibration = SOSA.IoTDevice(device_id="device4")
    observation_vibration = SOSA.Observation(observation_id="obs5", observed_property="vibration", value="normal")
    iot_device_vibration.add_observation(observation_vibration)
    event_vibration_check.attributes.append(Attribute(key="vibration", value="normal"))

    iot_device_light = SOSA.IoTDevice(device_id="device5")
    observation_light = SOSA.Observation(observation_id="obs6", observed_property="light", value="bright")
    iot_device_light.add_observation(observation_light)
    event_light_check.attributes.append(Attribute(key="light", value="bright"))

    iot_device_pressure = SOSA.IoTDevice(device_id="device6")
    observation_pressure = SOSA.Observation(observation_id="obs7", observed_property="pressure", value="1 atm")
    iot_device_pressure.add_observation(observation_pressure)
    event_pressure_check.attributes.append(Attribute(key="pressure", value="1 atm"))

    # Record events in IoT Devices
    iot_device_temperature.record_event(event_temperature_check)
    iot_device_humidity.record_event(event_humidity_check)
    iot_device_vibration.record_event(event_vibration_check)
    iot_device_light.record_event(event_light_check)
    iot_device_pressure.record_event(event_pressure_check)

    ccm.add_iot_device(iot_device_temperature)
    ccm.add_iot_device(iot_device_humidity)
    ccm.add_iot_device(iot_device_vibration)
    ccm.add_iot_device(iot_device_light)
    ccm.add_iot_device(iot_device_pressure)

    # Define Data Source for sensors and link it to events
    data_source_sensor = SOSA.IoTDevice(device_id="sensor1")
    data_source_sensor.record_event(event_shipment_departure)
    object_sensor_rfid.add_data_source(data_source_sensor)

    # Define Information System and record events
    is_system = IS(system_id="SAP ERP")
    is_system.record_event(event_assembly_start)
    is_system.record_event(event_assembly_complete)
    is_system.record_event(event_quality_check)
    is_system.record_event(event_packaging_start)
    is_system.record_event(event_packaging_complete)
    is_system.record_event(event_dispatch_ready)
    is_system.record_event(event_dispatch_complete)

    # Add data sources to objects
    object_shipment.add_data_source(data_source_sensor)

    # Add objects and events to CCM
    ccm.add_object(object_shipment)
    ccm.add_object(object_sensor_rfid)
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

    # Add information system and IoT devices to CCM
    ccm.add_information_system(is_system)
    ccm.add_iot_device(data_source_sensor)
    ccm.add_iot_device(iot_device_temperature)
    ccm.add_iot_device(iot_device_humidity)
    ccm.add_iot_device(iot_device_vibration)
    ccm.add_iot_device(iot_device_light)
    ccm.add_iot_device(iot_device_pressure)

    object_sensor_rfid.add_related_object(object_shipment)

    return ccm


if __name__ == "__main__":
    ccm: CCM = create_ccm_env()

    table: pd.DataFrame = ccm.get_extended_table()
    ccm.visualize("use_case_init_long.png")
    print(table)
