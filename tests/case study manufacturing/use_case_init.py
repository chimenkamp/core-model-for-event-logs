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

    # Define Process Events derived from IoT Events for assembly line operations
    event_assembly_start = ProcessEvent(event_id="event3", attributes=[
        Attribute(key="label", value="assembly start"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    event_assembly_complete = ProcessEvent(event_id="event4", attributes=[
        Attribute(key="label", value="assembly complete"),
        Attribute(key="location", value="Main Manufacturing Plant")
    ])

    # Add activities to Process Events
    event_assembly_start.add_activity(Activity(activity_id="activity1", activity_type="start assembly"))
    event_assembly_complete.add_activity(Activity(activity_id="activity2", activity_type="complete assembly"))

    # Link IoT Events to Process Events
    event_assembly_start.add_related_event(event_shipment_arrival)
    event_assembly_complete.add_related_event(event_shipment_arrival)

    # Assign events to objects
    object_shipment.add_event(event_shipment_departure)
    object_shipment.add_event(event_shipment_arrival)
    object_sensor_rfid.add_event(event_shipment_departure)
    object_sensor_rfid.add_event(event_shipment_arrival)

    # Create IoT Device and Observations for assembly line operations
    iot_device_assembly = SOSA.IoTDevice(device_id="device1")
    observation_machine_performance = SOSA.Observation(observation_id="obs1", observed_property="machine_performance", value="optimal")
    observation_production_speed = SOSA.Observation(observation_id="obs2", observed_property="production_speed", value="fast")

    iot_device_assembly.add_observation(observation_machine_performance)
    iot_device_assembly.add_observation(observation_production_speed)

    # Link observations to events if necessary
    event_assembly_start.attributes.append(Attribute(key="machine_performance", value="optimal"))
    event_assembly_start.attributes.append(Attribute(key="production_speed", value="fast"))

    # Record events in IoT Device
    iot_device_assembly.record_event(event_assembly_start)
    iot_device_assembly.record_event(event_assembly_complete)
    ccm.add_iot_device(iot_device_assembly)

    # Define Data Source for sensors and link it to events
    data_source_sensor = SOSA.IoTDevice(device_id="sensor1")
    data_source_sensor.record_event(event_shipment_departure)
    object_sensor_rfid.add_data_source(data_source_sensor)

    # Define Information System and record events
    is_system = IS(system_id="SAP ERP")
    is_system.record_event(event_assembly_start)
    is_system.record_event(event_assembly_complete)

    # Add data sources to objects
    object_shipment.add_data_source(data_source_sensor)

    # Add objects and events to CCM
    ccm.add_object(object_shipment)
    ccm.add_object(object_sensor_rfid)
    ccm.add_event(event_shipment_departure)
    ccm.add_event(event_shipment_arrival)
    ccm.add_event(event_assembly_start)
    ccm.add_event(event_assembly_complete)

    # Add information system and IoT device to CCM
    ccm.add_information_system(is_system)
    ccm.add_iot_device(data_source_sensor)
    ccm.add_iot_device(iot_device_assembly)

    object_sensor_rfid.add_related_object(object_shipment)

    ccm.visualize("case_study_example.png")
    return ccm


if __name__ == "__main__":
    ccm: CCM = create_ccm_env()
    ccm.visualize("case_study_example.png")