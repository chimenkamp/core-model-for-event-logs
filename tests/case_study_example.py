import pandas as pd

from src.classes_ import DataSource, IS, SOSA, Attribute, IoTEvent, ProcessEvent, Object, CCM, \
    Activity


def case_study_example():
    ccm = CCM()

    object_batch = Object(object_id="batch1", object_type="batch", attributes=[
        Attribute(key="recipe", value="13a4a45")
    ])
    object_tank = Object(object_id="tank1", object_type="tank", attributes=[
        Attribute(key="flow_status", value="drift"),
        Attribute(key="product_flow", value=206)
    ])
    object_sensor = Object(object_id="sensor1", object_type="sensor", attributes=[
        Attribute(key="precision", value="0.1"),
        Attribute(key="unit", value="l/h")
    ])

    event_sample_taken = ProcessEvent(event_id="event1", attributes=[
        Attribute(key="label", value="sample taken"),
    ])

    event_apply_taken = ProcessEvent(event_id="event2", attributes=[
        Attribute(key="label", value="apply sample")
    ])

    event_peak_detected = IoTEvent(event_id="event3", attributes=[
        Attribute(key="label", value="detect peak")
    ])

    event_value_change = IoTEvent(event_id="event4", attributes=[
        Attribute(key="label", value="value change"),
        Attribute(key="product_flow", value=206)
    ])

    event_sample_taken.add_activity(Activity(activity_id="activity1", activity_type="take sample"))
    event_apply_taken.add_activity(Activity(activity_id="activity2", activity_type="apply sample"))

    object_batch.add_event(event_sample_taken)
    object_batch.add_event(event_apply_taken)
    object_tank.add_event(event_value_change)
    object_tank.add_event(event_peak_detected)

    iot_device = SOSA.IoTDevice(device_id="device1")
    observation1 = SOSA.Observation(observation_id="obs1", observed_property="flow_status", value="normal")
    observation2 = SOSA.Observation(observation_id="obs2", observed_property="drift", value=0.1)

    iot_device.add_observation(observation1)
    iot_device.add_observation(observation2)

    # Adding observations to relevant events if needed
    event_value_change.attributes.append(Attribute(key="observation_drift", value=0.1))

    iot_device.record_event(event_value_change)
    iot_device.record_event(event_peak_detected)
    ccm.add_iot_device(iot_device)

    data_source_sensor = SOSA.IoTDevice(device_id="sensor1")
    data_source_sensor.record_event(event_value_change)

    object_sensor.add_data_source(data_source_sensor)

    is_system = IS(system_id="SAP ERP")

    is_system.record_event(event_sample_taken)
    is_system.record_event(event_apply_taken)

    object_tank.add_data_source(data_source_sensor)

    ccm.add_object(object_batch)
    ccm.add_object(object_tank)
    ccm.add_object(object_sensor)
    ccm.add_event(event_sample_taken)
    ccm.add_event(event_apply_taken)
    ccm.add_event(event_peak_detected)
    ccm.add_event(event_value_change)

    ccm.add_information_system(is_system)
    ccm.add_iot_device(data_source_sensor)
    ccm.add_iot_device(iot_device)

    object_tank.add_related_object(object_batch)

    return ccm


if __name__ == "__main__":
    ccm = case_study_example()

    query = "SELECT * FROM Event WHERE self.event_type == 'process event'"

    result_df1 = ccm.query(query)
    print(result_df1)

    table: pd.DataFrame = ccm.get_extended_table()
    # ccm.save_to_json("case_study_example.json")
    # ccm.visualize("case_study_example.png")
