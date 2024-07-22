import pandas as pd

from src.classes_ import DataSource, IS, SOSA, Attribute, IoTEvent, ProcessEvent, Object, CCM, \
    Activity


def implement_case_study() -> CCM:
    """
    Implements the manufacturing case study based on the provided diagram.

    :return: An instance of the CCM class populated with the case study data.
    """
    ccm = CCM()

    # Create objects and attributes
    batch = Object(object_type="batch")
    batch_1 = Object(object_type="batch")
    recipe = Attribute(key="recipe", value="13g45x5")
    batch_1.add_related_object(batch)
    batch_1.add_data_source(recipe)

    analytics_1 = Object(object_type="analytics")
    tank_1 = Object(object_type="tank")
    flow_sensor = Object(object_type="sensor")
    precision = Attribute(key="precision", value="~0.1")
    flow_sensor.add_data_source(precision)

    unit = Object(object_type="unit")
    unit.add_data_source(Attribute(key="lt/h", value=1))

    flow_procedure_2 = Object(object_type="procedure")
    flow_status = Attribute(key="flow status", value="drift")
    flow_procedure_2.add_related_object(tank_1)
    tank_1.add_data_source(flow_status)
    tank_1.add_data_source(Attribute(key="flow status", value="206"))
    tank_1.add_data_source(Attribute(key="product flow", value="206"))

    # Create events
    event_sample_taken = ProcessEvent()
    event_sample_taken.add_object(batch_1)
    event_sample_taken.add_activity(Activity(activity_type="take sample"))
    event_sample_taken.add_data_source(recipe)

    event_peak_detected = IoTEvent()
    event_peak_detected.add_object(analytics_1)
    event_peak_detected.add_activity(Activity(activity_type="detect peak"))

    event_value_change = IoTEvent()
    event_value_change.add_object(flow_sensor)
    event_value_change.add_activity(Activity(activity_type="product flow"))

    # Create observations
    observation_1 = SOSA.Observation()
    observation_1.add_iot_device(SOSA.IoTDevice())

    # Add data to CCM
    ccm.add_object(batch)
    ccm.add_object(batch_1)
    ccm.add_object(analytics_1)
    ccm.add_object(tank_1)
    ccm.add_object(flow_sensor)
    ccm.add_object(unit)
    ccm.add_object(flow_procedure_2)

    ccm.add_event(event_sample_taken)
    ccm.add_event(event_peak_detected)
    ccm.add_event(event_value_change)

    ccm.add_observation(observation_1)

    return ccm

if __name__ == "__main__":
    ccm = implement_case_study()

    query = "SELECT * FROM Event WHERE self.event_type == 'process event'"

    result_df1 = ccm.query(query)
    print(result_df1)

    table: pd.DataFrame = ccm.get_extended_table()
    # ccm.save_to_json("case_study_example.json")
    # ccm.visualize("case_study_example.png")
