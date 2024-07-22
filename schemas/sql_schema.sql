CREATE TABLE Attribute (
    attribute_id UUID PRIMARY KEY,
    key VARCHAR(255) NOT NULL,
    value TEXT
);

CREATE TABLE Activity (
    activity_id UUID PRIMARY KEY,
    activity_type TEXT
);

CREATE TABLE DataSource (
    data_source_id UUID PRIMARY KEY,
    data_source_type VARCHAR(50) CHECK (data_source_type IN ('information system', 'iot device'))
);

CREATE TABLE Event (
    event_id UUID PRIMARY KEY,
    event_type VARCHAR(50) CHECK (event_type IN ('process event', 'iot event')),
    data_source_id UUID,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (data_source_id) REFERENCES DataSource(data_source_id)
);

CREATE TABLE ProcessEvent (
    process_event_id UUID PRIMARY KEY,
    event_id UUID,
    activity_id UUID,
    FOREIGN KEY (event_id) REFERENCES Event(event_id),
    FOREIGN KEY (activity_id) REFERENCES Activity(activity_id)
);

CREATE TABLE IoTEvent (
    iot_event_id UUID PRIMARY KEY,
    event_id UUID,
    FOREIGN KEY (event_id) REFERENCES Event(event_id)
);

CREATE TABLE IS (
    is_id UUID PRIMARY KEY,
    data_source_id UUID,
    event_id UUID,
    FOREIGN KEY (data_source_id) REFERENCES DataSource(data_source_id),
    FOREIGN KEY (event_id) REFERENCES ProcessEvent(process_event_id)
);

CREATE TABLE Observation (
    observation_id UUID PRIMARY KEY,
    iot_device_id UUID,
    FOREIGN KEY (iot_device_id) REFERENCES DataSource(data_source_id)
);

CREATE TABLE Object (
    object_id UUID PRIMARY KEY,
    object_type VARCHAR(255) NOT NULL,
    data_source_id UUID,
    FOREIGN KEY (data_source_id) REFERENCES DataSource(data_source_id)
);

CREATE TABLE EventObject (
    event_id UUID,
    object_id UUID,
    PRIMARY KEY (event_id, object_id),
    FOREIGN KEY (event_id) REFERENCES Event(event_id),
    FOREIGN KEY (object_id) REFERENCES Object(object_id)
);

CREATE TABLE RelatedObjects (
    object_id UUID,
    related_object_id UUID,
    PRIMARY KEY (object_id, related_object_id),
    FOREIGN KEY (object_id) REFERENCES Object(object_id),
    FOREIGN KEY (related_object_id) REFERENCES Object(object_id)
);
