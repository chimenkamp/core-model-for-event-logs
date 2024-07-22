CREATE TABLE Activity (
    ActivityID INT PRIMARY KEY
    -- Other attributes
);

CREATE TABLE Object (
    ObjectID INT PRIMARY KEY
    -- Other attributes
);

CREATE TABLE Event (
    EventID INT PRIMARY KEY,
    ObjectID INT,
    DataSourceID INT,
    DerivedFromEventID INT,
    FOREIGN KEY (ObjectID) REFERENCES Object(ObjectID),
    FOREIGN KEY (DataSourceID) REFERENCES DataSource(DataSourceID),
    FOREIGN KEY (DerivedFromEventID) REFERENCES Event(EventID)
    -- Other attributes
);

CREATE TABLE ProcessEvent (
    ProcessEventID INT PRIMARY KEY,
    FOREIGN KEY (ProcessEventID) REFERENCES Event(EventID)
    -- Other attributes
);

CREATE TABLE IoTEvent (
    IoTEventID INT PRIMARY KEY,
    FOREIGN KEY (IoTEventID) REFERENCES Event(EventID)
    -- Other attributes
);

CREATE TABLE DataSource (
    DataSourceID INT PRIMARY KEY
    -- Other attributes
);

CREATE TABLE IS (
    ISID INT PRIMARY KEY,
    EventID INT,
    FOREIGN KEY (EventID) REFERENCES Event(EventID)
    -- Other attributes
);

CREATE TABLE Observation (
    ObservationID INT PRIMARY KEY,
    IoTDeviceID INT,
    FOREIGN KEY (IoTDeviceID) REFERENCES IoTDevice(IoTDeviceID)
    -- Other attributes
);

CREATE TABLE IoTDevice (
    IoTDeviceID INT PRIMARY KEY
    -- Other attributes
);
