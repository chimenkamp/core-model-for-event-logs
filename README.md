# Common-Core Model

## Overview
The Common-Core Model (CCM) is a framework for managing and representing data related to events, objects, activities, and data sources in a structured format. It includes various classes to represent these entities and their relationships.

## Classes

| Class       | Description                                              | Relationships                                        |
|-------------|----------------------------------------------------------|------------------------------------------------------|
| Attribute   | Represents an attribute of an object or event.           | Used in Object, Event, and its subclasses.           |
| Event       | Represents a general event with an ID, type, timestamp, data source, and related objects. | Can be associated with DataSource and Object.        |
| IoTEvent    | Inherits from Event to represent an IoT-specific event.  | Subclass of Event.                                   |
| Activity    | Represents an activity associated with a process event.  | Used in ProcessEvent.                                |
| ProcessEvent| Inherits from Event to represent a process-specific event. | Subclass of Event, contains Activity.               |
| DataSource  | Represents a data source with an ID and type.            | Can be linked to Event and Object.                   |
| IS          | Inherits from DataSource to represent an information system. | Subclass of DataSource.                           |
| SOSA        | Namespace for the SOSA ontology, including Observation and IoTDevice classes. | SOSA.IoTDevice is a subclass of DataSource, contains SOSA.Observation. |
| Object      | Represents an object with an ID, type, related objects, events, and data sources. | Can be linked to Event, DataSource, and other Object instances. |
| CCM         | Represents the overall dataset containing events, objects, data sources, information systems, IoT devices, and activities. | Aggregates all other classes, manages their relationships, provides methods for data manipulation and visualization. |

## Class Relationships

### Attribute
- Used by Event, ProcessEvent, IoTEvent, Object, and DataSource.

### Event
- General class for events.
- Contains attributes (Attribute).
- Can have a DataSource.
- Can be linked to multiple Object.

### IoTEvent
- Inherits from Event.

### ProcessEvent
- Inherits from Event.
- Contains activities (Activity).

### Activity
- Used in ProcessEvent.

### DataSource
- Contains events (Event).
- Subclassed by IS and SOSA.IoTDevice.

### IS
- Inherits from DataSource.

### SOSA
- Contains Observation and IoTDevice.

### SOSA.Observation
- Used by SOSA.IoTDevice.

### SOSA.IoTDevice
- Inherits from DataSource.
- Contains SOSA.Observation.

### Object
- Contains attributes (Attribute).
- Can be linked to Event and DataSource.
- Can be linked to other Object instances.

### CCM
- Aggregates all other classes.
- Manages the relationships between events, objects, data sources, information systems, IoT devices, and activities.
- Provides methods for data manipulation (add_event, add_object, etc.).
- Provides methods for data visualization and exporting (visualize, save_to_json).

## Usage Example

```python
# Initialize CCM instance
ccm = CCM()

# Create objects
object_batch = Object(object_type="batch")
object_tank = Object(object_type="tank")
object_sensor = Object(object_type="sensor")

# Create events
event_sample_taken = ProcessEvent(activity=Activity(activity_type="take sample"))
event_apply_taken = ProcessEvent(activity=Activity(activity_type="apply sample"))
event_peak_detected = IoTEvent()
event_value_change = IoTEvent()

# Link events to objects
object_batch.add_event(event_sample_taken)
object_batch.add_event(event_apply_taken)
object_tank.add_event(event_value_change)
object_tank.add_event(event_peak_detected)

# Create IoT device and observations
iot_device = SOSA.IoTDevice()
observation1 = SOSA.Observation()
observation2 = SOSA.Observation()
iot_device.add_observation(observation1)
iot_device.add_observation(observation2)

# Record events in IoT device
ccm.add_iot_device(iot_device)

# Create data source sensor and record event
data_source_sensor = SOSA.IoTDevice()
ccm.add_iot_device(data_source_sensor)

# Link data source to object
object_sensor.add_data_source(data_source_sensor)

# Create information system and record events
is_system = IS()
ccm.add_information_system(is_system)

# Link data source to object
object_tank.add_data_source(data_source_sensor)

# Add entities to CCM
ccm.add_object(object_batch)
ccm.add_object(object_tank)
ccm.add_object(object_sensor)
ccm.add_event(event_sample_taken)
ccm.add_event(event_apply_taken)
ccm.add_event(event_peak_detected)
ccm.add_event(event_value_change)

# Link related objects
object_tank.add_related_object(object_batch)

# Visualize the data
ccm.visualize("case_study_example.png")

# Generate and save extended table
table = ccm.get_extended_table()
ccm.save_to_json("case_study_example.json")
