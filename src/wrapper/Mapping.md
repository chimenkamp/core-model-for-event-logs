# Mapping of Classes to OCEL

## 1. Event (Base Class)

**Attributes:**
- `event_id` → OCEL `event_id_column`
- `timestamp` → OCEL `event_timestamp`
- `related_objects` → OCEL `relations`
- `data_source` → OCEL `objects`
- `derived_from_events` → OCEL `e2e`

## 2. ProcessEvent (Inherits from Event)

**Attributes:**
- `activity` → OCEL `event_activity`

## 3. IoTEvent (Inherits from Event)

**Attributes:**
- `observations` → OCEL `objects` and `o2o` (relationships between observations and IoT devices)

## 4. Object

**Attributes:**
- `object_id` → OCEL `object_id_column`
- `object_type` → OCEL `object_type_column`
- `related_objects` → OCEL `o2o` (relationships between objects)
- `data_source` → OCEL `objects`

## 5. DataSource

**Attributes:**
- `data_source_id` → OCEL `object_id_column`
- `data_source_type` → OCEL `object_type_column`

## 6. IS (Inherits from DataSource)

**Attributes:**
- `is_id` → OCEL `object_id_column` (inherited from `data_source_id`)
- `event` → Not directly mapped, handled as part of `ProcessEvent`

## 7. SOSA.Observation

**Attributes:**
- `observation_id` → OCEL `object_id_column`
- `iot_device` → OCEL `o2o` (relationship between observations and IoT devices)

## 8. SOSA.IoTDevice (Inherits from DataSource)

**Attributes:**
- `iot_device_id` → OCEL `object_id_column` (inherited from `data_source_id`)

# Detailed Storage in OCEL

## Events DataFrame

**Columns:**
- `event_id_column`: Stores `event_id` from `Event`, `ProcessEvent`, `IoTEvent`
- `event_activity`: Stores `activity.activity_type` from `ProcessEvent`, "unset" for `IoTEvent`
- `event_timestamp`: Stores `timestamp` from `Event`, `ProcessEvent`, `IoTEvent`

## Objects DataFrame

**Columns:**
- `object_id_column`: Stores `object_id` from `Object`, `data_source_id` from `DataSource`, `observation_id` from `SOSA.Observation`
- `object_type_column`: Stores `object_type` from `Object`, `data_source_type` from `DataSource`, "observation" for `SOSA.Observation`

## Relations DataFrame

**Columns:**
- `event_id_column`: Stores `event_id` from `Event`, `ProcessEvent`, `IoTEvent`
- `object_id_column`: Stores `object_id` from `Object`
- `object_type_column`: Stores `object_type` from `Object`

## e2e DataFrame (Event to Event Relationships)

**Columns:**
- `event_id_column`: Stores `event_id` from `Event`, `ProcessEvent`, `IoTEvent`
- `event_id_column_2`: Stores `event_id` from `derived_from_events` in `Event`, `ProcessEvent`, `IoTEvent`
- `qualifier`: Stores relationship type, e.g., "derived_from"

## o2o DataFrame (Object to Object Relationships)

**Columns:**
- `object_id_column`: Stores `object_id` from `Object`, `observation_id` from `SOSA.Observation`
- `object_id_column_2`: Stores `object_id` from related `Object`, `iot_device_id` from `SOSA.IoTDevice`
- `qualifier`: Stores relationship type, e.g., "related_to", "observed_by"
