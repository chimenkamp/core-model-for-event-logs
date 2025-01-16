# Common-Core Model

## Overview
The Common-Core Model (CCM) is a framework for managing and representing data related to events, objects, activities, and data sources in a structured format. It includes various classes to represent these entities and their relationships.


### Event Attributes

| **Attribute**       | **Location (OCEL)**                     | **Description**                                                 |
|----------------------|-----------------------------------------|-----------------------------------------------------------------|
| `event_id`          | `events['ocel:eid']`                   | Unique identifier for each event                                |
| `timestamp`         | `events['ocel:timestamp']`             | Event occurrence timestamp                                      |
| `activity`          | `events['ocel:activity']`              | Activity type/label                                             |
| `event_type`        | `events['ocel:event_type']`            | Type of event                                                   |
| `event_class`       | `events['ocel:event_class']`           | Class categorization ('iot_event', 'process_event', 'observation') |
| Custom attributes   | `events['ocel:attr:*']`                | User-defined attributes prefixed with 'ocel:attr:'             |

*Table: Event Attributes Storage Structure*

---

### Object Attributes

| **Attribute**       | **Location (OCEL)**                     | **Description**                                                |
|----------------------|-----------------------------------------|----------------------------------------------------------------|
| `object_id`         | `objects['ocel:oid']`                  | Unique identifier for each object                              |
| `object_type`       | `objects['ocel:type']`                 | Type classification of the object                              |
| Custom attributes   | `objects['ocel:attr:*']`               | User-defined attributes prefixed with 'ocel:attr:'            |

*Table: Object Attributes Storage Structure*

---

### Event-Object Relations

| **Attribute**       | **Location (OCEL)**                     | **Description**                                                |
|----------------------|-----------------------------------------|----------------------------------------------------------------|
| `event_id`          | `relations['ocel:eid']`                | Event identifier                                               |
| `object_id`         | `relations['ocel:oid']`                | Object identifier                                              |
| `object_type`       | `relations['ocel:type']`               | Type of related object                                         |
| `activity`          | `relations['ocel:activity']`           | Activity of the event                                          |
| `qualifier`         | `relations['ocel:qualifier']`          | Relationship qualifier (default: "related")                   |

*Table: Event-Object Relations Storage Structure*

---

### Object-Object Relations (O2O)

| **Attribute**       | **Location (OCEL)**                     | **Description**                                                |
|----------------------|-----------------------------------------|----------------------------------------------------------------|
| `object_id_1`       | `o2o['ocel:oid']`                      | First object identifier                                        |
| `object_id_2`       | `o2o['ocel:oid_2']`                    | Second object identifier                                       |
| `qualifier`         | `o2o['ocel:qualifier']`                | Relationship qualifier (default: "related")                   |

*Table: Object-Object Relations Storage Structure*

---

### Event-Event Relations (E2E)

**TODO:** Remove table and add linking over object.

| **Attribute**       | **Location (OCEL)**                     | **Description**                                                |
|----------------------|-----------------------------------------|----------------------------------------------------------------|
| `event_id_1`        | `e2e['ocel:eid']`                      | First event identifier                                         |
| `event_id_2`        | `e2e['ocel:eid_2']`                    | Second event identifier                                        |
| `qualifier`         | `e2e['ocel:qualifier']`                | Relationship qualifier (default: "derived_from")              |

*Table: Event-Event Relations Storage Structure*

---

### Special Object Types

| **Object Type**      | **Storage Details**                                | **Description**                                                |
|-----------------------|---------------------------------------------------|----------------------------------------------------------------|
| IoT Device           | Stored as object with type `iot_device_[uuid]`    | Contains device-specific attributes                            |
| Information System   | Stored as object with type `information_system`   | Contains system name and attributes                            |
| Generic Objects      | Stored as object with user-defined type           | Standard object storage                                        |

*Table: Special Object Types Storage Structure*
