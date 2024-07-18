from typing import Any


def define_schema(log: Any, IS) -> dict[str, Any]:
    data: dict[str, Any] = {
        'ccm:events': [
            {
                'event_id': event.event_id,
                'event_type': event.event_type,
                'attributes': {attr.key: attr.value for attr in event.attributes},
                'timestamp': event.timestamp.isoformat(),
                'object_id': [o.object_id for o in event.related_objects],
                'data_source_id': event.data_source.source_id
            } for event in log.event_log
        ],
        'ccm:objects': [
            {
                'object_id': obj.object_id,
                'object_type': obj.object_type,
                'attributes': {attr.key: attr.value for attr in obj.attributes},
                'events': [event.event_id for event in obj.events]
            } for obj in log.objects
        ],
        'ccm:data_sources': [
            {
                'source_id': ds.source_id,
                'source_type': ds.source_type,
                'events': [event.event_id for event in ds.events]
            } for ds in log.data_sources
        ],
        'ccm:information_systems': [
            {
                'system_id': is_.source_id,
                'data_sources': [ds.source_id for ds in log.data_sources if isinstance(ds, IS)],
                'events': [event.event_id for event in log.event_log if
                           isinstance(event.data_source.source_id, IS) and event.data_source.source_id == is_.source_id]
            } for is_ in log.information_systems
        ],
        'ccm:iot_devices': [
            {
                'device_id': device.source_id,
                'observations': [
                    {
                        'observation_id': obs.observation_id,
                        'observed_property': obs.observed_property,
                        'value': obs.value
                    } for obs in device.observations
                ]
            } for device in log.iot_devices
        ]
    }
    return data