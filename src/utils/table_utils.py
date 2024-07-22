from typing import List, Dict, Any, Optional
import src
import pandas as pd


def create_row_element(
        data_sources: List['src.classes_.DataSource'],
        event: 'src.classes_.Event',
        related_object: Optional['src.classes_.Object'] = None
) -> Dict[str, Any]:
    row: Dict[str, Any] = {
        'ccm:event_id': event.event_id,
        'ccm:event_type': event.event_type,
        'ccm:timestamp': event.timestamp,
        'ccm:object_id': related_object.object_id if related_object else None,
        'ccm:object_type': related_object.object_type if related_object else None,
        'ccm:data_source_id': event.data_source.data_source_id if event.data_source else None,
        'ccm:data_source_type': event.data_source.data_source_type if event.data_source else None
    }

    for attr in event.attributes:
        row[f'event:{attr.key}'] = attr.value

    for attr in (related_object.attributes if related_object else []):
        row[f'object:{attr.key}'] = attr.value

    if event.data_source:
        for ds in data_sources:
            if ds.data_source_id == event.data_source.data_source_id:
                for attr in ds.attributes:
                    row[f'data_source:{attr.key}'] = attr.value

    if event.event_type == 'process event':
        event: 'src.classes_.ProcessEvent' = event
        row[f'activity:activity_type'] = event.activity.activity_type

    return row


def create_extended_table(objects: List, event_log: List, data_sources: List) -> pd.DataFrame:
    rows = []

    for event in event_log:
        related_objects = [obj for obj in objects if obj in event.related_objects]

        for related_object in related_objects:
            # TODO: Only unique events are added to the table. Necessary if a event is related to multiple objects

            if any([d['ccm:event_id'] == event.event_id for d in rows]):
                break

            row = create_row_element(data_sources=data_sources, event=event, related_object=related_object)
            rows.append(row)
        else:
            row = create_row_element(data_sources=data_sources, event=event)
            rows.append(row)

    df = pd.DataFrame(rows)
    return df
