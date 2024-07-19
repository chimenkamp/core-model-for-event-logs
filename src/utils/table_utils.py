from typing import List, Dict, Any

import pandas as pd


def create_extended_table(objects: List, event_log: List, data_sources: List) -> pd.DataFrame:
    rows = []

    for event in event_log:
        related_objects = [obj for obj in objects if obj in event.related_objects]

        for related_object in related_objects:
            row: Dict[str, Any] = {
                'ccm:event_id': event.event_id,
                'ccm:event_type': event.event_type,
                'ccm:timestamp': event.timestamp,
                'ccm:object_id': related_object.object_id,
                'ccm:object_type': related_object.object_type,
                'ccm:data_source_id': event.data_source.source_id if event.data_source else None,
                'ccm:data_source_type': event.data_source.source_type if event.data_source else None
            }

            for attr in event.attributes:
                row[f'event:{attr.key}'] = attr.value

            for attr in related_object.attributes:
                row[f'object:{attr.key}'] = attr.value

            if event.data_source:
                for ds in data_sources:
                    if ds.source_id == event.data_source.source_id:
                        for ds_event in ds.events:
                            if ds_event.event_id == event.event_id:
                                for e_attr in ds_event.attributes:
                                    row[f'data_source_event:{e_attr.key}'] = e_attr.value

            if event.event_type == 'process event':
                for activity in event.activities:
                    row[f'activity:{activity.activity_id}_type'] = activity.activity_type

            rows.append(row)

    df = pd.DataFrame(rows)
    return df