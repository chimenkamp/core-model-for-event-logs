import datetime
from typing import List, Optional
import pandas as pd
from pm4py import OCEL

from src.classes_ import CCM, Object, DataSource, Event, ProcessEvent, IoTEvent


class OCELToCCMMapper:
    """
    Class to map data from OCEL format to a custom CCM format.
    """

    def __init__(self, ocel: OCEL) -> None:
        self.ocel = ocel
        self.ccm: CCM = self.map_to_ccm()

    def map_to_ccm(self) -> CCM:
        """
        Converts the OCEL format to the custom data format (CCM).

        :return: A CCM object.
        """
        ccm = CCM()

        # Map objects
        for _, ocel_obj in self.ocel.objects.iterrows():
            obj = Object(
                object_type=ocel_obj['ocel:type'],
                object_id=ocel_obj['ocel:oid']
            )
            ccm.add_object(obj)

        # Map events
        for _, ocel_event in self.ocel.events.iterrows():
            event_type = ocel_event['ocel:activity']
            event = Event(
                event_typ='process event' if isinstance(event_type, str) else 'iot event',
                event_id=ocel_event['ocel:eid'],
                timestamp=ocel_event['ocel:timestamp']
                if isinstance(ocel_event['ocel:timestamp'], datetime.datetime)
                else datetime.datetime.fromisoformat(ocel_event['ocel:timestamp'])
            )
            ccm.add_event(event)

        # Map relations
        for _, ocel_relation in self.ocel.relations.iterrows():
            event = next(e for e in ccm.event_log if e.event_id == ocel_relation['ocel:eid'])
            related_object = next(o for o in ccm.objects if o.object_id == ocel_relation['ocel:oid'])
            event.add_object(related_object)

        # Handle event attributes and additional data source if available
        for _, ocel_event in self.ocel.events.iterrows():
            event = next(e for e in ccm.event_log if e.event_id == ocel_event['ocel:eid'])
            if 'ocel:vmap' in ocel_event and 'data_source' in ocel_event['ocel:vmap']:
                data_source = DataSource(data_source_type=ocel_event['ocel:vmap']['data_source'])
                event.add_data_source(data_source)

        return ccm
