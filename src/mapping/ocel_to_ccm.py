import datetime
from typing import List, Optional
import pandas as pd
from pm4py import OCEL

from src.classes_ import CCM, Object, DataSource, Event, ProcessEvent, IoTEvent, Activity


def _print_progress(current_step: int, total_steps: int) -> None:
    """
    Prints the progress of the mapping process.

    :param current_step: Current step of the mapping process.
    :param total_steps: Total steps in the mapping process.
    """
    percentage = (current_step / total_steps) * 100
    progress_bar_length = 40
    filled_length = int(progress_bar_length * current_step // total_steps)
    bar = '█' * filled_length + '-' * (progress_bar_length - filled_length)
    print(f'\r|{bar}| {percentage:.2f}% Complete', end='\r')
    if current_step == total_steps:
        print()


class OCELToCCMMapper:
    """
    Class to map data from OCEL format to a custom CCM format.
    """

    def __init__(self, ocel: OCEL) -> None:
        """
        Initializes the OCELToCCMMapper with the given OCEL data.

        :param ocel: The OCEL data to be mapped.
        """
        self.ocel: OCEL = ocel
        self.ccm: CCM = self.map_to_ccm()

    def map_to_ccm(self) -> CCM:
        """
        Converts the OCEL format to the custom data format (CCM).

        :return: A CCM object.
        """
        ccm = CCM()
        total_steps = len(self.ocel.objects) + len(self.ocel.events) + len(self.ocel.relations)
        current_step = 0

        # Map objects
        for _, ocel_obj in self.ocel.objects.iterrows():
            obj = Object(
                object_type=ocel_obj['ocel:type'],
                object_id=ocel_obj['ocel:oid']
            )
            ccm.add_object(obj)
            current_step += 1
            _print_progress(current_step, total_steps)

        # Map events
        for _, ocel_event in self.ocel.events.iterrows():
            event_type = ocel_event['ocel:activity']
            e_id: str = ocel_event['ocel:eid']
            if e_id not in [e.event_id for e in ccm.event_log]:
                event = ProcessEvent(
                    event_id=ocel_event['ocel:eid'],
                    timestamp=ocel_event['ocel:timestamp'] if isinstance(ocel_event['ocel:timestamp'], datetime.datetime) else datetime.datetime.fromisoformat(ocel_event['ocel:timestamp']),
                    activity=Activity(activity_type=event_type)
                )
                ccm.add_event(event)
            current_step += 1
            _print_progress(current_step, total_steps)

        # Map relations
        for _, ocel_relation in self.ocel.relations.iterrows():
            event = next(e for e in ccm.event_log if e.event_id == ocel_relation['ocel:eid'])
            related_object = next(o for o in ccm.objects if o.object_id == ocel_relation['ocel:oid'])
            event.add_object(related_object)
            current_step += 1
            _print_progress(current_step, total_steps)

        # Handle event attributes and additional data source if available
        for _, ocel_event in self.ocel.events.iterrows():
            event = next(e for e in ccm.event_log if e.event_id == ocel_event['ocel:eid'])
            if 'ocel:vmap' in ocel_event and 'data_source' in ocel_event['ocel:vmap']:
                data_source = DataSource(data_source_type=ocel_event['ocel:vmap']['data_source'])
                event.add_data_source(data_source)
            current_step += 1
            _print_progress(current_step, total_steps)

        return ccm
