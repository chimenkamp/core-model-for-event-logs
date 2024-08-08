from typing import Optional

import pandas as pd
import pm4py
from pm4py import OCEL

from src.classes_ import CCM, Event, Object, ProcessEvent, IoTEvent


class CoreOCELWrapper:
    def __init__(self, ocel: OCEL) -> None:
        """
        Initialize the CoreOCELWrapper with an existing OCEL instance.

        :param ocel: Instance of OCEL.
        """
        self.ocel = ocel

    def add_ccm_data(self, ccm: CCM) -> None:
        """
        Add data from CCM to the OCEL instance.

        :param ccm: CCM instance containing the data.
        :return: None.
        """
        for event in ccm.get_events():
            self.add_event(event)
        for obj in ccm.objects:
            self.add_object(obj)

    def add_event(self, event: Event) -> None:
        """
        Add an event to the OCEL instance.

        :param event: Event instance to add.
        :return: None.
        """
        event_data = {
            self.ocel.event_id_column: event.event_id,
            self.ocel.event_timestamp: event.timestamp
        }

        if isinstance(event, ProcessEvent):
            event_data[self.ocel.event_activity] = event.activity.activity_type
        else:
            event_data[self.ocel.event_activity] = "unset"

        self.ocel.events = pd.concat([self.ocel.events, pd.DataFrame([event_data])], ignore_index=True)

        for obj in event.related_objects:
            relation_data = {
                self.ocel.event_id_column: event.event_id,
                self.ocel.object_id_column: obj.object_id,
                self.ocel.object_type_column: obj.object_type
            }
            self.ocel.relations = pd.concat([self.ocel.relations, pd.DataFrame([relation_data])], ignore_index=True)

        if event.data_source:
            data_source_data = {
                self.ocel.object_id_column: event.data_source.data_source_id,
                self.ocel.object_type_column: event.data_source.data_source_type
            }
            self.ocel.objects = pd.concat([self.ocel.objects, pd.DataFrame([data_source_data])], ignore_index=True)

        for derived_event in event.derived_from_events:
            e2e_data = {
                self.ocel.event_id_column: event.event_id,
                f"{self.ocel.event_id_column}_2": derived_event.event_id,
                self.ocel.qualifier: "derived_from"
            }
            self.ocel.e2e = pd.concat([self.ocel.e2e, pd.DataFrame([e2e_data])], ignore_index=True)

        if isinstance(event, IoTEvent):
            for observation in event.observations:
                observation_data = {
                    self.ocel.object_id_column: observation.observation_id,
                    self.ocel.object_type_column: "observation"
                }
                self.ocel.objects = pd.concat([self.ocel.objects, pd.DataFrame([observation_data])], ignore_index=True)

                if observation.iot_device:
                    o2o_data = {
                        self.ocel.object_id_column: observation.observation_id,
                        f"{self.ocel.object_id_column}_2": observation.iot_device.data_source_id,
                        self.ocel.qualifier: "observed_by"
                    }
                    self.ocel.o2o = pd.concat([self.ocel.o2o, pd.DataFrame([o2o_data])], ignore_index=True)

    def add_object(self, obj: Object) -> None:
        """
        Add an object to the OCEL instance.

        :param obj: Object instance to add.
        :return: None.
        """
        object_data = {
            self.ocel.object_id_column: obj.object_id,
            self.ocel.object_type_column: obj.object_type
        }
        self.ocel.objects = pd.concat([self.ocel.objects, pd.DataFrame([object_data])], ignore_index=True)

        if obj.data_source:
            data_source_data = {
                self.ocel.object_id_column: obj.data_source.data_source_id,
                self.ocel.object_type_column: obj.data_source.data_source_type
            }
            self.ocel.objects = pd.concat([self.ocel.objects, pd.DataFrame([data_source_data])], ignore_index=True)

        for related_obj in obj.related_objects:
            o2o_data = {
                self.ocel.object_id_column: obj.object_id,
                f"{self.ocel.object_id_column}_2": related_obj.object_id,
                self.ocel.qualifier: "related_to"
            }
            print(o2o_data)
            self.ocel.o2o = pd.concat([self.ocel.o2o, pd.DataFrame([o2o_data])], ignore_index=True)

    def get_extended_table(self) -> pd.DataFrame:
        """
        Get the extended table representation of the OCEL data.

        :return: Pandas DataFrame representing the extended table.
        """
        return self.ocel.get_extended_table()

    def get_summary(self) -> str:
        """
        Get the summary of the OCEL data.

        :return: String summary of the OCEL data.
        """
        return self.ocel.get_summary()

    def is_ocel20(self) -> bool:
        """
        Check if the OCEL instance adheres to the OCEL 2.0 standard.

        :return: True if OCEL 2.0 standard, otherwise False.
        """
        return self.ocel.is_ocel20()

    def get_ocel(self) -> OCEL:
        return self.ocel

    def write_to_file(self, path: str) -> None:
        """
        Write the OCEL data to a file.

        :param path: Path to write the file to.
        :return: None.
        """
        pm4py.write_ocel2(self.ocel, path)



