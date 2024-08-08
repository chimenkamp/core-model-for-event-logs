from typing import Any

from pm4py import OCEL

from src.classes_ import Object, ProcessEvent, Activity, CCM


class OCELExtended(OCEL):
    """
    Class for the OCEL Extended mapping.
    """

    def __init__(self, ontology: CCM, data: dict[str, Any], config: dict[str, Any]) -> None:
        """
        Constructor of the OCEL Extended mapping.

        :param ontology: The ontology to be used.
        :param data: The data to be used.
        :param config: The configuration to be used.
        """
        super().__init__()
        self.ontology = ontology
        self.data = data
        self.config = config

        self.map_elements()

    def map_elements(self):
        """
        Maps the data to the ontology using the OCEL Extended mapping.
        """
        self._map_objects()
        self._map_events()
        self._map_relations()
        self._map_globals()
        self._map_o2o()
        self._map_e2e()
        self._map_object_changes()

    def _map_objects(self):
        """
        Maps the objects.
        :return:
        """
        for _, ocel_obj in self.data.objects.iterrows():
            obj = Object(
                object_type=ocel_obj[self.ontology.object_type_column],
                object_id=ocel_obj[self.ontology.object_id_column]
            )
            self.ontology.add_object(obj)

    def _map_events(self):
        """
        Maps the events.
        :return:
        """
        for _, ocel_event in self.data.events.iterrows():
            event_type = ocel_event[self.ontology.event_activity]
            e_id: str = ocel_event[self.ontology.event_id_column]
            if e_id not in [e.event_id for e in self.ontology.event_log]:
                event = ProcessEvent(
                    event_id=e_id,
                    activity=Activity(activity_type=event_type)
                )
                self.ontology.add_event(event)

    def _map_relations(self):
        """
        Maps the relations.
        :return:
        """
        for _, ocel_relation in self.data.relations.iterrows():
            event = next(
                e for e in self.ontology.event_log if e.event_id == ocel_relation[self.ontology.event_id_column])
            related_object = next(
                o for o in self.ontology.objects if o.object_id == ocel_relation[self.ontology.object_id_column])
            event.add_object(related_object)

    def _map_globals(self):
        """
        Maps the globals.
        :return:
        """

    def _map_o2o(self):
        """
        Maps the o2o.
        :return:
        """

    def _map_e2e(self):
        """
        Maps the e2e.
        :return:
        """

    def _map_object_changes(self):
        """
        Maps the object changes.
        :return:
        """

    def get_extended_table(self):
        """
        Gets the extended table.
        :return:
        """
        return self.ontology.get_extended_table()
