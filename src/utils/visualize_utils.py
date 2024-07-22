from typing import Set, List, Tuple
import src


def create_graph(
        objects: List["src.classes_.Object"],
        event_log: List["src.classes_.Event"],
        data_sources: List["src.classes_.DataSource"],
        iot_devices: List["src.classes_.SOSA.IoTDevice"],
        information_systems: List["src.classes_.IS"],
        observations: List["src.classes_.SOSA.Observation"]
) -> 'Digraph':
    """
    This method creates a Graphviz Digraph object representing the CCM dataset.

    :param observations: List of observations.
    :param objects: List of objects.
    :param event_log: List of events.
    :param data_sources: List of data sources.
    :param iot_devices: List of IoT devices.
    :param information_systems: List of information systems.
    :return: A Graphviz Digraph object.
    """
    from graphviz import Digraph

    dot = Digraph(comment='CCM Diagram')
    edges: Set[Tuple[str, str]] = set()

    add_objects(dot, edges, objects)
    add_event_log(dot, edges, event_log)
    add_data_sources(dot, edges, data_sources)
    add_iot_devices(dot, edges, iot_devices, observations, event_log)
    add_information_systems(dot, edges, information_systems, event_log, objects)
    add_event_relations(dot, edges, event_log)

    return dot


def add_objects(dot: 'Digraph', edges: Set[Tuple[str, str]], objects: List["src.classes_.Object"]) -> None:
    """
    Adds object nodes and edges to the Graphviz Digraph.

    :param dot: The Graphviz Digraph object.
    :param edges: The set of edges to prevent duplicates.
    :param objects: The list of objects to add.
    :return: None
    """
    for obj in objects:
        dot.node(obj.object_id, label=f"Object\n{obj.object_type}\n{obj.object_id}", shape='box',
                 style='filled',
                 color='lightyellow')

        for attr in obj.attributes:
            attr_id = f"{obj.object_id}_{attr.key}"
            dot.node(attr_id, label=f"Attribute\n{attr.key}: {attr.value}", shape='ellipse', style='filled',
                     color='lightgrey')
            edge = (obj.object_id, attr_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)

        for related_obj in obj.related_objects:
            edge = (obj.object_id, related_obj.object_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)


def add_event_log(dot: 'Digraph', edges: Set[Tuple[str, str]], event_log: List['src.classes_.Event']) -> None:
    """
    Adds event log nodes and edges to the Graphviz Digraph.

    :param dot: The Graphviz Digraph object.
    :param edges: The set of edges to prevent duplicates.
    :param event_log: The list of events to add.
    :return: None
    """
    for event in event_log:
        event_label = "Process Event" if event.event_type == "process event" else "IoT Event"
        dot.node(event.event_id, label=f"{event_label}\n{event.event_id}", shape='box', style='filled',
                 color='lightblue')

        for attr in event.attributes:
            attr_id = f"{event.event_id}_{attr.key}"
            dot.node(attr_id, label=f"Event Attribute\n{attr.key}: {attr.value}", shape='ellipse', style='filled',
                     color='lightgreen')
            edge = (event.event_id, attr_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)

        for obj in event.related_objects:
            edge = (obj.object_id, event.event_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)

        if event.event_type == "process event":
            event: "src.classes_.ProcessEvent" = event

            dot.node(event.activity.activity_id, label=f"Activity\n{event.activity.activity_type}\n{event.activity.activity_id}",
                     shape='diamond', style='filled', color='lightcoral')
            edge = (event.event_id, event.activity.activity_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)


def add_data_sources(dot: 'Digraph', edges: Set[Tuple[str, str]],
                     data_sources: List["src.classes_.DataSource"]) -> None:
    """
    Adds data source nodes and edges to the Graphviz Digraph.

    :param dot: The Graphviz Digraph object.
    :param edges: The set of edges to prevent duplicates.
    :param data_sources: The list of data sources to add.
    :return: None
    """
    for ds in data_sources:
        dot.node(ds.data_source_id, label=f"Data Source\n{ds.data_source_type}\n{ds.data_source_id}", shape='box',
                 style='filled',
                 color='lightpink')

        for attr in ds.attributes:
            attr_id = f"{ds.data_source_id}_{attr.key}"
            dot.node(attr_id, label=f"Attribute\n{attr.key}: {attr.value}", shape='ellipse', style='filled',
                     color='lightgreen')
            edge = (ds.data_source_id, attr_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)


def add_iot_devices(
        dot: 'Digraph',
        edges: Set[Tuple[str, str]],
        iot_devices: List["src.classes_.SOSA.IoTDevice"],
        observations: List["src.classes_.SOSA.Observation"],
        events: List["src.classes_.Event"]) -> None:
    """
    Adds IoT device nodes and edges to the Graphviz Digraph.

    :param dot: The Graphviz Digraph object.
    :param edges: The set of edges to prevent duplicates.
    :param iot_devices: The list of IoT devices to add.
    :return: None
    """
    for device in iot_devices:
        dot.node(device.data_source_id, label=f"IoT Device\n{device.data_source_id}", shape='box', style='filled',
                 color='lightcyan')

        for obs in observations:
            if obs.iot_device and obs.iot_device.data_source_id == device.data_source_id:
                dot.node(obs.observation_id, label=f"Observation\n{obs.observation_id}", shape='ellipse',
                         style='filled',
                         color='lightblue')
                edge = (device.data_source_id, obs.observation_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

        for event in events:
            if (event.data_source and event.data_source.data_source_type == "IoT device" and
                    event.data_source.data_source_id == device.data_source_id):
                edge = (device.data_source_id, event.event_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)


def add_information_systems(dot: 'Digraph', edges: Set[Tuple[str, str]], information_systems: List['src.classes_.IS'],
                            event_log: List['src.classes_.Event'],
                            objects: List["src.classes_.Object"]) -> None:
    """
    Adds information system nodes and edges to the Graphviz Digraph.

    :param dot: The Graphviz Digraph object.
    :param edges: The set of edges to prevent duplicates.
    :param information_systems: The list of information systems to add.
    :param event_log: The list of events.
    :param objects: The list of objects.
    :return: None
    """
    for is_system in information_systems:
        dot.node(is_system.data_source_id, label=f"Information System\n{is_system.data_source_id}", shape='box',
                 style='filled',
                 color='black', fontcolor='white')

        for event in event_log:
            if (event.data_source and event.data_source.data_source_type == "information system" and
                    event.data_source.data_source_id == is_system.data_source_id):
                edge = (is_system.data_source_id, event.event_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

        for obj in objects:

            if not obj.data_source:
                continue

            edge = (obj.object_id, obj.data_source.data_source_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)


def add_event_relations(dot: 'Digraph', edges: Set[Tuple[str, str]], event_log: List["src.classes_.Event"]) -> None:
    """
    Adds event relation nodes and edges to the Graphviz Digraph.

    :param dot: The Graphviz Digraph object.
    :param edges: The set of edges to prevent duplicates.
    :param event_log: The list of events.
    :return: None
    """
    for event in event_log:
        if not event.derived_from_events:
            continue
        for related_event in event.derived_from_events:
            edge = (event.event_id, related_event.event_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)
