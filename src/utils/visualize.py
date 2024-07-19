from typing import Set, List, Tuple


def create_graph(objects: List, event_log: List, data_sources: List, iot_devices: List,
                 information_systems: List) -> 'Digraph':
    """
    This method creates a Graphviz Digraph object representing the CCM dataset.
    :return: A Graphviz Digraph object.
    """
    from graphviz import Digraph

    dot = Digraph(comment='CCM Diagram')
    edges: Set[Tuple[str, str]] = set()

    add_objects(dot, edges, objects)
    add_event_log(dot, edges, event_log)
    add_data_sources(dot, edges, data_sources)
    add_iot_devices(dot, edges, iot_devices)
    add_information_systems(dot, edges, information_systems, event_log, objects)

    return dot


def add_objects(dot: 'Digraph', edges: Set[Tuple[str, str]], objects: List) -> None:
    for obj in objects:
        dot.node(obj.object_id, label=f"Object\n{obj.object_type}\n{obj.object_id}", shape='box', style='filled',
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


def add_event_log(dot: 'Digraph', edges: Set[Tuple[str, str]], event_log: List) -> None:
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
            for activity in event.activities:
                dot.node(activity.activity_id, label=f"Activity\n{activity.activity_type}\n{activity.activity_id}",
                         shape='diamond', style='filled', color='lightcoral')
                edge = (event.event_id, activity.activity_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)


def add_data_sources(dot: 'Digraph', edges: Set[Tuple[str, str]], data_sources: List) -> None:
    for ds in data_sources:
        dot.node(ds.source_id, label=f"Data Source\n{ds.source_type}\n{ds.source_id}", shape='box', style='filled',
                 color='lightpink')

        for event in ds.events:
            edge = (ds.source_id, event.event_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)


def add_iot_devices(dot: 'Digraph', edges: Set[Tuple[str, str]], iot_devices: List) -> None:
    for device in iot_devices:
        dot.node(device.source_id, label=f"IoT Device\n{device.source_id}", shape='box', style='filled',
                 color='lightcyan')

        for obs in device.observations:
            obs_id = obs.observation_id
            dot.node(obs_id, label=f"Observation\n{obs.observed_property}: {obs.value}", shape='ellipse',
                     style='filled', color='lightgoldenrod')
            edge = (device.source_id, obs_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)

        for event in device.events:
            edge = (device.source_id, event.event_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)


def add_information_systems(dot: 'Digraph', edges: Set[Tuple[str, str]], information_systems: List, event_log: List,
                            objects: List) -> None:
    for is_system in information_systems:
        dot.node(is_system.source_id, label=f"Information System\n{is_system.source_id}", shape='box', style='filled',
                 color='black', fontcolor='white')

        for event in event_log:
            if event.data_source and event.data_source.source_type == "information system" and event.data_source.source_id == is_system.source_id:
                edge = (is_system.source_id, event.event_id)
                if edge not in edges:
                    dot.edge(*edge)
                    edges.add(edge)

    for obj in objects:
        for ds in obj.data_sources:
            edge = (obj.object_id, ds.source_id)
            if edge not in edges:
                dot.edge(*edge)
                edges.add(edge)
