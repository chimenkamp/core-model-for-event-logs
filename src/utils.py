def _match_conditions(event: "Event", conditions: dict) -> bool:
    """
    Checks if an event matches the provided conditions.
    :param event: The event to check.
    :param conditions: A dictionary with keys as attributes and values as desired values.
    :return: True if the event matches the conditions, False otherwise.
    """
    for key, value in conditions.items():
        if key.startswith("event:"):
            attr_key = key.split("event:")[1]
            if not any(attr.key == attr_key and attr.value == value for attr in event.attributes):
                return False
        elif key.startswith("object:"):
            attr_key = key.split("object:")[1]
            if not any(any(attr.key == attr_key and attr.value == value for attr in obj.attributes) for obj in event.related_objects):
                return False
        elif key == "event_type":
            if event.event_type != value:
                return False
        elif key == "data_source_id":
            if not event.data_source or event.data_source.source_id != value:
                return False
        elif key == "data_source_type":
            if not event.data_source or event.data_source.source_type != value:
                return False

    return True


def _parse_query(query_str: str) -> dict:
    """
    Parses a query string into a dictionary of conditions.
    :param query_str: The query string.
    :return: A dictionary of conditions.
    """
    conditions = {}
    conditions_list = query_str.split(" and ")
    for condition in conditions_list:
        key, value = condition.split("==")
        key = key.strip()
        value = value.strip().strip("'")
        conditions[key] = value

    return conditions
