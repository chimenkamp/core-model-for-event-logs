import re
import sys
import traceback
import typing
from typing import List, Dict, Union, Optional
import pandas as pd

import src

from src.utils.table_utils import create_extended_table
from src.utils.types import CCMEntry


def parse_query(query: str) -> Dict[str, Union[str, List[str]]]:
    """
    Parses the SQL-like query to extract SELECT fields, FROM class, and WHERE clause.

    :param query: The SQL-like query string.
    :return: A dictionary containing 'select', 'from', and 'where' keys with corresponding parsed values.
    """
    select_pattern: str = r'SELECT\s+(.*?)\s+FROM'
    from_pattern: str = r'FROM\s+(\w+)'
    where_pattern: str = r'WHERE\s+(.*)'

    select_match = re.search(select_pattern, query, re.IGNORECASE)
    from_match = re.search(from_pattern, query, re.IGNORECASE)
    where_match = re.search(where_pattern, query, re.IGNORECASE)

    select_fields: List[str] = select_match.group(1).split(',') if select_match else []
    from_class: str = from_match.group(1) if from_match else ''
    where_clause: str = where_match.group(1) if where_match else ''

    return {
        'select': [field.strip() for field in select_fields],
        'from': from_class,
        'where': where_clause
    }


def extract_first_elements(condition_string: str) -> List[str]:
    """
    Extract the first elements before the dot in each condition from the given string.

    :param condition_string: The condition string to extract elements from.
    :return: A list of the first elements before the dot in each condition.
    """
    pattern = r"(\w+)\."
    matches = re.findall(pattern, condition_string)
    return matches


def evaluate_where_clause(obj_dict: Dict[str, typing.Any], where_clause: str) -> List[dict[str, CCMEntry]]:
    """
    Evaluates the WHERE clause on the given objects.

    :param obj_dict: A dictionary with class names as keys and class instances as values.
    :param where_clause: The WHERE clause string.
    :return: True if the objects satisfy the WHERE clause, False otherwise.
    """
    # Replace = with == and replace AND/OR with Python's logical operators
    where_clause = (where_clause
                    .replace('=', '==')
                    .replace(' AND ', ' and ')
                    .replace(' OR ', ' or '))

    replace_map: dict[str, str] = {
        "Event": "e",
        "Object": "o",
        "InformationSystem": "is",
        "IoTDevice": "d",
        "Attribute": "a",
        "DataSource": "ds",
        "ProcessEvent": "pe",
        "Activity": "act",
        "Observation": "obs",
    }

    for class_name, instance in obj_dict.items():
        where_clause = where_clause.replace(class_name, replace_map[class_name])

    results: List[dict[str, CCMEntry]] = []

    for event in obj_dict['Event']:
        e = event
        obs: List[CCMEntry] = event.related_objects
        ds: "src.classes_.DataSource" = event.data_source

        for ob in obs:
            o = ob

            context = {
                "e": e,
                "o": o,
                "ds": ds
            }

            try:
                eval_result = eval(where_clause, {}, context)
                print(f"Evaluated where clause: {where_clause} -> {eval_result}")
                if eval_result:
                    results.append({"event": e, "object": o, "result": eval_result})
            except Exception as e:
                print("##################################################")
                print(f"Error evaluating where clause: {e}, {where_clause} -> {obj_dict}")
                traceback.print_exc(file=sys.stdout)
                print("##################################################")
                return False
    return results

def class_instances_to_dataframe(instances: List['CCMEntry'], fields: List[str]) -> pd.DataFrame:
    """
    Converts a list of class instances to a pandas DataFrame, extending the table for fields that are objects with attributes.

    :param instances: The list of class instances.
    :param fields: The list of fields to include in the DataFrame.
    :return: The resulting DataFrame.
    """
    rows = []

    for instance in instances:
        serialized = instance.serialize()
        row = {}

        for field in fields:
            if field == '*':
                for key, value in serialized.items():
                    if isinstance(value, list) and value and isinstance(value[0], CCMEntry):
                        for obj in value:
                            obj_serialized = obj.serialize()
                            for obj_key, obj_value in obj_serialized.items():
                                row[f'{key}:{obj_key}'] = obj_value
                    elif isinstance(value, CCMEntry):
                        nested_serialized = value.serialize()
                        for nested_key, nested_value in nested_serialized.items():
                            row[f'{field}:{nested_key}'] = nested_value
                    else:
                        row[key] = value
            elif field in serialized:
                value = serialized[field]
                if isinstance(value, list) and value and isinstance(value[0], CCMEntry):
                    for obj in value:
                        obj_serialized = obj.serialize()
                        for obj_key, obj_value in obj_serialized.items():
                            row[f'{field}:{obj_key}'] = obj_value
                elif isinstance(value, CCMEntry):
                    nested_serialized = value.serialize()
                    for nested_key, nested_value in nested_serialized.items():
                        row[f'{field}:{nested_key}'] = nested_value
                else:
                    row[field] = value

        rows.append(row)

    df = pd.DataFrame(rows)
    return df


def query_classes(
        query: str,
        classes: Dict[str, List['CCMEntry']],
        return_format: str = "dataframe") -> Union[pd.DataFrame, Dict[str, List['CCMEntry']]]:
    """
    Executes a SQL-like query on the given classes.

    :param return_format: The format to return the result in.
        dataframe: Return the result as a pandas DataFrame.
        class_reference: Return the result as a dictionary of class names to lists of class instances.
        extended_table: Return the result as an extended table with resolved references. Only works with From Event.
    :param query: The SQL-like query string.
    :param classes: A dictionary of class names to lists of class instances.
    :return: The resulting DataFrame or dictionary of lists of objects.
    """

    parsed_query = parse_query(query)
    select_fields: List[str] = parsed_query['select']
    from_class: str = parsed_query['from']
    where_clause: str = parsed_query['where']

    if return_format == 'extended_table' and from_class != 'Event':
        raise ValueError("Extended table format is only supported for querying from the Event class")

    if from_class not in classes:
        raise ValueError(f"Class {from_class} not found")

    filtered_instances: list[dict[str, CCMEntry]] = evaluate_where_clause(classes, where_clause)

    if return_format == 'class_reference':
        return {from_class: filtered_instances}
    elif return_format == 'extended_table':

        return create_extended_table(
            event_log=[instance['event'] for instance in filtered_instances if 'event' in instance],
            objects=[instance['object'] for instance in filtered_instances if 'object' in instance],
            data_sources=[instance['ds'] for instance in filtered_instances if 'ds' in instance]
        )
    else:
        raise ValueError(f"Unsupported return format: {return_format}")