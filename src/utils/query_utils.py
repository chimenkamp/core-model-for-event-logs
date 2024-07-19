from typing import List, Union, Dict
import pandas as pd
import re

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


# Function to evaluate the WHERE clause
def evaluate_where_clause(obj: CCMEntry, where_clause: str) -> bool:
    """
    Evaluates the WHERE clause on the given object.

    :param obj: The class instance to evaluate.
    :param where_clause: The WHERE clause string.
    :return: True if the object satisfies the WHERE clause, False otherwise.
    """
    try:
        return eval(where_clause, {}, {"self": obj})
    except Exception as e:
        print(f"Error evaluating where clause: {e}")
        return False


# Function to convert class instances to DataFrame
def class_instances_to_dataframe(instances: List[CCMEntry], fields: List[str]) -> pd.DataFrame:
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


# Main query function
def query_classes(query: str, classes: Dict[str, List[CCMEntry]], return_as_dataframe: bool = True) -> Union[pd.DataFrame, Dict[str, List[CCMEntry]]]:
    """
    Executes a SQL-like query on the given classes.

    :param query: The SQL-like query string.
    :param classes: A dictionary of class names to lists of class instances.
    :param return_as_dataframe: Flag to determine the return type. If True, returns a DataFrame; otherwise, returns a dictionary of lists of objects.
    :return: The resulting DataFrame or dictionary of lists of objects.
    """
    parsed_query = parse_query(query)
    select_fields: List[str] = parsed_query['select']
    from_class: str = parsed_query['from']
    where_clause: str = parsed_query['where']

    if from_class not in classes:
        raise ValueError(f"Class {from_class} not found")

    class_instances: List['CCMEntry'] = classes[from_class]
    filtered_instances: List['CCMEntry'] = [instance for instance in class_instances if evaluate_where_clause(instance, where_clause)]

    if return_as_dataframe:
        return class_instances_to_dataframe(filtered_instances, select_fields)
    else:
        result = {from_class: filtered_instances}
        return result
