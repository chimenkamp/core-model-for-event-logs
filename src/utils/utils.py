from typing import List, Union
import pandas as pd
import re

def parse_query(query: str) -> dict:
    select_pattern = r'SELECT\s+(.*?)\s+FROM'
    from_pattern = r'FROM\s+(\w+)'
    where_pattern = r'WHERE\s+(.*)'

    select_match = re.search(select_pattern, query, re.IGNORECASE)
    from_match = re.search(from_pattern, query, re.IGNORECASE)
    where_match = re.search(where_pattern, query, re.IGNORECASE)

    select_fields = select_match.group(1).split(',') if select_match else []
    from_class = from_match.group(1) if from_match else ''
    where_clause = where_match.group(1) if where_match else ''

    return {
        'select': [field.strip() for field in select_fields],
        'from': from_class,
        'where': where_clause
    }


# Function to evaluate the WHERE clause
def evaluate_where_clause(obj: 'CCMClass', where_clause: str) -> bool:
    try:
        return eval(where_clause, {}, {"self": obj})
    except Exception as e:
        print(f"Error evaluating where clause: {e}")
        return False


# Function to convert class instances to DataFrame
def class_instances_to_dataframe(instances: List['CCMClass'], fields: List[str]) -> pd.DataFrame:
    data = []
    for instance in instances:
        serialized = instance.serialize()
        row = {field: serialized.get(field, None) for field in fields}
        data.append(row)
    return pd.DataFrame(data)


# Main query function
def query_classes(query: str, classes: dict) -> Union[pd.DataFrame, dict]:
    parsed_query = parse_query(query)
    select_fields = parsed_query['select']
    from_class = parsed_query['from']
    where_clause = parsed_query['where']

    if from_class not in classes:
        raise ValueError(f"Class {from_class} not found")

    class_instances = classes[from_class]
    filtered_instances = [instance for instance in class_instances if evaluate_where_clause(instance, where_clause)]

    result_df = class_instances_to_dataframe(filtered_instances, select_fields)

    return result_df
