import re
import sys
import traceback
import typing
from typing import List, Dict, Union, Optional
import pandas as pd
import ast
import operator as op

import src
from src.utils.table_utils import create_extended_table
from src.utils.types import CCMEntry

# Define a dictionary of safe operators for the eval function replacement
SAFE_OPERATORS = {
    ast.Add: op.add,
    ast.Sub: op.sub,
    ast.Mult: op.mul,
    ast.Div: op.truediv,
    ast.Mod: op.mod,
    ast.Pow: op.pow,
    ast.BitXor: op.xor,
    ast.USub: op.neg,
    ast.Eq: op.eq,
    ast.NotEq: op.ne,
    ast.Lt: op.lt,
    ast.LtE: op.le,
    ast.Gt: op.gt,
    ast.GtE: op.ge,
    ast.And: op.and_,
    ast.Or: op.or_,
    ast.In: op.contains,
}


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


def evaluate_expr(node: ast.AST, context: Dict[str, typing.Any]) -> typing.Any:
    """
    Evaluates an AST node in a given context.

    :param node: The AST node to evaluate.
    :param context: The context in which to evaluate the node.
    :return: The result of the evaluation.
    """

    if isinstance(node, ast.Expression):
        return evaluate_expr(node.body, context)
    elif isinstance(node, ast.BoolOp):
        left = evaluate_expr(node.values[0], context)
        for value in node.values[1:]:
            right = evaluate_expr(value, context)
            if isinstance(node.op, ast.And):
                left = left and right
            elif isinstance(node.op, ast.Or):
                left = left or right
        return left
    elif isinstance(node, ast.Compare):
        left = evaluate_expr(node.left, context)
        for op_, comparator in zip(node.ops, node.comparators):
            right = evaluate_expr(comparator, context)
            operator_func = SAFE_OPERATORS[type(op_)]
            if not operator_func(left, right):
                return False
        return True
    elif isinstance(node, ast.BinOp):
        left = evaluate_expr(node.left, context)
        right = evaluate_expr(node.right, context)
        return SAFE_OPERATORS[type(node.op)](left, right)
    elif isinstance(node, ast.UnaryOp):
        operand = evaluate_expr(node.operand, context)
        return SAFE_OPERATORS[type(node.op)](operand)
    elif isinstance(node, ast.Name):
        return context[node.id]
    elif isinstance(node, ast.Attribute):
        value = evaluate_expr(node.value, context)
        return getattr(value, node.attr)
    elif isinstance(node, ast.Constant):
        return node.value
    else:
        raise TypeError(f"Unsupported AST node type: {type(node)}")


def evaluate_where_clause(obj_dict: Dict[str, typing.Any], where_clause: str) -> List[Dict[str, CCMEntry]]:
    """
    Evaluates the WHERE clause on the given objects.

    :param obj_dict: A dictionary with class names as keys and class instances as values.
    :param where_clause: The WHERE clause string.
    :return: A list of dictionaries with the filtered results.
    """
    # Replace SQL-like operators with Python logical operators
    where_clause = (where_clause
                    .replace('=', '==')
                    .replace(' AND ', ' and ')
                    .replace(' OR ', ' or '))

    replace_map: Dict[str, str] = {
        "Event": "e",
        "Object": "o",
        "DataSource": "ds",
        "InformationSystem": "is",
        "IoTDevice": "id",
        "Observation": "ob",
        "Activity": "a",
        "Attribute": "attr",

    }

    for class_name, instance in obj_dict.items():
        where_clause = where_clause.replace(class_name, replace_map[class_name])

    results: List[Dict[str, CCMEntry]] = []

    for event in obj_dict['Event']:
        temp_event_context: "src.classes_.Event" = event
        obs: List["src.classes_.Object"] = event.related_objects
        temp_data_source_context: "src.classes_.DataSource" = event.data_source

        for ob in obs:
            temp_object_context: "src.classes_.Object" = ob

            context = {
                "e": temp_event_context,
                "o": temp_object_context,
                "ds": temp_data_source_context
            }

            try:
                expr_ast = ast.parse(where_clause, mode='eval')
                eval_result = evaluate_expr(expr_ast, context)

                if eval_result:
                    results.append(
                        {
                            "event": temp_event_context,
                            "object": temp_object_context,
                            "ds": temp_data_source_context
                        }
                    )
            except Exception as e:
                print("##################################################")
                print(f"Error evaluating where clause: {e}, {where_clause} -> {obj_dict}")
                traceback.print_exc(file=sys.stdout)
                print("##################################################")
    return results


def query_classes(
        query: str,
        classes: Dict[str, List['CCMEntry']],
        return_format: typing.Literal["class_reference", "extended_table"] = "extended_table") -> Union[
    pd.DataFrame, Dict[str, List['CCMEntry']]
]:
    """
    Executes a SQL-like query on the given classes.

    :param return_format: The format to return the result in.
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

    filtered_instances: List[dict[str, CCMEntry]] = evaluate_where_clause(classes, where_clause)

    if return_format == 'class_reference':
        return {from_class: filtered_instances}
    elif return_format == 'extended_table':

        extended_table: pd.DataFrame = create_extended_table(
            event_log=[instance['event'] for instance in filtered_instances if 'event' in instance],
            objects=[instance['object'] for instance in filtered_instances if 'object' in instance],
            data_sources=[instance['ds'] for instance in filtered_instances if 'ds' in instance]
        )

        if select_fields and select_fields[0] != '*':
            select_fields = [f"ccm:{field}" for field in select_fields]
            extended_table = extended_table[select_fields]

        return extended_table
    else:
        raise ValueError(f"Unsupported return format: {return_format}")
