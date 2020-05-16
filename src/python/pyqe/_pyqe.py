"""Utilities for flattening Quantum Engine workflowresult objects."""

from pandas.io.json import json_normalize
import pandas as pd
from flatten_json import flatten
from sqlalchemy import create_engine
import json
from pyqe._sql import get_db_conn_str

def extract_lists(item, dataset, path=None, index_buffer=(), parent_id=None):
    """Recursively extract lists from a dict and add them to a separate dict.

    Args:
        item (Object): The item to recursively extract lists from. 
        dataset (dict): The dict to add list items to. Each type of list item
            will be appended to a separate key.
        path (str): The path to `item` within the outermost dict. For use in
            recursive calls.
        index_buffer (tuple): If `item` was wrapped in one or more lists, the
            indices of `item` in each list. For use in recursive calls.
        parent_id (str): The value to assign to the `parentId` field of children
            if `item` is a (nested). Note that `parentId` will not be added if
            the children are dicts because DCS already assigns `parentId` to
            lists of dicts.
    """
    if isinstance(item, dict):
        if item.get('id'):
            parent_id = item['id']
        
        # If the item is a dict that is an element in a list, it should be
        # extracted into a table corresponding to its path.
        if len(index_buffer) > 0:
            dataset[path].append(item)

        # Recursively extract lists from all of the item's values.
        to_pop = []
        for key in item:
            schema = None
            path_addition = key
            if isinstance(item[key], dict) and item[key].get('schema'):
                schema = item[key]['schema']
                path_addition = schema
            if path:
                new_path = path + '.' + path_addition
            else:
                new_path = path_addition
            extract_lists(item[key], dataset, new_path, (), parent_id)
            if isinstance(item[key], list):
                to_pop.append(key)
            elif schema is not None:
                if not dataset.get(schema):
                    dataset[schema] = []
                dataset[schema].append(item[key])
                to_pop.append(key)
        
        # Remove any keys whose values were lists or output artifacts because those
        # have been extracted.
        for key in to_pop:
                item.pop(key)

    # If the item is a list, recursively extract lists on each child.
    elif isinstance(item, list):
        if not dataset.get(path):
            dataset[path] = []
        for index, child in enumerate(item):
            extract_lists(child, dataset, path, index_buffer + (index,), parent_id)

    # If the item is neither a list nor a dict, but it an element in a list, it
    # should be moved into a table given by its path.
    elif len(index_buffer) > 0:
        item_as_dict = {'value': item}
        item_as_dict.update({f'index_{i}': j for i, j in enumerate(index_buffer)})     
        item_as_dict['parentId'] = parent_id
        dataset[path].append(item_as_dict)

def get_class_dict(workflowresult):
    """Given a Quantum Engine workflowresult dict, group all steps according to
    their task class.

    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    
    Returns:
        dict: A dict whose keys are the task classes, and values are lists of
            steps.
    """

    class_dict = {}

    for step in workflowresult:
        step_class = workflowresult[step]['class']
        if not class_dict.get(step_class):
            class_dict[step_class] = []
        class_dict[step_class].append(workflowresult[step])
    return class_dict

def get_super_dict(workflowresult):
    """
    Given a Quantum Engine workflowresult dict, flatten in into a "super" dict.

    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    
    Returns:
        dict: A dict that has a key for each task class that maps to a list of
            steps, and a key for each type of list element that maps to list of
            those elements.
    """

    # While we could just directly extract lists from the workflowresult
    # dict, this would have the undesirable effect of including the parent task
    # class in the key for each list element type. By looping over each step and
    # extracting lists from them one by one, this allows us to avoid this
    # problem.

    super_dict = get_class_dict(workflowresult)
    children = {}
    for class_name in super_dict:
        for step in super_dict[class_name]:
            extract_lists(step, children)
    super_dict.update(children)
    return super_dict

def extract_dataframes(workflowresult):
    """
    Given a Quantum Engine workflowresult dict, flatten in into pandas
    dataframes.

    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    
    Returns:
        dict: A dict that has a key for each task class that maps to a
            pandas.DataFrame object, and a key for each type of list element
            that maps to a pandas.DataFrame object.
    """

    super_dict = get_super_dict(workflowresult)
    dfs = {}
    for table_name in super_dict:
        dict_flattened = list((flatten(item, '.') for item in super_dict[table_name]))

        for item in dict_flattened:
            to_pop = []
            for key in item:
                if isinstance(item[key], dict):
                    if len(item[key]) == 0:
                        to_pop.append(key)
            for key in to_pop:
                item.pop(key)
        dfs[table_name] = pd.DataFrame(dict_flattened)
        # Esthetic changes imitating MongoDB behaviour
        if 'id' in dfs[table_name].columns:
            dfs[table_name].rename(columns={'id': '_id'}, inplace=True)
            dfs[table_name].set_index('_id', inplace=True)
        else:
            dfs[table_name].index.name = '_id'

    return dfs

def send_workflowresult_to_sql(workflowresult):
    """Given a Quantum Engine workflowresult dict, flatten it and upload to a
    SQL database.

    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    """

    engine = create_engine(get_db_conn_str())
    dfs = extract_dataframes(workflowresult)
    for table_name in dfs:
        dfs[table_name].to_sql(table_name, con=engine, if_exists='append')
