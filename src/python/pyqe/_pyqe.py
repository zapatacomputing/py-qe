from pandas.io.json import json_normalize
import pandas as pd
from flatten_json import flatten
from sqlalchemy import create_engine
import json
from pyqe._sql import get_db_conn_str

def extract_lists(item, dataset, path=None, index_buffer=(), parent_id=None):
    if isinstance(item, dict):
        if item.get('id'):
            parent_id = item['id']
        
        # Check if the item is a dict inside of a list
        if len(index_buffer) > 0:
            dataset[path].append(item)

        to_pop = []
        for key in item:
            if path:
                new_path = path + '.' + key
            else:
                new_path = key
            extract_lists(item[key], dataset, new_path, (), parent_id)
            if isinstance(item[key], list):
                to_pop.append(key)
        for key in to_pop:
                item.pop(key)

    elif isinstance(item, list):
        if not dataset.get(path):
            dataset[path] = []
        for index, child in enumerate(item):
            extract_lists(child, dataset, path, index_buffer + (index,), parent_id)

    elif len(index_buffer) > 0:
        item_as_dict = {'value': item}
        item_as_dict.update({f'index_{i}': j for i, j in enumerate(index_buffer)})     
        item_as_dict['parentId'] = parent_id
        dataset[path].append(item_as_dict)

def get_class_dict(workflowresult):
    class_dict = {}

    for step in workflowresult:
        step_class = workflowresult[step]['class']
        if not class_dict.get(step_class):
            class_dict[step_class] = []
        class_dict[step_class].append(workflowresult[step])
    return class_dict

def get_super_dict(workflowresult):
    super_dict = get_class_dict(workflowresult)
    children = {}
    for class_name in super_dict:
        for step in super_dict[class_name]:
            extract_lists(step, children)
    super_dict.update(children)
    return super_dict

def extract_dataframes(workflowresult):
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
    return dfs

def send_workflowresult_to_sql(workflowresult):
    engine = create_engine(get_db_conn_str())
    dfs = extract_dataframes(workflowresult)
    for table_name in dfs:
        dfs[table_name].to_sql(table_name, con=engine, if_exists='append')
