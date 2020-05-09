from pandas.io.json import json_normalize
import pandas as pd
from flatten_json import flatten
from sqlalchemy import create_engine

def extract_tasks(workflowresult):

    tasks = {}
    for step in workflowresult:
        step_class = workflowresult[step]['class']
        if not tasks.get(step_class):
            tasks[step_class] = []
        
        tasks[step_class].append(workflowresult[step])
    return tasks

def extract_dataframes(workflowresult):
    tasks = extract_tasks(workflowresult)
    dfs = {}
    for task in tasks:

        dict_flattened = (flatten(step, '.') for step in tasks[task])
        dfs[task] = pd.DataFrame(dict_flattened)
    return dfs

def send_workflowresult_to_sql(workflowresult):
    db_string = 'postgres://maxradin:@localhost:5433/hydrogen_qe'
    engine = create_engine(db_string)
    dfs = extract_dataframes(workflowresult)
    for task_class in dfs:
        dfs[task_class].to_sql(task_class, con=engine)
