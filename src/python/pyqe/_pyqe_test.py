from _pyqe import extract_tasks, extract_dataframes, send_workflowresult_to_sql
import json



with open('/Users/maxradin/science/vqe-tutorial-prebaked/49e36f22-9295-5488-af33-cabe24760799.json') as f:
    workflowresult = json.load(f)
tasks = extract_tasks(workflowresult)
print(tasks)

dfs = extract_dataframes(workflowresult)
print(dfs)

send_workflowresult_to_sql(workflowresult)
