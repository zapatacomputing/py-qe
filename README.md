# py-qe

This python module allows you to transform Quantum Engine workflow results into several representations:
* Pandas dataframes
* Excel files (XLSX)
* Comma-separated values (CSV) files
* PostgreSQL

## Installation

Run `pip install -e .`.

## Loading workflow results to pandas
You can use the `extract_dataframes` function to transform a workflow result into a set of pandas dataframes.

```python
import json
from pyqe import extract_dataframes

with open("workflow_result.json") as f:
    workflow_result_dict = json.load(f)

dataframes = extract_dataframes(workflow_result_dict)
```

## Exporting to Excel or CSV
Workflow results can be exported to Excel (XLSX) or CSV using the `convert-workflowresult` command-line interface.

```bash
convert-workflowresult export workflow_result.json --format xlsx
```
or
```bash
convert-workflowresult export workflow_result.json --format csv
```

## Uploading to PostgreSQL
Workflow results can be uploaded to a PostgreSQL database using the `convert-workflowresult` command-line interface.

### Setup a PostgreSQL database
You will need to setup your own PostgreSQL server and create an empty database.
For example this can be done using [Postico](https://eggerapps.at/postico/).

### Configuring your SQL connection
Use the `convert-workflowresult set-config` command to set your SQL connection.
See `convert-workflowresult set-config --help` for details.

### Uploading a workflowresult to SQL connection
Use the `convert-workflowresult upload` command to upload a workflowresult JSON file to the
postgres database.
See `convert-workflowresult upload --help` for details.
