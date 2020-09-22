"""Utilities for flattening Quantum Engine workflowresult objects."""

from pandas.io.json import json_normalize
import pandas as pd
from flatten_json import flatten
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.exc import ProgrammingError
import json
from pyqe._sql import get_db_conn_str
import os
import re


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
        if item.get("id"):
            parent_id = item["id"]

        # If the item is a dict that is an element in a list, it should be
        # extracted into a table corresponding to its path.
        if len(index_buffer) > 0:
            dataset[path].append(item)

        # Recursively extract lists from all of the item's values.
        to_pop = []
        for key in item:
            schema = None
            path_addition = key
            if isinstance(item[key], dict) and item[key].get("schema"):
                schema = item[key]["schema"]
                path_addition = schema
            if path:
                new_path = path + "." + path_addition
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
        item_as_dict = {"value": item}
        item_as_dict.update({f"index_{i}": j for i, j in enumerate(index_buffer)})
        item_as_dict["parentId"] = parent_id
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
        step_class = workflowresult[step]["class"]
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
        dict_flattened = list((flatten(item, ".") for item in super_dict[table_name]))

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
        if "id" in dfs[table_name].columns:
            dfs[table_name].rename(columns={"id": "_id"}, inplace=True)
            dfs[table_name].set_index("_id", inplace=True)
        else:
            dfs[table_name].index.name = "_id"

    return dfs


def _compress_name(original_name: str, length: int) -> str:
    """Compresses a table name to less than length characters so that it is 
    suitable for Excel/Postgres limitations.

    Args:
        original_name (str): The original table name to compress
        length (int): The maximum length of a name
    Returns:
        compressed_name (str): A new name that fits the length
    """

    if len(original_name) > length:
        compressed_name = re.sub("zapata-v1", "zv1", original_name)
    else:
        return original_name

    if len(compressed_name) > length:
        compressed_name = re.sub("[aeiouy]", "", compressed_name)
    if len(compressed_name) > length:
        compressed_name = compressed_name[:length]

    return compressed_name

def export_to_csv(workflowresult):
    """Given a Quantum Engine workflowresult dict, unflatten it and 
    write to csv files, using one file per table.
                      
    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    """

    dfs = extract_dataframes(workflowresult)
    if not os.path.isdir("./csv_data"):
        os.mkdir("./csv_data")
    for table_name in dfs:
        filepath = "./csv_data/" + table_name + ".csv"
        if os.path.isfile(filepath):
            old_df = pd.read_csv(filepath)
            old_df.set_index("_id", inplace=True)
            df_to_write = old_df.append(dfs[table_name], ignore_index=False)
        else:
            df_to_write = dfs[table_name]

        df_to_write.to_csv(filepath, index=True)
        print(f"Updated {filepath}")    

def export_to_xlsx(workflowresult: dict):
    """Given a Quantum Engine workflowresult dict, unflatten it and 
    write or append results to an Excel file, using one worksheet per table.
                      
    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    """

    dfs = extract_dataframes(workflowresult)

    max_len_excel = 31
    filepath = "./excel_data.xlsx"
    with pd.ExcelWriter(filepath) as writer:
        if os.path.isfile(filepath):
            excel_file = pd.ExcelFile(filepath)
        else:
            excel_file = None
        for table_name in dfs:
            if (
                excel_file is not None
                and _compress_name(table_name, max_len_excel)
                in excel_file.sheet_names
            ):
                old_df = excel_file.parse(_compress_name(table_name, max_len_excel))
                old_df.set_index("_id", inplace=True)
                df_to_write = old_df.append(dfs[table_name], ignore_index=False)
            else:
                df_to_write = dfs[table_name]

            # Excel only allows up to about a million rows...
            if len(df_to_write.index) > 1048576:
                print(
                    "Table {} has more than 1048576 rows and cannot be written to Excel worksheet: it has been dropped.".format(
                        table_name
                    )
                )
            else:
                df_to_write.to_excel(
                    writer,
                    sheet_name=_compress_name(table_name, max_len_excel),
                    index=True,
                )
        # Loop over sheets that are not in the current json
        # if an excel file already exists
        # Ensures they are also written to the new Excel file
        compressed_table_names = [
            _compress_name(x, max_len_excel) for x in dfs.keys()
        ]
        if excel_file:
            for sheet_name in excel_file.sheet_names:
                if sheet_name not in compressed_table_names:
                    transferred_df = excel_file.parse(sheet_name)
                    transferred_df.set_index("_id", inplace=True)
                    transferred_df.to_excel(writer, sheet_name=sheet_name, index=True)

        print(f"Updated {filepath}")

def send_workflowresult_to_sql(workflowresult: dict):
    """Given a Quantum Engine workflowresult dict, flatten it and upload to a
    SQL database.

    Args:
        workflowresult (dict): A Quantum Engine workflowresult dict.
    """
    max_len_postgres = 63

    dfs = extract_dataframes(workflowresult)

    engine = create_engine(get_db_conn_str())
    for table_name in dfs:
        try:
            dfs[table_name].to_sql(
                _compress_name(table_name, max_len_postgres),
                con=engine,
                if_exists="append",
            )
        except ProgrammingError as e:
            if "UndefinedColumn" in e.args[0]:
                # Since several columns may be missing, we iterate through all of them.
                md = MetaData()
                table = Table(
                    _compress_name(table_name, max_len_postgres),
                    md,
                    autoload=True,
                    autoload_with=engine,
                )
                # existing_cols = engine.execute('PRAGMA table_info({})'.format(table_name))
                existing_cols = [x.name for x in table.c]
                found_missing_col = False
                for col in dfs[table_name].columns.values.tolist():
                    if col not in existing_cols:
                        found_missing_col = True
                        col_type = dfs[table_name][col].dtypes.name
                        print(
                            "Adding new column {} to table {}".format(
                                col, _compress_name(table_name, max_len_postgres)
                            )
                        )
                        sql_type = None
                        # Type mapping based on pandas.io.sql.py _SQL_TYPES dictionary
                        if "int" in col_type:
                            sql_type = "INTEGER"
                        elif "float" in col_type:
                            sql_type = "REAL"
                        elif "bool" in col_type:
                            sql_type = "INTEGER"
                        elif "datetime" in col_type:
                            sql_type = "TIMESTAMP"
                        elif "date" in col_type:
                            sql_type = "DATE"
                        elif "time" in col_type:
                            sql_type = "TIME"
                        elif "object" in col_type or "str" in col_type:
                            sql_type = "TEXT"
                        else:
                            print("Defaulting to TEXT for SQL column type")
                            sql_type = "TEXT"

                        engine.execute(
                            'ALTER TABLE "%s" ADD COLUMN "%s" %s'
                            % (
                                _compress_name(table_name, max_len_postgres),
                                col,
                                sql_type,
                            )
                        )

                if not found_missing_col:
                    print("Could not find missing column name")
                    raise e

                # Now try adding the table again
                dfs[table_name].to_sql(
                    _compress_name(table_name, max_len_postgres),
                    con=engine,
                    if_exists="append",
                )
            else:
                raise e

