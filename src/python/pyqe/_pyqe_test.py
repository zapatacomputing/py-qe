import unittest
from _pyqe import (extract_tasks, extract_dataframes,
    send_workflowresult_to_sql, extract_lists)
import json


class TestPyqe(unittest.TestCase):

    def test_(self):
        task_data_1 = {
            "id" : 0,
            "two_dimensional_array" : [[1, -1], [2, -2], [3, -3]],
            "scalar" : 1,
            "one_dimensional_array" : [1, 2, 3],
            "list_of_dicts" : [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }
        task_data_2 = {
            "id" : 0,
            "two_dimensional_array" : [[1, -1], [2, -2], [3, -3]],
            "scalar" : 1,
            "one_dimensional_array" : [1, 2, 3],
            "list_of_dicts" : [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }
        children = {}
        extract_lists(task_data_1, children)
        extract_lists(task_data_2, children)

        print(json.dumps(children, indent=2))

        self.assertTrue(len(children['two_dimensional_array']), 8)
        self.assertFalse(task_data_1.get('two_dimensional_array'))
        self.assertFalse(task_data_2.get('two_dimensional_array'))

        self.assertTrue(len(children['one_dimensional_array']), 6)
        self.assertFalse(task_data_1.get('one_dimensional_array'))
        self.assertFalse(task_data_2.get('one_dimensional_array'))

        self.assertEqual(task_data_1['scalar'], 1)
        self.assertEqual(task_data_2['scalar'], 1)

        self.assertTrue(len(children['list_of_dicts']), 4)
        self.assertFalse(task_data_1.get('list_of_dicts'))
        self.assertFalse(task_data_2.get('list_of_dicts'))

if __name__ == '__main__':
    unittest.main()

# 

# with open('/Users/maxradin/science/vqe-tutorial-prebaked/49e36f22-9295-5488-af33-cabe24760799.json') as f:
#     workflowresult = json.load(f)
# tasks = extract_tasks(workflowresult)
# print(tasks)

# dfs = extract_dataframes(workflowresult)
# print(dfs)

# send_workflowresult_to_sql(workflowresult)
