import unittest
from _pyqe import (extract_dataframes, get_super_dict, get_class_dict,
    send_workflowresult_to_sql, extract_lists)
import json


class TestPyqe(unittest.TestCase):

    def setUp(self):
        self.workflowresult = {}
        self.workflowresult["step-1"] = {
            "class" : "class-A",
            "id" : 0,
            "two_dimensional_array" : [[1, -1], [2, -2], [3, -3]],
            "scalar" : 1,
            "one_dimensional_array" : [1, 2, 3],
            "list_of_dicts" : [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }
        self.workflowresult["step-2"] = {
            "class" : "class-B",
            "id" : 0,
            "two_dimensional_array" : [[1, -1], [2, -2], [3, -3]],
            "scalar" : 1,
            "one_dimensional_array" : [1, 2, 3],
            "list_of_dicts" : [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }

    def test_extract_lists(self):

        children = {}
        extract_lists(self.workflowresult["step-1"], children)
        extract_lists(self.workflowresult["step-2"], children)

        print(json.dumps(children, indent=2))

        self.assertTrue(len(children['two_dimensional_array']), 8)
        self.assertFalse(self.workflowresult["step-1"].get('two_dimensional_array'))
        self.assertFalse(self.workflowresult["step-2"].get('two_dimensional_array'))

        self.assertTrue(len(children['one_dimensional_array']), 6)
        self.assertFalse(self.workflowresult["step-1"].get('one_dimensional_array'))
        self.assertFalse(self.workflowresult["step-2"].get('one_dimensional_array'))

        self.assertEqual(self.workflowresult["step-1"]['scalar'], 1)
        self.assertEqual(self.workflowresult["step-2"]['scalar'], 1)

        self.assertTrue(len(children['list_of_dicts']), 4)
        self.assertFalse(self.workflowresult["step-1"].get('list_of_dicts'))
        self.assertFalse(self.workflowresult["step-2"].get('list_of_dicts'))

    def test_get_class_dict(self):
        class_dict = get_class_dict(self.workflowresult)
        self.assertEqual(len(class_dict), 2)
        self.assertIn('class-A', class_dict)
        self.assertIn('class-B', class_dict)

    def test_get_super_dict(self):
        super_dict = get_super_dict(self.workflowresult)
        self.assertEqual(len(super_dict), 5)
        for key in ('class-A', 'class-B', 'one_dimensional_array',
                'two_dimensional_array', 'list_of_dicts'):
            self.assertIn(key, super_dict)

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
