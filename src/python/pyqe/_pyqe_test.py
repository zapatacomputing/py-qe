import unittest
from _pyqe import (extract_dataframes,
    send_workflowresult_to_sql, extract_lists)
import json


class TestPyqe(unittest.TestCase):

    def test_(self):
        task_data_1 = {
            "id" : 1,
            "two_dimensional_array" : [[1, -1], [2, -2], [3, -3]],
            "scalar" : 1,
            "one_dimensional_array" : [1, 2, 3],
            "list_of_dicts" : [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }
        task_data_2 = {
            "id" : 2,
            "two_dimensional_array" : [[1, -1], [2, -2], [3, -3]],
            "scalar" : 1,
            "one_dimensional_array" : [1, 2, 3],
            "list_of_dicts" : [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        }

        children = {}
        extract_lists(task_data_1, children)
        extract_lists(task_data_2, children)

        print(json.dumps(children, indent=2))

        self.assertEqual(len(children['two_dimensional_array']), 12)
        self.assertFalse(task_data_1.get('two_dimensional_array'))
        self.assertFalse(task_data_2.get('two_dimensional_array'))

        self.assertEqual(len(children['one_dimensional_array']), 6)
        self.assertFalse(task_data_1.get('one_dimensional_array'))
        self.assertFalse(task_data_2.get('one_dimensional_array'))

        self.assertEqual(task_data_1['scalar'], 1)
        self.assertEqual(task_data_2['scalar'], 1)

        self.assertEqual(len(children['list_of_dicts']), 4)
        self.assertFalse(task_data_1.get('list_of_dicts'))
        self.assertFalse(task_data_2.get('list_of_dicts'))

    def test_output_artifacts(self):

        task_data = {
                "class": "get-expectation-values-from-rdms",
                "expectations": {
                    "covariances": [ {"id": "qe"}, {"id": "we"} ],
                    "id": "h2-nrepr-purif-v2-9xhzg-2941914382/expectations",
                    "expectation_values": {
                        "real": [
                            -0.889743677639184,
                            -0.8897353171358477,
                        ]
                    },
                    "schema": "io-zapOS-v1alpha1-expectation_values",
                    "taskClass": "get-expectation-values-from-rdms",
                    "taskId": "h2-nrepr-purif-v2-9xhzg-2941914382",
                    "workflowId": "h2-nrepr-purif-v2-9xhzg"
                },
                "id": "h2-nrepr-purif-v2-9xhzg-2941914382",
            }
        children = {}
        extract_lists(task_data, children)

        print(json.dumps(children, indent=2))

        self.assertEqual(len(task_data), 2)
        self.assertEqual(len(children), 3)

        self.assertEqual(len(children['io-zapOS-v1alpha1-expectation_values.expectation_values.real']), 2)
        self.assertFalse(task_data.get('expectations'))

        self.assertEqual(len(children['io-zapOS-v1alpha1-expectation_values.covariances']), 2)

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
