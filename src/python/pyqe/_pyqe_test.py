import unittest
from _pyqe import (
    extract_dataframes,
    get_super_dict,
    get_class_dict,
    send_workflowresult_to_sql,
    extract_lists,
)
import json


class TestPyqe(unittest.TestCase):
    def setUp(self):
        self.workflowresult = {}
        self.workflowresult["step-1"] = {
            "class": "class-A",
            "id": 0,
            "two_dimensional_array": [[1, -1], [2, -2], [3, -3]],
            "scalar": 1,
            "one_dimensional_array": [1, 2, 3],
            "list_of_dicts": [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
        }
        self.workflowresult["step-2"] = {
            "class": "class-B",
            "id": 0,
            "two_dimensional_array": [[1, -1], [2, -2], [3, -3]],
            "scalar": 1,
            "one_dimensional_array": [1, 2, 3],
            "list_of_dicts": [{"a": 1, "b": 2}, {"a": 3, "b": 4}],
        }

    def test_extract_lists(self):

        children = {}
        extract_lists(self.workflowresult["step-1"], children)
        extract_lists(self.workflowresult["step-2"], children)

        print(json.dumps(children, indent=2))

        self.assertEqual(len(children["two_dimensional_array"]), 12)
        self.assertFalse(self.workflowresult["step-1"].get("two_dimensional_array"))
        self.assertFalse(self.workflowresult["step-2"].get("two_dimensional_array"))

        self.assertEqual(len(children["one_dimensional_array"]), 6)
        self.assertFalse(self.workflowresult["step-1"].get("one_dimensional_array"))
        self.assertFalse(self.workflowresult["step-2"].get("one_dimensional_array"))

        self.assertEqual(self.workflowresult["step-1"]["scalar"], 1)
        self.assertEqual(self.workflowresult["step-2"]["scalar"], 1)

        self.assertEqual(len(children["list_of_dicts"]), 4)
        self.assertFalse(self.workflowresult["step-1"].get("list_of_dicts"))
        self.assertFalse(self.workflowresult["step-2"].get("list_of_dicts"))

    def test_get_class_dict(self):
        class_dict = get_class_dict(self.workflowresult)
        self.assertEqual(len(class_dict), 2)
        self.assertIn("class-A", class_dict)
        self.assertIn("class-B", class_dict)

    def test_get_super_dict(self):
        super_dict = get_super_dict(self.workflowresult)
        self.assertEqual(len(super_dict), 5)
        for key in (
            "class-A",
            "class-B",
            "one_dimensional_array",
            "two_dimensional_array",
            "list_of_dicts",
        ):
            self.assertIn(key, super_dict)

    def test_extract_dataframes(self):
        dataframes = extract_dataframes(self.workflowresult)
        self.assertEqual(len(dataframes), 5)
        self.assertEqual(len(dataframes["class-A"].index), 1)
        self.assertEqual(len(dataframes["class-B"].index), 1)
        self.assertEqual(len(dataframes["one_dimensional_array"].index), 6)
        self.assertEqual(len(dataframes["two_dimensional_array"].index), 12)
        self.assertEqual(len(dataframes["list_of_dicts"].index), 4)

    def test_ci_skip_send_workflowresult_to_sql(self):
        # This test requires you to have configured the SQL backend.
        send_workflowresult_to_sql(self.workflowresult)

    def test_output_artifacts(self):

        task_data = {
            "class": "get-expectation-values-from-rdms",
            "expectations": {
                "covariances": [{"id": "qe"}, {"id": "we"}],
                "id": "h2-nrepr-purif-v2-9xhzg-2941914382/expectations",
                "expectation_values": {
                    "real": [-0.889743677639184, -0.8897353171358477,]
                },
                "schema": "io-zapOS-v1alpha1-expectation_values",
                "taskClass": "get-expectation-values-from-rdms",
                "taskId": "h2-nrepr-purif-v2-9xhzg-2941914382",
                "workflowId": "h2-nrepr-purif-v2-9xhzg",
            },
            "id": "h2-nrepr-purif-v2-9xhzg-2941914382",
        }
        children = {}
        extract_lists(task_data, children)

        print(json.dumps(children, indent=2))

        self.assertEqual(len(task_data), 2)
        self.assertEqual(len(children), 3)

        self.assertEqual(
            len(
                children["io-zapOS-v1alpha1-expectation_values.expectation_values.real"]
            ),
            2,
        )
        self.assertFalse(task_data.get("expectations"))

        self.assertEqual(
            len(children["io-zapOS-v1alpha1-expectation_values.covariances"]), 2
        )


if __name__ == "__main__":
    unittest.main()

#

# with open('/Users/maxradin/science/vqe-tutorial-prebaked/49e36f22-9295-5488-af33-cabe24760799.json') as f:
#     workflowresult = json.load(f)
# tasks = extract_tasks(workflowresult)
# print(tasks)

# dfs = extract_dataframes(workflowresult)
# print(dfs)

# send_workflowresult_to_sql(workflowresult)
# send_workflowresult_to_sql(workflowresult, csv=True)
# send_workflowresult_to_sql(workflowresult, excel=True)
