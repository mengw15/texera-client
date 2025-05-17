import json
from typing import Dict, List


def convertWorkflowContentToLogicalPlan(workflowRawContent: str) -> dict:
    '''
    Convert the workflow content string to a logical plan dict.

    The difference between the workflowRawContent and the logical plan is that the raw content contains some lower-level features and info, whereas the logical plan does not.

    This function expands operator properties from the workflow_dict to the outside, directly under the operator level.

    :param workflowRawContent: a str containing the raw workflow content.
    :return: a dict representing the logical plan.
    '''

    # Parse the raw content into a Python dictionary
    workflow_dict = json.loads(workflowRawContent)

    # Initialize the logical plan dictionary
    logical_plan = {
        "operators": [],
        "links": [],
        "opsToReuseResult": [],
        "opsToViewResult": []
    }

    # Utility functions
    def get_input_port_ordinal(operatorID: str, inputPortID: str) -> int:
        operator = next(op for op in workflow_dict["operators"] if op["operatorID"] == operatorID)
        return next(i for i, port in enumerate(operator["inputPorts"]) if port["portID"] == inputPortID)

    def get_output_port_ordinal(operatorID: str, outputPortID: str) -> int:
        operator = next(op for op in workflow_dict["operators"] if op["operatorID"] == operatorID)
        return next(i for i, port in enumerate(operator["outputPorts"]) if port["portID"] == outputPortID)

    # Convert operators
    for operator in workflow_dict.get("operators", []):
        # Flatten operatorProperties and merge into the top level
        new_operator = {
            **operator["operatorProperties"],
            "operatorID": operator["operatorID"],
            "operatorType": operator["operatorType"],
            "inputPorts": operator["inputPorts"],
            "outputPorts": operator["outputPorts"],
        }
        logical_plan["operators"].append(new_operator)

    # Convert links
    for link in workflow_dict.get("links", []):
        output_port_idx = get_output_port_ordinal(link["source"]["operatorID"], link["source"]["portID"])
        input_port_idx = get_input_port_ordinal(link["target"]["operatorID"], link["target"]["portID"])

        new_link = {
            "fromOpId": link["source"]["operatorID"],
            "fromPortId": {"id": output_port_idx, "internal": False},
            "toOpId": link["target"]["operatorID"],
            "toPortId": {"id": input_port_idx, "internal": False}
        }
        logical_plan["links"].append(new_link)

    # Convert opsToReuseResult and opsToViewResult
    operator_ids = set(op["operatorID"] for op in workflow_dict.get("operators", []))
    logical_plan["opsToViewResult"] = list(operator_ids.intersection(workflow_dict.get("opsToViewResult", [])))
    logical_plan["opsToReuseResult"] = list(operator_ids.intersection(workflow_dict.get("opsToReuseResult", [])))

    return logical_plan
