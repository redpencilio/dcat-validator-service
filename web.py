from task import Task
from flask import request

import shacl
from constants import SHACL_VALIDATION_OPERATION, SHACL_VALIDATION_JOB_OPERATION

# SAMPLE_SHACL = "https://raw.githubusercontent.com/mobilityDCAT-AP/mobilityDCAT-AP/refs/heads/gh-pages/releases/1.1.0/shaclShapes/mobilitydcat-ap_shacl_shapes.ttl"
SAMPLE_SHACL = [
    "https://raw.githubusercontent.com/mobilityDCAT-AP/validation/refs/heads/main/shacl/mobilitydcat-ap-shacl.ttl",
    "https://raw.githubusercontent.com/mobilityDCAT-AP/validation/refs/heads/main/shacl/mobilitydcat-ap-shacl-ranges.ttl",
]
# SAMPLE_DATA = "https://raw.githubusercontent.com/mobilityDCAT-AP/validation/refs/heads/main/sample_data/baseline-dcat-ap/positives/B-P-03-full-minimal-chain.ttl"
SAMPLE_DATA = "https://raw.githubusercontent.com/mobilityDCAT-AP/validation/refs/heads/main/sample_data/mobility/positives/M-P-04-full-mobility-catalog.ttl"
SAMPLE_DATA = "https://raw.githubusercontent.com/mobilityDCAT-AP/validation/refs/heads/main/sample_data/mobility/positives/M-P-01-mandatory-homepage.ttl"

@app.route("/validate/")
def validate():
    """Example endpoint to demonstrate the service"""
    import rdflib
    from helpers import generate_uuid
    from utils import store_graph

    data_graph = rdflib.Graph()
    data_graph.parse(SAMPLE_DATA)
    data_graph_name = "http://mu.semte.ch/vocabularies/ext/data-graph/" + generate_uuid()
    shacl_graph = rdflib.Graph()
    shacl_graph.parse(SAMPLE_SHACL[0])
    shacl_graph_name = "http://mu.semte.ch/vocabularies/ext/shacl-graph/" + generate_uuid()

    store_graph(data_graph, data_graph_name)
    store_graph(shacl_graph, shacl_graph_name)

    input = shacl.ValidationInput(
        shacl_graph=shacl_graph_name,
        data_graph=data_graph_name
    )

    input_uri = shacl.save_input(input)

    task = Task(
        input=input_uri,
        operation=SHACL_VALIDATION_OPERATION,
        job_operation=SHACL_VALIDATION_JOB_OPERATION
    )

    # shacl.run_shacl_validation_task(task)

    task.insert(SHACL_VALIDATION_JOB_OPERATION)

    from task_runner import run_tasks

    run_tasks()

    return {
        "success": True,
        "meta": {
            "input_uri": input_uri,
            "task_uri": task.uri
        }
    }


@app.route("/jobs-delta")
def process_jobs_delta():
    from task_runner import run_tasks
    import threading

    inserts = request.json[0]["inserts"]
    has_tasks = any(
        t
        for t in inserts
        if t["predicate"]["value"] == "http://www.w3.org/ns/adms#status"
        and t["object"]["value"]
        == "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
    )
    if not has_tasks:
        return "Can't do anything with this delta. Skipping.", 500

    thread = threading.Thread(target=run_tasks)
    thread.start()

    return "", 200
