from string import Template
from utils import listize
import traceback
from helpers import logger, update, generate_uuid
from escape_helpers import sparql_escape_uri, sparql_escape_string

from constants import TASKS_GRAPH, CONTAINER_URI_PREFIX, MU_APPLICATION_GRAPH
from task import find_actionable_task_of_types, TaskStatus

_runners = {}

def register(operation_uri, runner):
    _runners[operation_uri] = runner

def run_task(task):
    try:
        runner = _runners[task.operation]
    except KeyError:
        raise RuntimeError(f"Could not find a runner for operation: {task.operation}")

    try:
        task.update_status(TaskStatus.BUSY)
        results = runner(task)
        attach_task_results_container(task, results)
        task.update_status(TaskStatus.SUCCESS)
    except Exception as e:
        print(f"Failed to run task <{task.uri}> with operation <{task.operation}>:")
        traceback.print_exc()
        task.update_status(TaskStatus.FAILED)

def run_tasks():

    while True:

        actionable_task = find_actionable_task_of_types(
            _runners.keys(),
            TASKS_GRAPH
        )

        if not actionable_task:
            logger.debug("No more tasks found")
            return

        try:
            run_task(actionable_task)
        except:
            logger.warn(
                f"Problem while running task {actionable_task.uri}, operation {actionable_task.operation}"
            )

def attach_task_results_container(task, results, graph=MU_APPLICATION_GRAPH):
    container_uuid = generate_uuid()
    container_uri = CONTAINER_URI_PREFIX + container_uuid

    container_query_template = Template("""
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

INSERT {
    GRAPH $graph {
        $task task:resultsContainer $container .
        $container a nfo:DataContainer ;
            mu:uuid $container_uuid ;
            ext:content $results .
    }
}
WHERE {
    GRAPH $graph {
        $task a task:Task .
    }
}""")
    container_query_string = container_query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        task=sparql_escape_uri(task.uri),
        container_uuid=sparql_escape_string(container_uuid),
        container=sparql_escape_uri(container_uri),
        results=", ".join([sparql_escape_uri(result) for result in listize(results)])
    )

    update(container_query_string)
