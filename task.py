from dataclasses import dataclass
from string import Template
import datetime
from typing import Optional
from enum import Enum
import json

from helpers import generate_uuid
from context_query import query, update
from sudo_query import query_sudo, update_sudo
from escape_helpers import sparql_escape_uri, sparql_escape_datetime, sparql_escape_string

from utils import from_binding
from constants import TASKS_GRAPH, CONTAINER_URI_PREFIX, JOB_URI_PREFIX, TASK_URI_PREFIX

class TaskStatus(Enum):
    BUSY = "http://redpencil.data.gift/id/concept/JobStatus/busy"
    SCHEDULED = "http://redpencil.data.gift/id/concept/JobStatus/scheduled"
    SUCCESS = "http://redpencil.data.gift/id/concept/JobStatus/success"
    FAILED = "http://redpencil.data.gift/id/concept/JobStatus/failed"

@dataclass
class Task:
    input: str
    operation: str
    job_operation: str
    headers: Optional[dict] = None

    uri: str = None
    id: str = None
    created: datetime.datetime = None

    def __post_init__(self):
        if not self.created:
            self.created = datetime.datetime.now()
        if not self.id:
            self.id = generate_uuid()
        if not self.uri:
            self.uri = TASK_URI_PREFIX + self.id

        if isinstance(self.headers, str):
            self.load_headers(self.headers)

    def load_headers(self, json_str):
        header_dict = json.loads(json_str)
        assert isinstance(header_dict, dict)
        assert all(isinstance(key, str) for key in header_dict.keys())
        assert all(isinstance(value, str) for value in header_dict.values())

        self.headers = header_dict

    def insert(self, graph=TASKS_GRAPH):
        query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
INSERT DATA {
    GRAPH $graph {
        $container_uri a nfo:DataContainer;
            mu:uuid $container_uuid;
            ext:content $input .
        $job_uri a cogs:Job ;
            mu:uuid $job_uuid;
            dct:created $created;
            dct:modified $created;
            dct:creator "empty";
            task:operation $job_operation;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
        $task_uri a task:Task ;
            $headers
            mu:uuid $task_uuid ;
            dct:created $created ;
            dct:modified $updated ;
            task:index "0";
            dct:isPartOf $job_uri;
            task:inputContainer $container_uri;
            task:operation $operation ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> .
    }
}
  """)
        headers = f"ext:headers {sparql_escape_string(json.dumps(self.headers))} ;\n" if self.headers else ""
        container_uuid = generate_uuid()
        container_uri = CONTAINER_URI_PREFIX + container_uuid
        job_uuid = generate_uuid()
        job_uri = JOB_URI_PREFIX + job_uuid
        updated= datetime.datetime.now()
        query = query_template.substitute(
            graph=sparql_escape_uri(graph),
            container_uri=sparql_escape_uri(container_uri),
            job_uri=sparql_escape_uri(job_uri),
            task_uri=sparql_escape_uri(self.uri),
            container_uuid=sparql_escape_string(container_uuid),
            job_uuid=sparql_escape_string(job_uuid),
            task_uuid=sparql_escape_string(self.id),
            created=sparql_escape_datetime(self.created),
            updated=sparql_escape_datetime(updated),
            input=sparql_escape_uri(self.input),
            operation=sparql_escape_uri(self.operation),
            job_operation=sparql_escape_uri(self.job_operation),
            headers=headers
        )

        return update_sudo(query)


    def update_status(self, status: TaskStatus, graph=TASKS_GRAPH):
        time = datetime.datetime.now()

        query_template = Template("""
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX dct: <http://purl.org/dc/terms/>

DELETE {
    GRAPH $graph {
        $task adms:status ?old_status ;
            dct:modified ?old_modified .
    }
}
INSERT {
    GRAPH $graph {
        $task adms:status $new_status ;
            dct:modified $modified .
    }
}
WHERE {
  GRAPH $graph {
      $task a task:Task ;
            adms:status ?old_status .
      OPTIONAL { $task dct:modified ?old_modified }
  }
}""")
        query_string = query_template.substitute(
            graph=sparql_escape_uri(graph) if graph else "?g",
            task=sparql_escape_uri(self.uri),
            new_status=sparql_escape_uri(status.value),
            modified=sparql_escape_datetime(time),
        )

        update_sudo(query_string)

def find_actionable_task_of_types(type_urls, graph=None) -> Optional[Task]:
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>
PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>
PREFIX adms: <http://www.w3.org/ns/adms#>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT (?task as ?uri) (?uuid as ?id) ?created ?input ?operation ?job_operation ?headers WHERE {
    GRAPH $graph {
        ?task a task:Task ;
            dct:created ?created ;
            adms:status <http://redpencil.data.gift/id/concept/JobStatus/scheduled> ;
            task:operation ?operation ;
            mu:uuid ?uuid .
        OPTIONAL { ?task task:inputContainer/ext:content ?input}
        OPTIONAL { ?task dct:isPartOf/task:operation ?job_operation }
        OPTIONAL { ?task ext:headers ?headers }
        VALUES ?operation {$task_types}
    }
} LIMIT 1
    """)
    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        task_types = " ".join([sparql_escape_uri(uri) for uri in type_urls])
    )
    query_res = query_sudo(query_string)

    if not query_res["results"]["bindings"]:
        return None

    return from_binding(Task, query_res["results"]["bindings"][0])


