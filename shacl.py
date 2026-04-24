from typing import Optional
from dataclasses import dataclass
from string import Template

import pyshacl
import rdflib
from rdflib.plugins.stores import sparqlstore

from helpers import generate_uuid
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_bool
from context_query import query, update

from constants import DATA_GRAPH, SHACL_VALIDATION_OPERATION, SHACL_VALIDATION_INPUT_URI_PREFIX, MU_SPARQL_ENDPOINT, SHACL_VALIDATION_RESULT_URI_PREFIX, SHACL_VALIDATION_RESULT_GRAPH_URI_PREFIX
from utils import from_binding, store_graph
import task_runner

@dataclass
class ValidationInput:
    shacl_graph: str
    data_graph: str

@dataclass
class ValidationResult:
    success: bool
    result_graph: str
    result_text: str

def run_shacl_validation_task(task):
    input = get_input(task.input, DATA_GRAPH)
    if not input:
        raise Exception(f"Input {task.input} not found!")

    store = sparqlstore.SPARQLStore(MU_SPARQL_ENDPOINT)
    # shacl_graph = rdflib.Graph(store, identifier=rdflib.URIRef(input.shacl_graph))
    data_graph = rdflib.Graph(store, identifier=rdflib.URIRef(input.data_graph))

    # TODO: this is an ugly workaround for:
    # https://github.com/RDFLib/pySHACL/blob/master/pyshacl/shapes_graph.py#L65
    # def ignore(*args, **kwargs):
        # pass
    # shacl_graph.add = ignore

    shacl_graph = rdflib.Graph()
    shacl_graph.parse(
        "https://raw.githubusercontent.com/mobilityDCAT-AP/mobilityDCAT-AP/refs/heads/gh-pages/releases/1.1.0/shaclShapes/mobilitydcat-ap_shacl_shapes.ttl"
    )

    # print(shacl_graph.serialize(format='ttl'))

    (conforms, result_graph, result_text) = pyshacl.validate(
        data_graph=data_graph,
        shacl_graph=shacl_graph
    )

    result_graph = result_graph.skolemize(authority='http://mu.semte.ch', basepath='.well-known/genid/pyshacl-validation-result/')

    result_graph_uuid = generate_uuid()
    result_graph_uri = SHACL_VALIDATION_RESULT_GRAPH_URI_PREFIX + result_graph_uuid

    print(result_text)

    result = ValidationResult(
        success=conforms,
        result_graph=result_graph_uri,
        result_text=result_text
    )

    result_uri = save_result(result, task.input)

    # Store the graph after we stored the result so we don't lose track of which graph belongs to which result
    store_graph(result_graph, result_graph_uri)

    return result_uri

task_runner.register(SHACL_VALIDATION_OPERATION, run_shacl_validation_task)

def get_input(input_uri, graph=None) -> Optional[ValidationInput]:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?shacl_graph ?data_graph WHERE {
    GRAPH $graph {
        $input_uri a ext:DCATValidationRequest ;
            ext:shaclGraph ?shacl_graph ;
            ext:dataGraph ?data_graph .
    }
}
LIMIT 1
    """)

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph) if graph else "?g",
        input_uri=sparql_escape_uri(input_uri)
    )
    query_res = query(query_string)

    if not query_res["results"]["bindings"]:
        return None

    return from_binding(ValidationInput, query_res["results"]["bindings"][0])

def save_input(input: ValidationInput, graph=DATA_GRAPH):
    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

INSERT DATA {
    GRAPH $graph {
        $input_uri a ext:DCATValidationRequest ;
            mu:uuid $uuid ;
            ext:shaclGraph $shacl_graph ;
            ext:dataGraph $data_graph .
    }
}
    """)

    uuid = generate_uuid()
    input_uri = SHACL_VALIDATION_INPUT_URI_PREFIX + uuid

    query = query_template.substitute(
        graph=sparql_escape_uri(graph),
        input_uri=sparql_escape_uri(input_uri),
        uuid=sparql_escape_string(uuid),
        shacl_graph=sparql_escape_uri(input.shacl_graph),
        data_graph=sparql_escape_uri(input.data_graph)
    )

    update(query)

    return input_uri

def save_result(result: ValidationResult, input, graph=DATA_GRAPH):
    uuid = generate_uuid()
    uri = SHACL_VALIDATION_RESULT_URI_PREFIX + uuid

    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

INSERT DATA {
    GRAPH $graph {
        $result_uri a ext:ShaclValidationResult ;
            mu:uuid $uuid ;
            # TODO: There is probably an existing vocab that covers this?
            ext:validated $input ;
            ext:validationSuccess $success ;
            ext:resultGraph $result_graph ;
            ext:resultText $result_text .
    }
}
    """)

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        result_uri=sparql_escape_uri(uri),
        input=sparql_escape_uri(input),
        uuid=sparql_escape_string(uuid),
        success=sparql_escape_bool(result.success),
        result_graph=sparql_escape_uri(result.result_graph),
        result_text=sparql_escape_string(result.result_text)
    )

    update(query_string)

    return uri

