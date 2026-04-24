from typing import Optional
from dataclasses import dataclass
from string import Template

import pyshacl
import rdflib
# from rdflib.plugins.stores import sparqlstore
import sparql_store

from helpers import generate_uuid
from escape_helpers import sparql_escape_uri, sparql_escape_string, sparql_escape_bool
# from context_query import query, update
from sudo_query import query_sudo as query, update_sudo as update

from constants import (
    DATA_GRAPH, PUBLIC_GRAPH, TASKS_GRAPH,
    SHACL_VALIDATION_OPERATION, SHACL_VALIDATION_INPUT_URI_PREFIX,
    MU_SPARQL_ENDPOINT, SHACL_VALIDATION_RESULT_URI_PREFIX,
    SHACL_VALIDATION_RESULT_GRAPH_URI_PREFIX,
    VALIDATION_SUMMARY_URI_PREFIX, TARGET_CLASS_SUMMARY_URI_PREFIX,
    RULE_SUMMARY_URI_PREFIX, SHACL_REPORT_PREDICATE,
)
from utils import from_binding, store_graph
import task_runner

@dataclass
class ValidationInput:
    # shacl_graph: str
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

    store = sparql_store.SPARQLStore(MU_SPARQL_ENDPOINT, headers={'mu-auth-sudo': 'true'})

    ds = rdflib.Dataset(store)
    data_graph = ds.get_context(rdflib.URIRef(input.data_graph))

    # TODO: this is an ugly workaround for:
    # https://github.com/RDFLib/pySHACL/blob/master/pyshacl/shapes_graph.py#L65
    # def ignore(*args, **kwargs):
        # pass
    # shacl_graph.add = ignore

    shacl_graph = rdflib.Graph()
    shacl_graph.parse(
        "https://raw.githubusercontent.com/mobilityDCAT-AP/mobilityDCAT-AP/refs/heads/gh-pages/releases/1.1.0/shaclShapes/mobilitydcat-ap_shacl_shapes.ttl"
    )

    (conforms, result_graph, result_text) = pyshacl.validate(
        data_graph=data_graph,
        shacl_graph=shacl_graph
    )

    result_graph = result_graph.skolemize(authority='http://mu.semte.ch', basepath='.well-known/genid/pyshacl-validation-result/')

    result_graph_uuid = generate_uuid()
    result_graph_uri = SHACL_VALIDATION_RESULT_GRAPH_URI_PREFIX + result_graph_uuid

    result = ValidationResult(
        success=conforms,
        result_graph=result_graph_uri,
        result_text=result_text
    )

    result_uri = save_result(result, task)

    # Store the graph after we stored the result so we don't lose track of which graph belongs to which result
    store_graph(result_graph, result_graph_uri)

    summary_uri = create_shacl_summary(result_graph_uri, input.data_graph, PUBLIC_GRAPH)
    task_runner.link_report_to_job(task.uri, summary_uri, predicate_uri=SHACL_REPORT_PREDICATE, graph=TASKS_GRAPH)

    return result_uri

task_runner.register(SHACL_VALIDATION_OPERATION, run_shacl_validation_task)


DCAT_CLASSES = [
    "http://www.w3.org/ns/dcat#Catalog",
    "http://www.w3.org/ns/dcat#Dataset",
    "http://www.w3.org/ns/dcat#Distribution",
    "http://www.w3.org/ns/dcat#CatalogRecord",
]


def aggregate_shacl_violations(result_graph_uri: str, data_graph_uri: str) -> dict:
    """Group sh:ValidationResult entries by (class, path, shape, severity)."""
    values = " ".join(sparql_escape_uri(c) for c in DCAT_CLASSES)
    q = f"""
PREFIX sh: <http://www.w3.org/ns/shacl#>

SELECT ?class ?path ?shape ?severity (COUNT(DISTINCT ?result) as ?count) WHERE {{
    GRAPH {sparql_escape_uri(result_graph_uri)} {{
        ?result a sh:ValidationResult ;
            sh:resultSeverity ?severity ;
            sh:focusNode ?node .
        OPTIONAL {{ ?result sh:resultPath ?path }}
        OPTIONAL {{ ?result sh:sourceShape ?shape }}
    }}
    GRAPH {sparql_escape_uri(data_graph_uri)} {{
        ?node a ?class .
        VALUES ?class {{ {values} }}
    }}
}} GROUP BY ?class ?path ?shape ?severity
"""
    res = query(q)
    by_class = {}
    for b in res["results"]["bindings"]:
        class_uri = b["class"]["value"]
        by_class.setdefault(class_uri, []).append({
            "path": b.get("path", {}).get("value"),
            "shape": b.get("shape", {}).get("value"),
            "severity": b["severity"]["value"],
            "count": int(b["count"]["value"]),
        })
    return by_class


def count_class_entities(data_graph_uri: str, class_uri: str) -> int:
    q = f"""
SELECT (COUNT(DISTINCT ?s) as ?count) WHERE {{
    GRAPH {sparql_escape_uri(data_graph_uri)} {{
        ?s a {sparql_escape_uri(class_uri)} .
    }}
}}
"""
    res = query(q)
    bindings = res["results"]["bindings"]
    return int(bindings[0]["count"]["value"]) if bindings else 0


def create_shacl_summary(result_graph_uri: str, data_graph_uri: str, graph: str) -> str:
    by_class = aggregate_shacl_violations(result_graph_uri, data_graph_uri)
    total_violations = sum(row["count"] for rows in by_class.values() for row in rows)

    summary_uuid = generate_uuid()
    summary_uri = VALIDATION_SUMMARY_URI_PREFIX + summary_uuid

    triples = [
        f"{sparql_escape_uri(summary_uri)} a shv:ValidationSummary ; "
        f"mu:uuid {sparql_escape_string(summary_uuid)} ; "
        f"shv:totalViolations {total_violations} ."
    ]

    for class_uri, rows in by_class.items():
        resource_count = count_class_entities(data_graph_uri, class_uri)
        tc_uuid = generate_uuid()
        tc_uri = TARGET_CLASS_SUMMARY_URI_PREFIX + tc_uuid
        triples.append(
            f"{sparql_escape_uri(summary_uri)} shv:hasTargetClassSummary {sparql_escape_uri(tc_uri)} ."
        )
        triples.append(
            f"{sparql_escape_uri(tc_uri)} a shv:TargetClassSummary ; "
            f"mu:uuid {sparql_escape_string(tc_uuid)} ; "
            f"shv:hasTargetClass {sparql_escape_uri(class_uri)} ; "
            f"shv:resourceCount {resource_count} ."
        )
        for row in rows:
            rs_uuid = generate_uuid()
            rs_uri = RULE_SUMMARY_URI_PREFIX + rs_uuid
            triples.append(
                f"{sparql_escape_uri(tc_uri)} shv:hasRuleSummary {sparql_escape_uri(rs_uri)} ."
            )
            rs = (
                f"{sparql_escape_uri(rs_uri)} a shv:RuleSummary ; "
                f"mu:uuid {sparql_escape_string(rs_uuid)} ; "
                f"shv:violationCount {row['count']} ; "
                f"shv:hasSeverity {sparql_escape_uri(row['severity'])}"
            )
            if row["path"]:
                rs += f" ; shv:hasRuleConstraint {sparql_escape_uri(row['path'])}"
            if row["shape"]:
                rs += f" ; shv:hasRule {sparql_escape_uri(row['shape'])}"
            rs += " ."
            triples.append(rs)

    q = f"""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX shv: <http://shacl.data.gift/shacl-validation#>

INSERT DATA {{
    GRAPH {sparql_escape_uri(graph)} {{
        {chr(10).join(triples)}
    }}
}}
"""
    update(q)
    return summary_uri

def get_input(input_uri, graph=None) -> Optional[ValidationInput]:
    query_template = Template("""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?data_graph WHERE {
    GRAPH $graph {
        $input_uri a ext:DCATValidationRequest ;
            # ext:shaclGraph ?shacl_graph ;
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
            # ext:shaclGraph $shacl_graph ;
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
        # shacl_graph=sparql_escape_uri(input.shacl_graph),
        data_graph=sparql_escape_uri(input.data_graph)
    )

    update(query)

    return input_uri

def save_result(result: ValidationResult, task, graph=DATA_GRAPH):
    uuid = generate_uuid()
    uri = SHACL_VALIDATION_RESULT_URI_PREFIX + uuid

    query_template = Template("""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX cogs: <http://vocab.deri.ie/cogs#>

INSERT DATA {
    GRAPH $graph {
        $result_uri a ext:ShaclValidationResult ;
            mu:uuid $uuid ;
            # TODO: There is probably an existing vocab that covers this?
            ext:validated $input ;
            ext:job $job ;
            ext:validationSuccess $success ;
            ext:resultGraph $result_graph ;
            ext:resultText $result_text .
    }
}
    """)

    query_string = query_template.substitute(
        graph=sparql_escape_uri(graph),
        result_uri=sparql_escape_uri(uri),
        input=sparql_escape_uri(task.input),
        job=sparql_escape_uri(task.get_job_uri()),
        uuid=sparql_escape_string(uuid),
        success=sparql_escape_bool(result.success),
        result_graph=sparql_escape_uri(result.result_graph),
        result_text=sparql_escape_string(result.result_text)
    )

    update(query_string)

    return uri

