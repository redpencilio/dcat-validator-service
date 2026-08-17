from __future__ import annotations

from itertools import islice
from constants import GRAPH_LOAD_BATCH_SIZE
from escape_helpers import sparql_escape_uri
from sudo_query import query_sudo as query, update_sudo as update
from constants import TASKS_GRAPH
import rdflib
import dataclasses
import json


def from_binding(datacls, binding, **extra):
    values = {
        field.name: binding[field.name]["value"]
        for field in dataclasses.fields(datacls)
        if field.name in binding
    }
    values.update(extra)
    return datacls(**values)

# From python itertools documentation
def batched(iterable, n, *, strict=False):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError('batched(): incomplete batch')
        yield batch

# adapted from https://github.com/RDFLib/rdflib/issues/1704
def store_graph(g: rdflib.Graph, graph_name: str):
    """RDFlib graph to sparql"""
    for triples_batch in batched(g.triples((None, None, None)), GRAPH_LOAD_BATCH_SIZE):
        updatequery = "\n".join(
            [f"PREFIX {prefix}: {ns.n3()}" for prefix, ns in g.namespaces()]
        )
        updatequery += f"\nINSERT DATA {{\n\tGRAPH {sparql_escape_uri(graph_name)} {{\n"
        updatequery += " .\n".join(
            [f"\t\t{s.n3()} {p.n3()} {o.n3()}" for (s, p, o) in triples_batch]
        )
        updatequery += f" . \n\t }}\n}}\n"

        update(updatequery)

def listize(object):
    """Wraps `object` in a list, unless it is already a list."""
    if isinstance(object, list):
        return object
    else:
        return [object]

def save_json_report(
    report_dict,
    output_path: str = "/app/shacl_report.json",
):
    with open(output_path, "w") as f:
        json.dump(report_dict, f, indent=4)

    print(f"Readable JSON report saved to {output_path}")

def get_endpoint_url(task_uri: str) -> str | None:
    q = f"""
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?url WHERE {{
    GRAPH {sparql_escape_uri(TASKS_GRAPH)} {{
        {sparql_escape_uri(task_uri)} dct:isPartOf ?job .
        ?job ext:endpointUrl ?url .
    }}
}} LIMIT 1
"""
    res = query(q)
    bindings = res.get("results", {}).get("bindings", [])
    return bindings[0]["url"]["value"] if bindings else None
