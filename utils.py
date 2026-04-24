from itertools import islice
from string import Template
from constants import GRAPH_LOAD_BATCH_SIZE
from escape_helpers import sparql_escape_uri
from context_query import query, update
import rdflib
import dataclasses

def from_binding(datacls, binding, **extra):
    values = {
        field.name: binding[field.name]["value"]
        for field in dataclasses.fields(datacls)
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

