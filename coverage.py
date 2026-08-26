from __future__ import annotations
import os
from typing import Optional
from dataclasses import dataclass, field, asdict

from helpers import generate_uuid
from sudo_query import query_sudo, update_sudo
from escape_helpers import sparql_escape_uri, sparql_escape_string

from constants import (
    DATA_GRAPH,
    PUBLIC_GRAPH,
    TASKS_GRAPH,
    COVERAGE_ANALYSIS_OPERATION,
    COVERAGE_REPORT_PREDICATE,
    VALIDATION_SUMMARY_URI_PREFIX,
    TARGET_CLASS_SUMMARY_URI_PREFIX,
    RULE_SUMMARY_URI_PREFIX,
)
from spec import Requirement, SEVERITY, MOBILITY_DCAT_AP_SPEC_VERSIONED, SpecVersion
from task import Task
import task_runner
from utils import save_json_report, get_endpoint_url, count_entities
from custom_exceptions import ResourceNotFoundError
mode = os.getenv("MODE", "production")

@dataclass
class RuleViolation:
    property_uri: str
    requirement: Requirement
    entities_with_property: int
    total_entities: int

    @property
    def violation_count(self) -> int:
        return max(0, self.total_entities - self.entities_with_property)


@dataclass
class ClassCoverage:
    class_uri: str
    total_entities: int = 0
    rule_violations: list[RuleViolation] = field(default_factory=list)


@dataclass
class CoverageResult:
    total_violations: int
    class_coverages: list[ClassCoverage] = field(default_factory=list)


def get_data_graph(input_uri: str, graph: str) -> Optional[str]:
    """Resolve the data graph from either an ext:DCATValidationRequest
    or an ext:ShaclValidationResult (following ext:validated)."""
    q = f"""
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

SELECT ?data_graph WHERE {{
    GRAPH {sparql_escape_uri(graph)} {{
        {{
            {sparql_escape_uri(input_uri)} a ext:DCATValidationRequest ;
                ext:dataGraph ?data_graph .
        }} UNION {{
            {sparql_escape_uri(input_uri)} a ext:ShaclValidationResult ;
                ext:validated/ext:dataGraph ?data_graph .
        }}
    }}
}} LIMIT 1
"""
    res = query_sudo(q)
    bindings = res["results"]["bindings"]
    return bindings[0]["data_graph"]["value"] if bindings else None


def run_coverage_analysis_task(task):
    data_graph = get_data_graph(task.input, DATA_GRAPH)
    if not data_graph:
        raise ResourceNotFoundError("The harvested data graph could not be found.")

    endpoint_url = get_endpoint_url(task.uri)
    coverage_result = compute_coverage(data_graph=data_graph, dcat_ap_version=task.dcat_ap_version)
    coverage_summary_uri = save_summary(coverage_result, endpoint_url=endpoint_url, graph=PUBLIC_GRAPH)
    task_runner.link_report_to_job(task.uri, coverage_summary_uri, predicate_uri=COVERAGE_REPORT_PREDICATE, graph=TASKS_GRAPH)
    return coverage_summary_uri

task_runner.register(COVERAGE_ANALYSIS_OPERATION, run_coverage_analysis_task)

def compute_coverage(data_graph: str, dcat_ap_version="1.1.0") -> CoverageResult:
    class_coverages = []
    total_violations = 0

    for class_uri, requirement_props in MOBILITY_DCAT_AP_SPEC_VERSIONED[dcat_ap_version].items():
        total = count_entities(data_graph, class_uri)
        all_props = [p for props in requirement_props.values() for p in props]
        prop_counts = count_entities_with_property(data_graph, class_uri, all_props)

        rule_violations = []
        for requirement, prop_uris in requirement_props.items():
            for prop_uri in prop_uris:
                rv = RuleViolation(
                    property_uri=prop_uri,
                    requirement=requirement,
                    entities_with_property=prop_counts.get(prop_uri, 0),
                    total_entities=total,
                )
                rule_violations.append(rv)
                total_violations += rv.violation_count

        class_coverages.append(ClassCoverage(class_uri=class_uri, total_entities=total, rule_violations=rule_violations))

    result = CoverageResult(total_violations=total_violations, class_coverages=class_coverages)
    if mode == "development":
        save_json_report(asdict(result), "/app/coverage_report.json")
    return result


def count_entities_with_property(data_graph: str, class_uri: str, property_uris: list[str]) -> dict[str, int]:
    """One GROUP BY query over all properties (replaces N single-property COUNTs)."""
    if not property_uris:
        return {}
    values = " ".join(sparql_escape_uri(p) for p in property_uris)
    q = f"""
SELECT ?prop (COUNT(DISTINCT ?s) as ?count) WHERE {{
    GRAPH {sparql_escape_uri(data_graph)} {{
        ?s a {sparql_escape_uri(class_uri)} ;
           ?prop ?o .
        VALUES ?prop {{ {values} }}
    }}
}} GROUP BY ?prop
"""
    res = query_sudo(q)
    return {b["prop"]["value"]: int(b["count"]["value"]) for b in res["results"]["bindings"]}


def save_summary(result: CoverageResult, graph: str, endpoint_url: Optional[str] = None) -> str:
    """Write shv:ValidationSummary / TargetClassSummary / RuleSummary.

    Matches app-mobilitydcatap-validator/doc/model.ttl and
    config/resources/shacl-validation.lisp.
    """
    summary_uuid = generate_uuid()
    summary_uri = VALIDATION_SUMMARY_URI_PREFIX + summary_uuid

    endpoint_triple = f"ext:endpointUrl {sparql_escape_string(endpoint_url)} ; " if endpoint_url else ""
    triples = [
        f"{sparql_escape_uri(summary_uri)} a shv:ValidationSummary ; "
        f"mu:uuid {sparql_escape_string(summary_uuid)} ; "
        f"{endpoint_triple}"
        f"shv:totalViolations {result.total_violations} ."
    ]

    for class_cov in result.class_coverages:
        tc_uuid = generate_uuid()
        tc_uri = TARGET_CLASS_SUMMARY_URI_PREFIX + tc_uuid
        triples.append(
            f"{sparql_escape_uri(summary_uri)} shv:hasTargetClassSummary {sparql_escape_uri(tc_uri)} ."
        )
        triples.append(
            f"{sparql_escape_uri(tc_uri)} a shv:TargetClassSummary ; "
            f"mu:uuid {sparql_escape_string(tc_uuid)} ; "
            f"shv:hasTargetClass {sparql_escape_uri(class_cov.class_uri)} ; "
            f"shv:resourceCount {class_cov.total_entities} ."
        )

        for rv in class_cov.rule_violations:
            rs_uuid = generate_uuid()
            rs_uri = RULE_SUMMARY_URI_PREFIX + rs_uuid
            triples.append(
                f"{sparql_escape_uri(tc_uri)} shv:hasRuleSummary {sparql_escape_uri(rs_uri)} ."
            )
            triples.append(
                f"{sparql_escape_uri(rs_uri)} a shv:RuleSummary ; "
                f"mu:uuid {sparql_escape_string(rs_uuid)} ; "
                f"shv:hasRuleConstraint {sparql_escape_uri(rv.property_uri)} ; "
                f"shv:violationCount {rv.violation_count} ; "
                f"shv:hasSeverity {sparql_escape_uri(SEVERITY[rv.requirement])} ."
            )

    q = f"""
PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
PREFIX shv: <http://shacl.data.gift/shacl-validation#>

INSERT DATA {{
    GRAPH {sparql_escape_uri(graph)} {{
        {chr(10).join(triples)}
    }}
}}
"""
    update_sudo(q)
    return summary_uri



# ---------------------------------------------------------------------------
# Additional checks — defined here so they're easy to wire in later.
# Neither is yet invoked by run_coverage_analysis_task.
# ---------------------------------------------------------------------------

def find_uri_reuse(data_graph: str) -> list[dict]:
    """URIs typed as multiple incompatible DCAT classes.

    E.g. the same URI used for both dcat:Distribution and dcat:Dataset. These
    are almost always bugs in the feed.
    """
    q = f"""
PREFIX dcat: <http://www.w3.org/ns/dcat#>

SELECT ?s (GROUP_CONCAT(DISTINCT STR(?t); separator=",") as ?types) WHERE {{
    GRAPH {sparql_escape_uri(data_graph)} {{
        ?s a ?t .
        VALUES ?t {{ dcat:Catalog dcat:Dataset dcat:Distribution dcat:CatalogRecord }}
    }}
}}
GROUP BY ?s
HAVING (COUNT(DISTINCT ?t) > 1)
"""
    res = query_sudo(q)
    return [
        {"uri": b["s"]["value"], "types": b["types"]["value"].split(",")}
        for b in res["results"]["bindings"]
    ]


# Class URI -> correct lowercased predicate. Detects feeds that use e.g.
# `dcat:Dataset` where `dcat:dataset` was intended.
CAPITALIZATION_TYPOS = {
    "http://www.w3.org/ns/dcat#Dataset": "http://www.w3.org/ns/dcat#dataset",
    "http://www.w3.org/ns/dcat#Distribution": "http://www.w3.org/ns/dcat#distribution",
    "http://www.w3.org/ns/dcat#CatalogRecord": "http://www.w3.org/ns/dcat#record",
}


def find_capitalization_typos(data_graph: str) -> list[dict]:
    """Triples where a class URI is used as a predicate (a common typo pattern)."""
    values = " ".join(sparql_escape_uri(p) for p in CAPITALIZATION_TYPOS)
    q = f"""
SELECT ?typo (COUNT(*) as ?count) WHERE {{
    GRAPH {sparql_escape_uri(data_graph)} {{
        ?s ?typo ?o .
        VALUES ?typo {{ {values} }}
    }}
}} GROUP BY ?typo
"""
    res = query_sudo(q)
    return [
        {
            "typo": b["typo"]["value"],
            "intended": CAPITALIZATION_TYPOS.get(b["typo"]["value"]),
            "count": int(b["count"]["value"]),
        }
        for b in res["results"]["bindings"]
    ]
