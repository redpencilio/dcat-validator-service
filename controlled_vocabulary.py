from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

import rdflib
from escape_helpers import sparql_escape_int, sparql_escape_string, sparql_escape_uri
from helpers import generate_uuid
from rdflib.namespace import RDF, SKOS

import task_runner
from constants import (
    DATA_GRAPH,
    DCAT_CLASSES,
    PUBLIC_GRAPH,
    RULE_SUMMARY_URI_PREFIX,
    TARGET_CLASS_SUMMARY_URI_PREFIX,
    TASKS_GRAPH,
    VALIDATION_SUMMARY_URI_PREFIX,
    VOCAB_REPORT_PREDICATE,
    VOCABULARY_ANALYSIS_OPERATION,
)
from sudo_query import query_sudo as query
from sudo_query import update_sudo as update
from utils import get_endpoint_url, save_json_report

mode = os.getenv("MODE", "production")


@dataclass
class VocabularyViolation:
    property_uri: str
    invalid_term: str
    violation_count: int
    severity: str


@dataclass
class ClassVocabularyCompliance:
    class_uri: str
    total_entities_checked: int = 0
    vocabulary_violations: list[VocabularyViolation] = field(default_factory=list)


@dataclass
class VocabularyResult:
    total_violations: int
    class_compliances: list[ClassVocabularyCompliance] = field(default_factory=list)


PROPERTY_MAPPING = {
    # File Stem: Predicate URI
    "transport-mode": "https://w3id.org/mobilitydcat-ap#transportMode",
    "mobility-theme": "https://w3id.org/mobilitydcat-ap#mobilityTheme",
    "mobility-data-standard": "https://w3id.org/mobilitydcat-ap#mobilityDataStandard",
    "application-layer-protocol": "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol",
    "communication-method": "https://w3id.org/mobilitydcat-ap#communicationMethod",
    "network-coverage": "https://w3id.org/mobilitydcat-ap#networkCoverage",
    "georeferencing-method": "https://w3id.org/mobilitydcat-ap#georeferencingMethod",
    "intended-information-service": "https://w3id.org/mobilitydcat-ap#intendedInformationService",
    "grammar": "https://w3id.org/mobilitydcat-ap#grammar",
}


def get_vocabulary_dict() -> dict[str, str]:
    vocabulary_dict = {}
    vocabularies_path = Path("/app/vocabularies")
    for vocab_file in vocabularies_path.iterdir():
        if vocab_file.is_file() and vocab_file.name.endswith(".ttl"):
            predicate = PROPERTY_MAPPING.get(vocab_file.stem)
            if not predicate:
                print(f"Skipping {vocab_file.name}: not found in PROPERTY_MAPPING")
                continue

            g = rdflib.Graph()
            g.parse(vocab_file.absolute(), format="turtle")
            allowed_set = set()

            for term in g.subjects(predicate=RDF.type, object=SKOS.Concept):
                allowed_set.add(str(term))

            vocabulary_dict[predicate] = allowed_set

    return vocabulary_dict


ALLOWED_VOCABULARIES = get_vocabulary_dict()


def count_entities(data_graph_uri: str, dcat_class: str) -> int:
    q = f"""
        SELECT (COUNT(DISTINCT ?s) as ?count) WHERE {{
            GRAPH {sparql_escape_uri(data_graph_uri)} {{
                ?s a {sparql_escape_uri(dcat_class)} .
            }}
        }}
    """
    res = query(q)
    bindings = res.get("results", {}).get("bindings", [])
    return int(bindings[0]["count"]["value"]) if bindings else 0


def compute_vocabulary_compliance(data_graph_uri: str) -> VocabularyResult:
    class_compliances: list[ClassVocabularyCompliance] = []
    grand_total_violations = 0

    for dcat_class in DCAT_CLASSES:
        class_vocabulary_violations: list[VocabularyViolation] = []

        total_entities = count_entities(data_graph_uri, dcat_class)

        for term in ALLOWED_VOCABULARIES:
            violations = get_property_violations(
                data_graph_uri, dcat_class=dcat_class, term_predicate=term
            )
            class_vocabulary_violations.extend(violations)

            for v in violations:
                grand_total_violations += v.violation_count

        class_compliances.append(
            ClassVocabularyCompliance(
                class_uri=dcat_class,
                total_entities_checked=total_entities,
                vocabulary_violations=class_vocabulary_violations,
            )
        )

    result = VocabularyResult(
        total_violations=grand_total_violations,
        class_compliances=class_compliances,
    )
    if mode == "development":
        save_json_report(asdict(result), "/app/vocabulary_result.json")
    return result


def get_property_violations(
    data_graph_uri: str, dcat_class: str, term_predicate: str
) -> list[VocabularyViolation]:
    q = f"""
        SELECT ?term (COUNT(DISTINCT ?s) as ?count) WHERE {{
            GRAPH {sparql_escape_uri(data_graph_uri)} {{
                ?s a {sparql_escape_uri(dcat_class)};
                    {sparql_escape_uri(term_predicate)} ?term.
            }}
        }} GROUP BY ?term
    """
    query_res = query(q)
    result: list[VocabularyViolation] = []
    bindings = query_res.get("results", {}).get("bindings", [])

    for binding in bindings:
        used_term = binding["term"]["value"]
        count = int(binding["count"]["value"])

        if used_term not in ALLOWED_VOCABULARIES[term_predicate]:
            result.append(
                VocabularyViolation(
                    property_uri=term_predicate,
                    invalid_term=used_term,
                    violation_count=count,
                    severity="http://www.w3.org/ns/shacl#Violation",
                )
            )

    return result


def save_vocabulary_summary(
    result: VocabularyResult, graph: str, endpoint_url: str | None = None
) -> str:
    """Write shv:ValidationSummary / TargetClassSummary / RuleSummary.

    Matches app-mobilitydcatap-validator/doc/model.ttl and
    config/resources/shacl-validation.lisp.
    """
    summary_uuid = generate_uuid()
    summary_uri = VALIDATION_SUMMARY_URI_PREFIX + summary_uuid

    endpoint_triple = (
        f"ext:endpointUrl {sparql_escape_string(endpoint_url)} ; "
        if endpoint_url
        else ""
    )
    triples = [
        (
            f"{sparql_escape_uri(summary_uri)} a shv:ValidationSummary ; "
            f"mu:uuid {sparql_escape_string(summary_uuid)} ; "
            f"{endpoint_triple}"
            f"shv:totalViolations {sparql_escape_int(result.total_violations)} ."
        )
    ]

    for class_cov in result.class_compliances:
        tc_uuid = generate_uuid()
        tc_uri = TARGET_CLASS_SUMMARY_URI_PREFIX + tc_uuid
        triples.append(
            f"{sparql_escape_uri(summary_uri)} shv:hasTargetClassSummary {sparql_escape_uri(tc_uri)} ."
        )
        triples.append(
            f"{sparql_escape_uri(tc_uri)} a shv:TargetClassSummary ; "
            f"mu:uuid {sparql_escape_string(tc_uuid)} ; "
            f"shv:hasTargetClass {sparql_escape_uri(class_cov.class_uri)} ; "
            f"shv:resourceCount {sparql_escape_int(class_cov.total_entities_checked)} ."
        )

        for rv in class_cov.vocabulary_violations:
            rs_uuid = generate_uuid()
            rs_uri = RULE_SUMMARY_URI_PREFIX + rs_uuid
            triples.append(
                f"{sparql_escape_uri(tc_uri)} shv:hasRuleSummary {sparql_escape_uri(rs_uri)} ."
            )
            triples.append(
                f"{sparql_escape_uri(rs_uri)} a shv:RuleSummary ; "
                f"mu:uuid {sparql_escape_string(rs_uuid)} ; "
                f"shv:hasRuleConstraint {sparql_escape_uri(rv.property_uri)} ; "
                f"shv:violationCount {sparql_escape_int(rv.violation_count)} ; "
                f"shv:message {sparql_escape_string(rv.invalid_term)} ; "
                f"shv:hasSeverity {sparql_escape_uri(rv.severity)} ."
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
    update(q)
    return summary_uri


def get_data_graph(input_uri: str, graph: str) -> str | None:
    """Resolve the data graph by tracing a Coverage Report (shv:ValidationSummary) back to the original request."""
    q = f"""
        PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX task: <http://redpencil.data.gift/vocabularies/tasks/>

        SELECT ?data_graph WHERE {{
            GRAPH {sparql_escape_uri(TASKS_GRAPH)} {{
                ?job ext:coverageReport {sparql_escape_uri(input_uri)} .
                ?any_task dct:isPartOf ?job ;
                          task:inputContainer/ext:content ?original_req .
            }}
            GRAPH {sparql_escape_uri(graph)} {{
                ?original_req ext:dataGraph ?data_graph .
            }}
        }} LIMIT 1
    """
    res = query(q)
    bindings = res.get("results", {}).get("bindings", [])
    return bindings[0]["data_graph"]["value"] if bindings else None


def run_vocabulary_analysis_task(task):
    data_graph = get_data_graph(task.input, DATA_GRAPH)
    if not data_graph:
        raise Exception(f"Input {task.input} not found!")

    endpoint_url = get_endpoint_url(task.uri)
    vocabulary_result = compute_vocabulary_compliance(data_graph_uri=data_graph)
    vocabulary_summary_uri = save_vocabulary_summary(
        vocabulary_result, endpoint_url=endpoint_url, graph=PUBLIC_GRAPH
    )

    task_runner.link_report_to_job(
        task.uri,
        vocabulary_summary_uri,
        predicate_uri=VOCAB_REPORT_PREDICATE,
        graph=TASKS_GRAPH,
    )
    return vocabulary_summary_uri


task_runner.register(VOCABULARY_ANALYSIS_OPERATION, run_vocabulary_analysis_task)
