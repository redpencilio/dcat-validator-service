from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

from escape_helpers import sparql_escape_int, sparql_escape_string, sparql_escape_uri
from helpers import generate_uuid

import task_runner
from constants import (
    DATA_GRAPH,
    PUBLIC_GRAPH,
    RULE_SUMMARY_URI_PREFIX,
    TARGET_CLASS_SUMMARY_URI_PREFIX,
    TASKS_GRAPH,
    VALIDATION_SUMMARY_URI_PREFIX,
    VOCAB_REPORT_PREDICATE,
    VOCABULARY_ANALYSIS_OPERATION,
)
from spec import (
    DCAT_CLASSES,
    MOBILITY_DCAT_AP_SPEC,
    SEVERITY,
)
from sudo_query import query_sudo as query
from sudo_query import update_sudo as update
from task import Task
from utils import get_endpoint_url, save_json_report

mode = os.getenv("MODE", "production")


@dataclass
class VocabularyViolation:
    property_uri: str
    invalid_terms: list[str]
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


def get_vocabulary_dict() -> dict[str, set[str]]:
    vocab_json_path = Path(
        os.environ.get(
            "VOCABULARIES_JSON", Path(__file__).resolve().parent / "vocabularies.json"
        )
    )

    if not vocab_json_path.exists() or vocab_json_path.stat().st_size == 0:
        print(
            f"{vocab_json_path.name} not found. Generating from source vocabularies..."
        )
        try:
            from generate_vocabularies import generate_vocabulary_dict

            raw_dict = generate_vocabulary_dict(output_path=vocab_json_path)
            return {k: set(v) for k, v in raw_dict.items()}
        except Exception as e:
            print(f"Error generating vocabulary dictionary: {e}")
            return {}

    print(f"Loading controlled vocabularies from {vocab_json_path.name}...")
    try:
        with open(vocab_json_path, "r", encoding="utf-8") as f:
            cached_dict = json.load(f)
        return {k: set(v) for k, v in cached_dict.items()}
    except Exception as e:
        print(f"Error loading {vocab_json_path}: {e}")
        return {}


ALLOWED_VOCABULARIES = get_vocabulary_dict()

AT_LEAST_ONE_VOCAB_PROPERTIES: set[str] = {
    "https://w3id.org/mobilitydcat-ap#mobilityTheme",
    "http://www.w3.org/ns/dcat#theme",
    "https://w3id.org/mobilitydcat-ap#georeferencingMethod",
    "https://w3id.org/mobilitydcat-ap#networkCoverage",
    "https://w3id.org/mobilitydcat-ap#transportMode",
    "https://w3id.org/mobilitydcat-ap#intendedInformationService",
    "https://w3id.org/mobilitydcat-ap#mobilityDataStandard",
    "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol",
    "http://purl.org/dc/terms/type",
}


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


def compute_vocabulary_compliance(
    data_graph_uri: str, dcat_ap_version: str
) -> VocabularyResult:

    class_compliances: list[ClassVocabularyCompliance] = []
    grand_total_violations = 0

    for dcat_class in DCAT_CLASSES:
        class_vocabulary_violations: list[VocabularyViolation] = []

        total_entities = count_entities(data_graph_uri, dcat_class)

        for term in ALLOWED_VOCABULARIES:
            violations = get_property_violations(
                data_graph_uri,
                dcat_class=dcat_class,
                term_predicate=term,
                dcat_ap_version=dcat_ap_version,
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
    data_graph_uri: str,
    dcat_class: str,
    term_predicate: str,
    dcat_ap_version: str = "1.1.0",
) -> list[VocabularyViolation]:
    # Check if this property is applicable to this DCAT class
    class_reqs = MOBILITY_DCAT_AP_SPEC.get(dcat_class, {})
    severity = None
    for req, props in class_reqs.items():
        if term_predicate in props:
            severity = SEVERITY[req]
            break

    if not severity:
        return []

    q = f"""
        PREFIX dct: <http://purl.org/dc/terms/>
        PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
        SELECT DISTINCT ?s ?term ?identifier WHERE {{
            GRAPH {sparql_escape_uri(data_graph_uri)} {{
                ?s a {sparql_escape_uri(dcat_class)};
                    {sparql_escape_uri(term_predicate)} ?term.
                OPTIONAL {{
                    ?term dct:identifier | skos:exactMatch | skos:inScheme ?identifier .
                    FILTER(isIRI(?identifier))
                }}
            }}
        }}
    """
    if mode == "development":
        print(f"[QUERY]: {q}")
    query_res = query(q)
    bindings = query_res.get("results", {}).get("bindings", [])

    invalid_terms = set()
    non_compliant_resources = set()

    # Group identifiers by subject and term node to evaluate each resource as a whole
    # resources_map: dict[subject_uri, dict[(term_val, term_type), set[identifier_uris]]]
    resources_map: dict[str, dict[tuple[str, str], set[str]]] = {}
    for binding in bindings:
        s = binding["s"]["value"]
        term_val = binding["term"]["value"]
        term_type = binding["term"]["type"]
        identifier = binding.get("identifier", {}).get("value")

        if s not in resources_map:
            resources_map[s] = {}
        key = (term_val, term_type)
        if key not in resources_map[s]:
            resources_map[s][key] = set()
        if identifier:
            resources_map[s][key].add(identifier)

    allowed = ALLOWED_VOCABULARIES[term_predicate]

    # "At least one" rule applies for mobilityDCAT-AP v3.0.0+ on specific properties
    is_at_least_one_rule = (
        dcat_ap_version.startswith("3")
        and term_predicate in AT_LEAST_ONE_VOCAB_PROPERTIES
    )

    for s, terms_dict in resources_map.items():
        resource_has_valid_term = False
        resource_invalid_terms = set()

        for (term_val, term_type), identifiers in terms_dict.items():
            # If it's a blank node (skolemized or not) and has NO identifiers, just let it pass.
            # (the publisher didn't provide an invalid term, they just used a blank node wrapper)
            is_blank_node = term_type == "bnode" or ".well-known/genid" in term_val
            if is_blank_node and not identifiers:
                continue

            # First check if the term itself is a valid URI
            if term_type == "uri" and term_val in allowed:
                resource_has_valid_term = True
                continue

            # Then check if any of its identifiers map to the allowed vocabulary
            is_valid = False
            if identifiers:
                for identifier in identifiers:
                    if identifier in allowed:
                        is_valid = True
                        break
            else:
                # If it's not a blank node and has no nested identifiers, we check the term itself
                is_valid = term_type == "uri" and term_val in allowed

            if is_valid:
                resource_has_valid_term = True
            else:
                if identifiers:
                    for identifier in identifiers:
                        resource_invalid_terms.add(identifier)
                else:
                    resource_invalid_terms.add(term_val)

        if is_at_least_one_rule:
            # Rule: At least 1 value from controlled vocabulary.
            # If the resource has at least one valid term, non-controlled vocabulary values are tolerated.
            # If the resource has NO valid term from the controlled vocabulary, mark it non-compliant.
            if not resource_has_valid_term and resource_invalid_terms:
                invalid_terms.update(resource_invalid_terms)
                non_compliant_resources.add(s)
        else:
            # Default / Strict rule: ALL values must be from the controlled vocabulary.
            if resource_invalid_terms:
                invalid_terms.update(resource_invalid_terms)
                non_compliant_resources.add(s)

    formatted_invalid_terms = sorted(invalid_terms)
    if len(formatted_invalid_terms) > 10:
        extra_count = len(formatted_invalid_terms) - 10
        formatted_invalid_terms = formatted_invalid_terms[:10] + [
            f"(+{extra_count} more)"
        ]

    return [
        VocabularyViolation(
            property_uri=term_predicate,
            invalid_terms=formatted_invalid_terms,
            violation_count=len(non_compliant_resources),
            severity=severity,
        )
    ]


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

            message_triple = (
                f"shv:message {sparql_escape_string(', '.join(rv.invalid_terms))} ; "
                if rv.invalid_terms
                else ""
            )

            triples.append(
                f"{sparql_escape_uri(tc_uri)} shv:hasRuleSummary {sparql_escape_uri(rs_uri)} ."
            )
            triples.append(
                f"{sparql_escape_uri(rs_uri)} a shv:RuleSummary ; "
                f"mu:uuid {sparql_escape_string(rs_uuid)} ; "
                f"shv:hasRuleConstraint {sparql_escape_uri(rv.property_uri)} ; "
                f"shv:violationCount {sparql_escape_int(rv.violation_count)} ; "
                f"{message_triple}"
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


def run_vocabulary_analysis_task(task: Task):
    data_graph = get_data_graph(task.input, DATA_GRAPH)
    if not data_graph:
        raise Exception(f"Input {task.input} not found!")

    endpoint_url = get_endpoint_url(task.uri)
    vocabulary_result = compute_vocabulary_compliance(
        data_graph_uri=data_graph, dcat_ap_version=task.dcat_ap_version or "1.1.0"
    )
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
