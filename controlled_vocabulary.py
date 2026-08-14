from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path

import rdflib
from escape_helpers import sparql_escape_uri
from rdflib.namespace import RDF, SKOS

from constants import DCAT_CLASSES
from sudo_query import query_sudo as query
from sudo_query import update_sudo as update
from utils import save_json_report

mode = os.getenv("MODE", "production")


@dataclass
class VocabularyViolation:
    property_uri: str
    invalid_term: str
    violation_count: int


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
    "grammar": "https://w3id.org/mobilitydcat-ap#grammar"
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
                )
            )

    return result
