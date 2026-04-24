from typing import Optional
from dataclasses import dataclass, field
from string import Template
from enum import Enum

from helpers import query, update, generate_uuid
from escape_helpers import sparql_escape_uri, sparql_escape_string

from constants import (
    DATA_GRAPH,
    COVERAGE_ANALYSIS_OPERATION,
    VALIDATION_SUMMARY_URI_PREFIX,
    TARGET_CLASS_SUMMARY_URI_PREFIX,
    RULE_SUMMARY_URI_PREFIX,
)
from shacl import get_input
import task_runner


class Requirement(str, Enum):
    MANDATORY = "mandatory"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


# mobilityDCAT-AP spec. Source:
# https://mobilitydcat-ap.github.io/mobilityDCAT-AP/releases/index.html
MOBILITY_DCAT_AP_SPEC = {
    "http://www.w3.org/ns/dcat#Catalog": {
        Requirement.MANDATORY: [
            "http://purl.org/dc/terms/description",
            "http://purl.org/dc/terms/spatial",
            "http://xmlns.com/foaf/0.1/homepage",
            "http://purl.org/dc/terms/publisher",
            "http://www.w3.org/ns/dcat#record",
            "http://purl.org/dc/terms/title",
        ],
        Requirement.RECOMMENDED: [
            "http://purl.org/dc/terms/language",
            "http://purl.org/dc/terms/license",
            "http://purl.org/dc/terms/modified",
            "http://purl.org/dc/terms/issued",
            "http://www.w3.org/ns/dcat#themeTaxonomy",
        ],
        Requirement.OPTIONAL: [
            "http://www.w3.org/ns/dcat#dataset",
            "http://purl.org/dc/terms/hasPart",
            "http://purl.org/dc/terms/identifier",
            "http://www.w3.org/ns/adms#identifier",
        ],
    },
    "http://www.w3.org/ns/dcat#Dataset": {
        Requirement.MANDATORY: [
            "http://www.w3.org/ns/dcat#distribution",
            "http://purl.org/dc/terms/description",
            "http://purl.org/dc/terms/accrualPeriodicity",
            "http://purl.org/dc/terms/spatial",
            "https://w3id.org/mobilitydcat-ap#mobilityTheme",
            "http://purl.org/dc/terms/publisher",
            "http://purl.org/dc/terms/title",
        ],
        Requirement.RECOMMENDED: [
            "https://w3id.org/mobilitydcat-ap#georeferencingMethod",
            "http://www.w3.org/ns/dcat#contactPoint",
            "http://www.w3.org/ns/dcat#keyword",
            "https://w3id.org/mobilitydcat-ap#networkCoverage",
            "http://purl.org/dc/terms/conformsTo",
            "http://purl.org/dc/terms/rightsHolder",
            "http://purl.org/dc/terms/temporal",
            "http://www.w3.org/ns/dcat#theme",
            "https://w3id.org/mobilitydcat-ap#transportMode",
        ],
        Requirement.OPTIONAL: [
            "http://data.europa.eu/r5r/#applicableLegislation",
            "https://w3id.org/mobilitydcat-ap#assessmentResult",
            "http://purl.org/dc/terms/hasVersion",
            "http://purl.org/dc/terms/identifier",
            "https://w3id.org/mobilitydcat-ap#intendedInformationService",
            "http://purl.org/dc/terms/isReferencedBy",
            "http://purl.org/dc/terms/isVersionOf",
            "http://purl.org/dc/terms/language",
            "http://www.w3.org/ns/adms#identifier",
            "http://purl.org/dc/terms/relation",
            "http://purl.org/dc/terms/issued",
            "http://purl.org/dc/terms/modified",
            "http://www.w3.org/2002/07/owl#versionInfo",
            "http://www.w3.org/ns/adms#versionNotes",
            "http://www.w3.org/ns/dqv#hasQualityAnnotation",
        ],
    },
    "http://www.w3.org/ns/dcat#Distribution": {
        Requirement.MANDATORY: [
            "http://www.w3.org/ns/dcat#accessURL",
            "https://w3id.org/mobilitydcat-ap#mobilityDataStandard",
            "http://purl.org/dc/terms/format",
            "http://purl.org/dc/terms/rights",
        ],
        Requirement.RECOMMENDED: [
            "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol",
            "http://purl.org/dc/terms/description",
            "http://purl.org/dc/terms/license",
        ],
        Requirement.OPTIONAL: [
            "http://www.w3.org/ns/dcat#accessService",
            "http://www.w3.org/2011/content#characterEncoding",
            "https://w3id.org/mobilitydcat-ap#communicationMethod",
            "https://w3id.org/mobilitydcat-ap#dataFormatNotes",
            "http://www.w3.org/ns/dcat#downloadURL",
            "https://w3id.org/mobilitydcat-ap#grammar",
            "http://www.w3.org/ns/adms#sample",
            "http://purl.org/dc/terms/temporal",
        ],
    },
    "http://www.w3.org/ns/dcat#CatalogRecord": {
        Requirement.MANDATORY: [
            "http://purl.org/dc/terms/created",
            "http://purl.org/dc/terms/language",
            "http://purl.org/dc/terms/modified",
            "http://xmlns.com/foaf/0.1/primaryTopic",
        ],
        Requirement.OPTIONAL: [
            "http://purl.org/dc/terms/publisher",
            "http://purl.org/dc/terms/source",
        ],
    },
}


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
    rule_violations: list[RuleViolation] = field(default_factory=list)


@dataclass
class CoverageResult:
    total_violations: int
    class_coverages: list[ClassCoverage] = field(default_factory=list)


def run_coverage_analysis_task(task):
    input = get_input(task.input, DATA_GRAPH)
    if not input:
        raise Exception(f"Input {task.input} not found!")

    result = compute_coverage(data_graph=input.data_graph)
    return save_summary(result, graph=DATA_GRAPH)


task_runner.register(COVERAGE_ANALYSIS_OPERATION, run_coverage_analysis_task)


def compute_coverage(data_graph: str) -> CoverageResult:
    class_coverages = []
    total_violations = 0

    for class_uri, requirement_props in MOBILITY_DCAT_AP_SPEC.items():
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
                if requirement == Requirement.MANDATORY:
                    total_violations += rv.violation_count

        class_coverages.append(ClassCoverage(class_uri=class_uri, rule_violations=rule_violations))

    return CoverageResult(total_violations=total_violations, class_coverages=class_coverages)


def count_entities(data_graph: str, class_uri: str) -> int:
    q = Template("""
SELECT (COUNT(DISTINCT ?s) as ?count) WHERE {
    GRAPH $graph {
        ?s a $class .
    }
}
""").substitute(graph=sparql_escape_uri(data_graph), class_=sparql_escape_uri(class_uri)).replace("$class", sparql_escape_uri(class_uri))
    # Template doesn't allow `class` as a key; swap manually.
    res = query(q)
    bindings = res["results"]["bindings"]
    return int(bindings[0]["count"]["value"]) if bindings else 0


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
    res = query(q)
    return {b["prop"]["value"]: int(b["count"]["value"]) for b in res["results"]["bindings"]}


def save_summary(result: CoverageResult, graph: str) -> str:
    """Write shv:ValidationSummary / TargetClassSummary / RuleSummary.

    Matches app-mobilitydcatap-validator/doc/model.ttl and
    config/resources/shacl-validation.lisp.
    """
    summary_uuid = generate_uuid()
    summary_uri = VALIDATION_SUMMARY_URI_PREFIX + summary_uuid

    triples = [
        f"{sparql_escape_uri(summary_uri)} a shv:ValidationSummary ; "
        f"mu:uuid {sparql_escape_string(summary_uuid)} ; "
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
            f"shv:hasTargetClass {sparql_escape_uri(class_cov.class_uri)} ."
        )

        for rv in class_cov.rule_violations:
            if rv.violation_count == 0:
                continue  # nothing to report for a fully satisfied rule
            rs_uuid = generate_uuid()
            rs_uri = RULE_SUMMARY_URI_PREFIX + rs_uuid
            triples.append(
                f"{sparql_escape_uri(tc_uri)} shv:hasRuleSummary {sparql_escape_uri(rs_uri)} ."
            )
            triples.append(
                f"{sparql_escape_uri(rs_uri)} a shv:RuleSummary ; "
                f"mu:uuid {sparql_escape_string(rs_uuid)} ; "
                f"shv:hasRuleConstraint {sparql_escape_uri(rv.property_uri)} ; "
                f"shv:violationCount {rv.violation_count} ."
                # TODO: shv:hasSeverity — map requirement (M/R/O) to sh:Violation/sh:Warning/sh:Info.
                # TODO: shv:hasValidationResult — link to matching sh:ValidationResult entries
                #       in the SHACL report graph so the UI can drill into offenders.
            )

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
    res = query(q)
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
    res = query(q)
    return [
        {
            "typo": b["typo"]["value"],
            "intended": CAPITALIZATION_TYPOS.get(b["typo"]["value"]),
            "count": int(b["count"]["value"]),
        }
        for b in res["results"]["bindings"]
    ]
