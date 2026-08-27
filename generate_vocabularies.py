"""
Generates vocabularies.json by parsing controlled vocabulary files (from local files or remote URLs)
into a compact JSON dictionary mapping DCAT property URIs to allowed Concept URIs.
"""

from __future__ import annotations

import json
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import rdflib
from rdflib.namespace import RDF, SKOS

from spec import STEM_PROPERTY_MAPPING

EU_BASE = "https://op.europa.eu/o/opportal-service/euvoc-download-handler?cellarURI="
MOB_BASE = (
    "https://github.com/mobilityDCAT-AP/controlled-vocabularies/raw/refs/heads/main"
)

# Remote sources for controlled vocabularies
REMOTE_VOCABULARY_SOURCES = {
    # EU Publications Office
    "continents-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fcontinent%2F20260617-0%2Frdf%2Fskos_core%2Fcontinents-skos.rdf&fileName=continents-skos.rdf",
        "format": "rdf-xml",
    },
    "countries-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fcountry%2F20260617-0%2Frdf%2Fskos_core%2Fcountries-skos.rdf&fileName=countries-skos.rdf",
        "format": "rdf-xml",
    },
    "places-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fplace%2F20260617-0%2Frdf%2Fskos_core%2Fplaces-skos.rdf&fileName=places-skos.rdf",
        "format": "rdf-xml",
    },
    "languages-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Flanguage%2F20260617-0%2Frdf%2Fskos_core%2Flanguages-skos.rdf&fileName=languages-skos.rdf",
        "format": "rdf-xml",
    },
    "filetypes-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Ffile-type%2F20260715-0%2Frdf%2Fskos_core%2Ffiletypes-skos.rdf&fileName=filetypes-skos.rdf",
        "format": "rdf-xml",
    },
    "frequencies-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Ffrequency%2F20260617-0%2Frdf%2Fskos_core%2Ffrequencies-skos.rdf&fileName=frequencies-skos.rdf",
        "format": "rdf-xml",
    },
    "data-theme-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fdata-theme%2F20241211-0%2Frdf%2Fskos_core%2Fdata-theme-skos.rdf&fileName=data-theme-skos.rdf",
        "format": "rdf-xml",
    },
    "distribution-status-skos": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fdistribution-status%2F20260617-0%2Frdf%2Fskos_core%2Fdistribution-status-skos.rdf&fileName=distribution-status-skos.rdf",
        "format": "rdf-xml",
    },
    # "corporatebodies-skos": {
    #     "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fcorporate-body%2F20260617-0%2Frdf%2Fskos_core%2Fcorporatebodies-skos.rdf&fileName=corporatebodies-skos.rdf",
    #     "format": "rdf-xml",
    # },
    "NUTS": {
        "url": f"{EU_BASE}http%3A%2F%2Fpublications.europa.eu%2Fresource%2Fdistribution%2Fnuts%2F20260701-0%2Frdf%2Fskos_xl%2FNUTS.rdf&fileName=NUTS.rdf",
        "format": "rdf-xml",
    },
    # mobilityDCAT-AP
    "application-layer-protocol": {
        "url": f"{MOB_BASE}/application-layer-protocol/latest/application-layer-protocol.ttl",
        "format": "turtle",
    },
    "communication-method": {
        "url": f"{MOB_BASE}/communication-method/latest/communication-method.ttl",
        "format": "turtle",
    },
    # "conditions-for-access-and-usage": {
    #     "url": f"{MOB_BASE}/conditions-for-access-and-usage/latest/conditions-for-access-and-usage.ttl",
    #     "format": "turtle",
    # },
    "energy-fuel-type": {
        "url": f"{MOB_BASE}/energy-fuel-type/latest/energy-fuel-type.ttl",
        "format": "turtle",
    },
    "georeferencing-method": {
        "url": f"{MOB_BASE}/georeferencing-method/latest/georeferencing-method.ttl",
        "format": "turtle",
    },
    "grammar": {
        "url": f"{MOB_BASE}/grammar/latest/grammar.ttl",
        "format": "turtle",
    },
    "intended-information-service": {
        "url": f"{MOB_BASE}/intended-information-service/latest/intended-information-service.ttl",
        "format": "turtle",
    },
    "mobility-data-standard": {
        "url": f"{MOB_BASE}/mobility-data-standard/latest/mobility-data-standard.ttl",
        "format": "turtle",
    },
    "mobility-theme": {
        "url": f"{MOB_BASE}/mobility-theme/latest/mobility-theme.ttl",
        "format": "turtle",
    },
    "network-coverage": {
        "url": f"{MOB_BASE}/network-coverage/latest/network-coverage.ttl",
        "format": "turtle",
    },
    "transport-mode": {
        "url": f"{MOB_BASE}/transport-mode/latest/transport-mode.ttl",
        "format": "turtle",
    },
    "update-frequency": {
        "url": f"{MOB_BASE}/update-frequency/latest/update-frequency.ttl",
        "format": "turtle",
    },
    # OGC - CRS NTS XML (non-SKOS: plain <identifier> list)
    "reference-systems": {
        "url": "https://www.opengis.net/def/crs/EPSG/0/",
        "format": "crs-nts-xml",
    },
}


# Namespace used by OGC CRS NTS XML identifier lists
_CRS_NTS_NS = "http://www.opengis.net/crs-nts/1.0"


def parse_crs_nts_xml_identifiers(url: str) -> set[str]:
    """Fetches an OGC CRS NTS XML document and returns all <identifier> URIs.

    The format looks like:
        <identifiers xmlns='http://www.opengis.net/crs-nts/1.0' ...>
          <identifier>https://www.opengis.net/def/crs/EPSG/0/4326</identifier>
          ...
        </identifiers>
    """
    req = urllib.request.Request(url, headers={"Accept": "application/xml"})
    with urllib.request.urlopen(req) as resp:
        raw = resp.read()
    root = ET.fromstring(raw)
    return {
        elem.text.strip()
        for elem in root.iter(f"{{{_CRS_NTS_NS}}}identifier")
        if elem.text and elem.text.strip()
    }


def parse_vocabulary_concepts(source: str | Path, rdf_format: str) -> set[str]:
    """Parses a graph from a file path or URL and returns all skos:Concept URIs."""
    g = rdflib.Graph()
    if isinstance(source, Path):
        g.parse(source.absolute(), format=rdf_format)
    else:
        g.parse(source, format=rdf_format)
    return {str(term) for term in g.subjects(predicate=RDF.type, object=SKOS.Concept)}


def generate_vocabulary_dict(
    local_dir: Path | None = None,
    output_path: Path | None = None,
) -> dict[str, list[str]]:
    """
    Generates the vocabulary dictionary mapping property URIs to lists of concept URIs.
    Prefers local files if available in local_dir, otherwise fetches directly from remote URLs.
    """
    if output_path is None:
        output_path = Path(__file__).resolve().parent / "vocabularies.json"

    if local_dir is None:
        local_dir = Path(__file__).resolve().parent / "vocabularies"

    vocabulary_dict: dict[str, set[str]] = {}

    for stem, predicates in STEM_PROPERTY_MAPPING.items():
        # Check if local file exists
        local_file = None
        if local_dir.exists():
            for ext in [".ttl", ".rdf"]:
                candidate = local_dir / f"{stem}{ext}"
                if candidate.exists() and candidate.stat().st_size > 0:
                    local_file = candidate
                    break

        concepts = set()
        if local_file:
            print(f"Parsing local file: {local_file.name}...")
            rdf_format = "turtle" if local_file.suffix == ".ttl" else "xml"
            concepts = parse_vocabulary_concepts(local_file, rdf_format)
        elif stem in REMOTE_VOCABULARY_SOURCES:
            meta = REMOTE_VOCABULARY_SOURCES[stem]
            print(f"Fetching from remote: {stem} ({meta['url'][:60]}...)...")
            if meta["format"] == "crs-nts-xml":
                concepts = parse_crs_nts_xml_identifiers(meta["url"])
            elif meta["format"] == "rdf-xml":
                concepts = parse_vocabulary_concepts(meta["url"], "xml")
            elif meta["format"] == "turtle":
                concepts = parse_vocabulary_concepts(meta["url"], "turtle")
        else:
            print(f"Warning: No local file or remote URL found for '{stem}'")

        print(f"  -> Found {len(concepts)} concepts for {stem}")

        for predicate in predicates:
            existing = vocabulary_dict.get(predicate, set())
            vocabulary_dict[predicate] = existing.union(concepts)

    # Convert sets to sorted lists for JSON serialization
    serialized = {k: sorted(v) for k, v in sorted(vocabulary_dict.items())}

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serialized, f, indent=2, ensure_ascii=False)

    print(
        f"\nSuccessfully saved vocabularies to {output_path} ({len(serialized)} properties mapped)"
    )
    return serialized


if __name__ == "__main__":
    generate_vocabulary_dict()
