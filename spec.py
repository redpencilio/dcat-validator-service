from __future__ import annotations

from enum import Enum

DCAT_CLASSES = [
    "http://www.w3.org/ns/dcat#Catalog",
    "http://www.w3.org/ns/dcat#Dataset",
    "http://www.w3.org/ns/dcat#Distribution",
    "http://www.w3.org/ns/dcat#CatalogRecord",
]

SHACL_BASE_PATH = "/app/shacl-files"

SHACL_FILES_VERSIONED: dict[str, list[str]] = {
    "1.1.0": [
        "v1.1.0/mobilitydcat-ap_1.1.0_shacl_shapes.ttl",
        "v1.1.0/mobilitydcat-ap_1.1.0_shacl_range.ttl",
        #"v1.1.0/mobilitydcat-ap_1.1.0_shacl_mdr-vocabularies.shape.ttl" --> This is handled by python scriptes now
    ],
    "3.0.0": [
        "v3.0.0/mobilitydcat-ap-shacl.ttl",
        "v3.0.0/mobilitydcat-ap-shacl-ranges.ttl",
    ],
}

class Requirement(str, Enum):
    MANDATORY = "mandatory"
    RECOMMENDED = "recommended"
    OPTIONAL = "optional"


SEVERITY = {
    Requirement.MANDATORY: "http://www.w3.org/ns/shacl#Violation",
    Requirement.RECOMMENDED: "http://www.w3.org/ns/shacl#Warning",
    Requirement.OPTIONAL: "http://www.w3.org/ns/shacl#Info",
}

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
            "http://data.europa.eu/r5r/applicableLegislation",
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

VOC_PROPERTY_MAPPING = {
    # File Stem: Predicate URI(s)
    "transport-mode": ["https://w3id.org/mobilitydcat-ap#transportMode"],
    "mobility-theme": ["https://w3id.org/mobilitydcat-ap#mobilityTheme"],
    "mobility-data-standard": ["https://w3id.org/mobilitydcat-ap#mobilityDataStandard"],
    "application-layer-protocol": [
        "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol"
    ],
    "communication-method": ["https://w3id.org/mobilitydcat-ap#communicationMethod"],
    "network-coverage": ["https://w3id.org/mobilitydcat-ap#networkCoverage"],
    "georeferencing-method": ["https://w3id.org/mobilitydcat-ap#georeferencingMethod"],
    "intended-information-service": [
        "https://w3id.org/mobilitydcat-ap#intendedInformationService"
    ],
    "grammar": ["https://w3id.org/mobilitydcat-ap#grammar"],
    "data-theme-skos": [
        "http://www.w3.org/ns/dcat#theme",
        "http://www.w3.org/ns/dcat#themeTaxonomy",
    ],
    "frequencies-skos": ["http://purl.org/dc/terms/accrualPeriodicity"],
    "filetypes-skos": ["http://purl.org/dc/terms/format"],
    "languages-skos": ["http://purl.org/dc/terms/language"],
    "continents-skos": ["http://purl.org/dc/terms/spatial"],
    "countries-skos": ["http://purl.org/dc/terms/spatial"],
    "places-skos": ["http://purl.org/dc/terms/spatial"],
    "NUTS": ["http://purl.org/dc/terms/spatial"],
}
