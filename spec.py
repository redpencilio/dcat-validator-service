from __future__ import annotations

from enum import Enum


class SpecVersion(str, Enum):
    V1_1_0 = "1.1.0"
    V3_0_0 = "3.0.0"

    @classmethod
    def from_value(cls, value) -> SpecVersion:
        if value == "3.0.0":
            return cls.V3_0_0
        return cls.V1_1_0


DCAT_CLASSES_VERSIONED: dict[SpecVersion, list[str]] = {
    SpecVersion.V1_1_0: [
        "http://www.w3.org/ns/dcat#Catalog",
        "http://www.w3.org/ns/dcat#Dataset",
        "http://www.w3.org/ns/dcat#Distribution",
        "http://www.w3.org/ns/dcat#CatalogRecord",
    ],
    SpecVersion.V3_0_0: [
        "http://www.w3.org/ns/dcat#Catalog",
        "http://www.w3.org/ns/dcat#Dataset",
        "http://www.w3.org/ns/dcat#Distribution",
        "http://www.w3.org/ns/dcat#CatalogRecord",
        "http://www.w3.org/ns/dcat#DatasetSeries",
    ],
}

SHACL_BASE_PATH = "/app/shacl-files"

SHACL_FILES_VERSIONED: dict[SpecVersion, list[str]] = {
    SpecVersion.V1_1_0: [
        "v1.1.0/mobilitydcat-ap_1.1.0_shacl_shapes.ttl",
        # "v1.1.0/dcat-ap_2.0.1_shacl_shapes.ttl",
        # "v1.1.0/mobilitydcat-ap_1.1.0_shacl_range.ttl",
        # "v1.1.0/mobilitydcat-ap_1.1.0_shacl_mdr-vocabularies.shape.ttl" --> This is handled by python scriptes now
    ],
    SpecVersion.V3_0_0: [
        "v3.0.0/mobilitydcat-ap-shacl.ttl",
        # "v3.0.0/dqv.ttl",
        # "v3.0.0/dcat-ap-SHACL.ttl",
        # "v3.0.0/mobilitydcat-ap-shacl-ranges.ttl",
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
# 1.1.0: https://mobilitydcat-ap.github.io/mobilityDCAT-AP/releases/index.html
# 3.0.0: https://mobilitydcat-ap.github.io/mobilityDCAT-AP/drafts/latest/index.html
MOBILITY_DCAT_AP_SPEC_VERSIONED: dict[
    SpecVersion, dict[str, dict[Requirement, list[str]]]
] = {
    SpecVersion.V1_1_0: {
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
    },
    SpecVersion.V3_0_0: {
        "http://www.w3.org/ns/dcat#Catalog": {
            Requirement.MANDATORY: [
                "http://purl.org/dc/terms/description",
                "http://purl.org/dc/terms/spatial",
                "http://xmlns.com/foaf/0.1/homepage",
                "http://purl.org/dc/terms/publisher",
                "http://www.w3.org/ns/dcat#record",
                "http://purl.org/dc/terms/title",
            ],
            Requirement.OPTIONAL: [
                "http://purl.org/dc/terms/language",
                "http://purl.org/dc/terms/license",
                "http://purl.org/dc/terms/modified",
                "http://purl.org/dc/terms/issued",
                "http://purl.org/dc/terms/hasPart",
                "http://www.w3.org/ns/dcat#themeTaxonomy",
                "http://www.w3.org/ns/dcat#dataset",
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
            Requirement.OPTIONAL: [
                "https://w3id.org/mobilitydcat-ap#georeferencingMethod",
                "http://www.w3.org/ns/dcat#contactPoint",
                "http://www.w3.org/ns/dcat#keyword",
                "https://w3id.org/mobilitydcat-ap#networkCoverage",
                "http://purl.org/dc/terms/conformsTo",
                "http://purl.org/dc/terms/rightsHolder",
                "http://purl.org/dc/terms/temporal",
                "http://www.w3.org/ns/dcat#theme",
                "https://w3id.org/mobilitydcat-ap#transportMode",
                "http://data.europa.eu/r5r/applicableLegislation",
                "https://w3id.org/mobilitydcat-ap#assessmentResult",
                "http://www.w3.org/ns/dcat#hasVersion",
                "http://www.w3.org/ns/dqv#hasQualityAnnotation",
                "http://purl.org/dc/terms/identifier",
                "http://www.w3.org/ns/adms#identifier",
                "https://w3id.org/mobilitydcat-ap#intendedInformationService",
                "http://www.w3.org/ns/dcat#inSeries",
                "http://purl.org/dc/terms/issued",
                "http://purl.org/dc/terms/language",
                "http://purl.org/dc/terms/modified",
                "http://xmlns.com/foaf/0.1/page",
                "http://data.europa.eu/930/referenceSystem",
                "http://purl.org/dc/terms/relation",
                "http://purl.org/dc/terms/source",
                "http://www.w3.org/ns/dcat#version",
                "http://www.w3.org/ns/adms#versionNotes",
            ],
        },
        "http://www.w3.org/ns/dcat#Distribution": {
            Requirement.MANDATORY: [
                "http://www.w3.org/ns/dcat#accessURL",
                "https://w3id.org/mobilitydcat-ap#mobilityDataStandard",
                "http://purl.org/dc/terms/format",
                "http://purl.org/dc/terms/rights",
            ],
            Requirement.OPTIONAL: [
                "http://purl.org/dc/terms/title",
                "http://data.europa.eu/r5r/applicableLegislation",
                "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol",
                "http://purl.org/dc/terms/description",
                "http://purl.org/dc/terms/license",
                "http://www.w3.org/2011/content#characterEncoding",
                "https://w3id.org/mobilitydcat-ap#communicationMethod",
                "http://purl.org/dc/terms/conformsTo",
                "https://w3id.org/mobilitydcat-ap#dataFormatNotes",
                "http://xmlns.com/foaf/0.1/page",
                "http://www.w3.org/ns/dcat#downloadURL",
                "http://purl.org/dc/terms/issued",
                "http://purl.org/dc/terms/language",
                "http://purl.org/dc/terms/modified",
                "http://www.w3.org/ns/adms#sample",
                "http://www.w3.org/ns/adms#status",
                "http://purl.org/dc/terms/temporal",
            ],
        },
        "http://www.w3.org/ns/dcat#CatalogRecord": {
            Requirement.MANDATORY: [
                "http://purl.org/dc/terms/issued",
                "http://purl.org/dc/terms/language",
                "http://purl.org/dc/terms/modified",
                "http://xmlns.com/foaf/0.1/primaryTopic",
            ],
            Requirement.OPTIONAL: [
                "http://purl.org/dc/terms/publisher",
                "http://purl.org/dc/terms/source",
                "http://purl.org/dc/terms/conformsTo",
            ],
        },
        "http://www.w3.org/ns/dcat#DatasetSeries": {
            Requirement.MANDATORY: [
                "http://purl.org/dc/terms/description",
                "http://purl.org/dc/terms/title",
            ],
            Requirement.OPTIONAL: [
                "http://purl.org/dc/terms/accrualPeriodicity",
                "http://data.europa.eu/r5r/applicableLegislation",
                "http://www.w3.org/ns/dcat#contactPoint",
                "http://purl.org/dc/terms/publisher",
                "http://purl.org/dc/terms/issued",
                "https://w3id.org/mobilitydcat-ap#mobilityTheme",
                "http://purl.org/dc/terms/modified",
                "http://purl.org/dc/terms/spatial",
                "http://purl.org/dc/terms/temporal",
            ],
        },
    },
}

STEM_PROPERTY_MAPPING: dict[str, list[str]] = {
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
    "distribution-status-skos": ["http://www.w3.org/ns/adms#status"],
    # "corporatebodies-skos": ["http://purl.org/dc/terms/publisher"],
}


class VocabularyPolicy(str, Enum):
    REQUIRED = "required"
    RECOMMENDED = "recommended"
    AT_LEAST_1 = "at-least-1"
    OPTIONAL = "optional"

    def to_severity(self) -> Requirement:
        if self == VocabularyPolicy.REQUIRED or self.name == 'AT_LEAST_1': return Requirement.MANDATORY
        elif self == VocabularyPolicy.OPTIONAL: return Requirement.OPTIONAL
        else: return Requirement.RECOMMENDED



PROPERTY_POLICY_MAPPING_VERSIONED: dict[SpecVersion, dict[str, VocabularyPolicy]] = {
    SpecVersion.V1_1_0: {
        "https://w3id.org/mobilitydcat-ap#transportMode": VocabularyPolicy.OPTIONAL,
        "https://w3id.org/mobilitydcat-ap#mobilityTheme": VocabularyPolicy.REQUIRED,
        "https://w3id.org/mobilitydcat-ap#mobilityDataStandard": VocabularyPolicy.REQUIRED,
        "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol": VocabularyPolicy.OPTIONAL,
        "https://w3id.org/mobilitydcat-ap#communicationMethod": VocabularyPolicy.OPTIONAL,
        "https://w3id.org/mobilitydcat-ap#networkCoverage": VocabularyPolicy.OPTIONAL,
        "https://w3id.org/mobilitydcat-ap#georeferencingMethod": VocabularyPolicy.REQUIRED,
        "https://w3id.org/mobilitydcat-ap#intendedInformationService": VocabularyPolicy.OPTIONAL,
        "https://w3id.org/mobilitydcat-ap#grammar": VocabularyPolicy.OPTIONAL,
        "http://www.w3.org/ns/dcat#theme": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/accrualPeriodicity": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/format": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/language": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/spatial": VocabularyPolicy.REQUIRED,
    },
    SpecVersion.V3_0_0: {
        "https://w3id.org/mobilitydcat-ap#transportMode": VocabularyPolicy.AT_LEAST_1,
        "https://w3id.org/mobilitydcat-ap#mobilityTheme": VocabularyPolicy.AT_LEAST_1,
        "https://w3id.org/mobilitydcat-ap#mobilityDataStandard": VocabularyPolicy.AT_LEAST_1,
        "https://w3id.org/mobilitydcat-ap#applicationLayerProtocol": VocabularyPolicy.AT_LEAST_1,
        "https://w3id.org/mobilitydcat-ap#communicationMethod": VocabularyPolicy.RECOMMENDED,
        "https://w3id.org/mobilitydcat-ap#networkCoverage": VocabularyPolicy.AT_LEAST_1,
        "https://w3id.org/mobilitydcat-ap#georeferencingMethod": VocabularyPolicy.AT_LEAST_1,
        "https://w3id.org/mobilitydcat-ap#intendedInformationService": VocabularyPolicy.AT_LEAST_1,
        "http://www.w3.org/ns/dcat#theme": VocabularyPolicy.AT_LEAST_1,
        "http://purl.org/dc/terms/accrualPeriodicity": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/format": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/language": VocabularyPolicy.REQUIRED,
        "http://purl.org/dc/terms/spatial": VocabularyPolicy.RECOMMENDED,
        "http://www.w3.org/ns/adms#status": VocabularyPolicy.REQUIRED,
    },
}
