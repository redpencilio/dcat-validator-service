import os

GRAPH_LOAD_BATCH_SIZE = 100

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH", "http://mu.semte.ch/graphs/public")
MU_SPARQL_ENDPOINT = os.environ.get("MU_SPARQL_ENDPOINT")

PUBLIC_GRAPH = "http://mu.semte.ch/graphs/public"
DATA_GRAPH = MU_APPLICATION_GRAPH
TASKS_GRAPH = "http://mu.semte.ch/graphs/jobs"

JOB_URI_PREFIX = "http://redpencil.data.gift/id/job/"
TASK_URI_PREFIX = "http://redpencil.data.gift/id/task/"
CONTAINER_URI_PREFIX = "http://redpencil.data.gift/id/container/"

SHACL_VALIDATION_INPUT_URI_PREFIX = "http://mu.semte.ch/vocabularies/ext/shacl-validation-input/"
SHACL_VALIDATION_JOB_OPERATION = "http://lblod.data.gift/id/jobs/concept/JobOperation/validation-job"
SHACL_VALIDATION_OPERATION = "http://mu.semte.ch/vocabularies/ext/ShaclValidationJob"
SHACL_VALIDATION_RESULT_URI_PREFIX = "http://mu.semte.ch/vocabularies/ext/shacl-validation-result/"
SHACL_VALIDATION_RESULT_GRAPH_URI_PREFIX = "http://mu.semte.ch/vocabularies/ext/shacl-validation-result-graph/"

COVERAGE_ANALYSIS_JOB_OPERATION = "http://lblod.data.gift/id/jobs/concept/JobOperation/coverage-analysis-job"
COVERAGE_ANALYSIS_OPERATION = "http://mu.semte.ch/vocabularies/ext/CoverageAnalysisJob"

SHACL_REPORT_PREDICATE = "http://mu.semte.ch/vocabularies/ext/shaclReport"
COVERAGE_REPORT_PREDICATE = "http://mu.semte.ch/vocabularies/ext/coverageReport"

VALIDATION_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/validation-summary/"
TARGET_CLASS_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/target-class-summary/"
RULE_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/rule-summary/"

DCAT_CLASSES = [
    "http://www.w3.org/ns/dcat#Catalog",
    "http://www.w3.org/ns/dcat#Dataset",
    "http://www.w3.org/ns/dcat#Distribution",
    "http://www.w3.org/ns/dcat#CatalogRecord",
]

VOCAB_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/vocabulary-summary/"
CLASS_VOCAB_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/class-vocabulary-summary/"
VOCAB_VIOLATION_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/vocabulary-violation-summary/"

MOBILITY_DCAT_AP_VERSION = "1.1.0"
SHACL_BASE_URL = "https://raw.githubusercontent.com/mobilityDCAT-AP/mobilityDCAT-AP/gh-pages"

# The files dont follow the same pattern across versions (1.1.0 has 3 files, 3.0.0 merges shapes and vocabularies into one file)
if MOBILITY_DCAT_AP_VERSION == "1.1.0":
    # You can find the shacl files for 1.1.0 here:
    # https://github.com/mobilityDCAT-AP/mobilityDCAT-AP/tree/gh-pages/releases/1.1.0/shaclShapes
    SHACL_FILES = [
        "/releases/1.1.0/shaclShapes/mobilitydcat-ap_1.1.0_shacl_shapes.ttl",
        "/releases/1.1.0/shaclShapes/mobilitydcat-ap_1.1.0_shacl_range.ttl",
#        "/releases/1.1.0/shaclShapes/mobilitydcat-ap_1.1.0_shacl_mdr-vocabularies.shape.ttl"
    ]
    # You can find the shacl files for 3.0.0 here:
    # https://github.com/mobilityDCAT-AP/mobilityDCAT-AP/tree/gh-pages/drafts/latest/shaclShapes
elif MOBILITY_DCAT_AP_VERSION == "3.0.0":
    SHACL_FILES = [
        "/drafts/latest/shaclShapes/mobilitydcat-ap-shacl.ttl",
        "/drafts/latest/shaclShapes/mobilitydcat-ap-shacl-ranges.ttl"
    ]
