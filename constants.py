import os

GRAPH_LOAD_BATCH_SIZE = 100

MU_APPLICATION_GRAPH = os.environ.get("MU_APPLICATION_GRAPH", "http://mu.semte.ch/graphs/public")
MU_SPARQL_ENDPOINT = os.environ.get("MU_SPARQL_ENDPOINT")

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

VALIDATION_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/validation-summary/"
TARGET_CLASS_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/target-class-summary/"
RULE_SUMMARY_URI_PREFIX = "http://redpencil.data.gift/id/rule-summary/"
