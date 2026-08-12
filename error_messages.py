import urllib.error

import SPARQLWrapper.SPARQLExceptions


def format_human_readable_error(e: Exception) -> str:
    # These custom explicit errors starting with 'Input' are thrown when task input (harvested data graph) could not be found
    if str(e).startswith("Input"):
        return (
            "Internal Error: The validator could not locate the harvested data graph."
        )

    elif isinstance(e, MemoryError):
        return "Validation failed because the server ran out of memory."

    elif isinstance(e, RecursionError):
        return "Validation failed due to a recursion error in the SHACL shapes."

    elif isinstance(e, (urllib.error.HTTPError, urllib.error.URLError)):
        return "Validation failed because the mobilityDCAT-AP SHACL shapes could not be downloaded."

    elif isinstance(e, SPARQLWrapper.SPARQLExceptions.EndPointInternalError):
        return "Validation failed because the internal database timed out or crashed during processing."

    else:
        return f"An unexpected error occured during SHACL validation: {e!s}"
