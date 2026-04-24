import contextlib
import datetime
import logging
import os
import sys
import uuid
from contextvars import ContextVar

from escape_helpers import sparql_escape
from flask import jsonify, request
from helpers import LOG_SPARQL_QUERIES, LOG_SPARQL_UPDATES, log
from rdflib.namespace import DC
from SPARQLWrapper import JSON, SPARQLWrapper

"""
The template provides the user with several helper methods. They aim to give you a step ahead for:

- logging
- JSONAPI-compliancy
- SPARQL querying

The below helpers can be imported from the `helpers` module. For example:
```py
from helpers import *
```

Available functions:
"""

MU_APPLICATION_GRAPH = os.environ.get('MU_APPLICATION_GRAPH')

MU_HEADERS = [
    "MU-SESSION-ID",
    "MU-CALL-ID",
    "MU-AUTH-ALLOWED-GROUPS",
    "MU-AUTH-USED-GROUPS"
]

mu_headers = ContextVar('mu_headers')

def _get_mu_headers():
    return {
        header: request.headers[header]
        for header in MU_HEADERS
        if header in request.headers
    }

def get_mu_headers():
    return mu_headers.get(None) or _get_mu_headers()

@contextlib.contextmanager
def use_mu_headers(headers):
    token = mu_headers.set(headers)
    try:
        yield
    finally:
        mu_headers.reset(token)

def session_id_header(request):
    """Returns the MU-SESSION-ID header from the given requests' headers"""
    return get_mu_headers().get('MU-SESSION-ID')


def rewrite_url_header(request):
    """Returns the X-REWRITE-URL header from the given requests' headers"""
    return get_mu_headers().get('X-REWRITE-URL')

sparqlQuery = SPARQLWrapper(os.environ.get('MU_SPARQL_ENDPOINT'), returnFormat=JSON)
sparqlUpdate = SPARQLWrapper(os.environ.get('MU_SPARQL_UPDATEPOINT'), returnFormat=JSON)
sparqlUpdate.method = 'POST'
if os.environ.get('MU_SPARQL_TIMEOUT'):
    timeout = int(os.environ.get('MU_SPARQL_TIMEOUT'))
    sparqlQuery.setTimeout(timeout)
    sparqlUpdate.setTimeout(timeout)

def query(the_query):
    """Execute the given SPARQL query (select/ask/construct) on the triplestore and returns the results in the given return Format (JSON by default)."""
    headers = get_mu_headers()
    for header in MU_HEADERS:
        if header in headers:
            sparqlQuery.customHttpHeaders[header] = headers[header]
        else: # Make sure headers used for a previous query are cleared
            if header in sparqlQuery.customHttpHeaders:
                del sparqlQuery.customHttpHeaders[header]
    sparqlQuery.setQuery(the_query)
    if LOG_SPARQL_QUERIES:
        log("Execute query: \n" + the_query)
    try:
        return sparqlQuery.query().convert()
    except Exception as e:
        log("Failed Query: \n" + the_query)
        raise e


def update(the_query):
    """Execute the given update SPARQL query on the triplestore. If the given query is not an update query, nothing happens."""
    headers = get_mu_headers()
    for header in MU_HEADERS:
        if header in headers:
            sparqlUpdate.customHttpHeaders[header] = headers[header]
        else: # Make sure headers used for a previous query are cleared
            if header in sparqlUpdate.customHttpHeaders:
                del sparqlUpdate.customHttpHeaders[header]
    sparqlUpdate.setQuery(the_query)
    if sparqlUpdate.isSparqlUpdateRequest():
        if LOG_SPARQL_UPDATES:
            log("Execute query: \n" + the_query)
        try:
            sparqlUpdate.query()
        except Exception as e:
            log("Failed Query: \n" + the_query)
            raise e
