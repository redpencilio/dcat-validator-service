from helpers import query as _query
from helpers import update as _update


def query_sudo(the_query, scope=None):
    """Execute the given SPARQL query (select/ask/construct)on the triple store and returns
    the results in the given returnFormat (JSON by default)."""
    return _query(the_query, sudo=True, scope=scope)


def update_sudo(the_query, scope=None):
    """Execute the given update SPARQL query on the triple store,
    if the given query is no update query, nothing happens."""
    return _update(the_query, sudo=True, scope=scope)
