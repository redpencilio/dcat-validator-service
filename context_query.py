import contextlib
from contextvars import ContextVar

from flask import request

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
