"""Offline compatibility checks for the shared HTTP dependency upgrades."""

import io
import ssl
import zlib

import certifi
import pytest
import requests
from urllib3.exceptions import DecodeError
from urllib3.response import HTTPResponse
from urllib3.util.ssl_ import create_urllib3_context


def test_requests_certificate_bundle_loads_with_verification_enabled():
    assert requests.certs.where() == certifi.where()
    context = create_urllib3_context()
    context.load_verify_locations(cafile=requests.certs.where())
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname
    assert context.cert_store_stats()["x509_ca"] > 0


def test_deflate_response_preserves_payload_when_read_incrementally():
    payload = b'{"status":"ok"}\n' * 100
    response = HTTPResponse(
        body=io.BytesIO(zlib.compress(payload)),
        headers={"content-encoding": "deflate"},
        preload_content=False,
    )
    with response:
        assert b"".join(response.stream(amt=7, decode_content=True)) == payload


def test_invalid_deflate_response_raises_decode_error():
    response = HTTPResponse(
        body=io.BytesIO(b"invalid compressed response"),
        headers={"content-encoding": "deflate"},
        preload_content=False,
    )
    with response, pytest.raises(DecodeError, match="failed to decode"):
        response.read(decode_content=True)
