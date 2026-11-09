#!/usr/bin/env python3
"""Read-only unauthenticated HTTPS smoke checks; never discovers credentials."""
import argparse
import json
from pathlib import Path
import ssl
import stat
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="DataSlate HTTPS origin, without path or credentials")
    parser.add_argument("--ca-file", type=Path, help="Additional trusted PEM CA bundle for a private HTTPS ingress; verification remains required")
    args = parser.parse_args()
    try:
        origin = urllib.parse.urlsplit(args.url)
        origin.port
    except ValueError:
        parser.error("provide a valid HTTPS origin")
    if any(c.isspace() for c in args.url) or "\\" in args.url:
        parser.error("provide a valid HTTPS origin")
    if origin.scheme != "https" or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ["", "/"]:
        parser.error("provide an HTTPS origin without credentials, path, query or fragment")
    base = args.url.rstrip("/")
    # A login redirect or HTTPS downgrade must not masquerade as a healthy API response.
    trust = ssl.create_default_context()
    if args.ca_file:
        try:
            info = args.ca_file.stat()
            if not stat.S_ISREG(info.st_mode) or not 1 <= info.st_size <= 1024 * 1024:
                raise ValueError("invalid CA bundle")
            trust.load_verify_locations(cafile=str(args.ca_file))
        except (OSError, ValueError, ssl.SSLError):
            raise SystemExit("FAIL trusted CA bundle: provide a readable PEM certificate file of at most 1 MiB") from None
    client = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=trust))
    for path, expected in [("/web-healthz", 200), ("/healthz", 200), ("/readyz", 200), ("/api/v1/auth/config", 200), ("/api/v1/status", 401)]:
        try:
            with client.open(base + path, timeout=15) as response:
                status, data = response.status, response.read(65537)
        except urllib.error.HTTPError as exc:
            status, data = exc.code, exc.read(65537)
        except (urllib.error.URLError, TimeoutError, OSError):
            raise SystemExit(f"FAIL {path}: HTTPS connection failed; check DNS, routing, certificate trust and hostname") from None
        if len(data) > 65536:
            raise SystemExit(f"FAIL {path}: response exceeds 64 KiB")
        if status != expected:
            raise SystemExit(f"FAIL {path}: expected HTTP {expected}, received {status}")
        if path == "/api/v1/auth/config":
            try:
                config = json.loads(data)
            except (ValueError, UnicodeError):
                raise SystemExit("FAIL production OIDC metadata: invalid JSON") from None
            if not isinstance(config, dict):
                raise SystemExit("FAIL production OIDC metadata: expected an object")
            if config.get("mode") != "oidc" or not isinstance(config.get("issuer"), str) or not config["issuer"].startswith("https://"):
                raise SystemExit("FAIL production OIDC metadata")
        print(f"PASS {path}: HTTP {status}")
    print("Anonymous smoke checks passed. Real login, authorization, failover and restore still require live tests.")


if __name__ == "__main__":
    main()
