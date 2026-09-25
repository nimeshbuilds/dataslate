#!/usr/bin/env python3
"""Read-only unauthenticated HTTPS smoke checks; never discovers credentials."""
import argparse
import json
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True, help="DataSlate HTTPS origin, without path or credentials")
    args = parser.parse_args()
    origin = urllib.parse.urlsplit(args.url)
    if origin.scheme != "https" or not origin.hostname or origin.username or origin.password or origin.query or origin.fragment or origin.path not in ["", "/"]:
        parser.error("provide an HTTPS origin without credentials, path, query or fragment")
    base = args.url.rstrip("/")
    # A login redirect or HTTPS downgrade must not masquerade as a healthy API response.
    client = urllib.request.build_opener(NoRedirect())
    for path, expected in [("/web-healthz", 200), ("/healthz", 200), ("/readyz", 200), ("/api/v1/auth/config", 200), ("/api/v1/status", 401)]:
        try:
            with client.open(base + path, timeout=15) as response:
                status, data = response.status, response.read(65536)
        except urllib.error.HTTPError as exc:
            status, data = exc.code, exc.read(65536)
        if status != expected:
            raise SystemExit(f"FAIL {path}: expected HTTP {expected}, received {status}")
        if path == "/api/v1/auth/config":
            config = json.loads(data)
            if config.get("mode") != "oidc" or not config.get("issuer", "").startswith("https://"):
                raise SystemExit("FAIL production OIDC metadata")
        print(f"PASS {path}: HTTP {status}")
    print("Anonymous smoke checks passed. Real login, authorization, failover and restore still require live tests.")


if __name__ == "__main__":
    main()
