#!/bin/sh
set -eu
# This is a deployment setting, never a browser-supplied URL. Restrict it before
# expanding into nginx configuration, and fail closed on malformed values.
: "${DATASLATE_API_UPSTREAM:=http://dataslate-api:8080}"
if ! printf '%s' "$DATASLATE_API_UPSTREAM" | grep -Eq '^https?://[a-zA-Z0-9.-]+(:[0-9]{1,5})?$'; then
  printf '%s\n' 'DATASLATE_API_UPSTREAM must be an http(s) service origin without a path or credentials.' >&2
  exit 1
fi
export DATASLATE_API_UPSTREAM
mkdir -p /tmp/dataslate-nginx/client /tmp/dataslate-nginx/proxy /tmp/dataslate-nginx/fastcgi /tmp/dataslate-nginx/uwsgi /tmp/dataslate-nginx/scgi
envsubst '${DATASLATE_API_UPSTREAM}' < /opt/dataslate/nginx.conf.template > /tmp/dataslate-nginx/nginx.conf
exec nginx -c /tmp/dataslate-nginx/nginx.conf -g 'daemon off;'
