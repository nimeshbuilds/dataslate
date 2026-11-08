PYTHON ?= python3

.PHONY: dev-api dev-web test-api test-sdk check-web test test-local

dev-api:
	cd api && DATASLATE_AUTH_MODE=development DATASLATE_ADDR=127.0.0.1:8080 go run ./cmd/dataslate-api

dev-web:
	cd web && npm run dev

test-api:
	cd api && go test -race ./... && go vet ./...

test-sdk:
	PYTHONPATH=sdk/python/src $(PYTHON) -m unittest discover -s sdk/python/tests -v

check-web:
	cd web && npm run build

test: test-api test-sdk check-web

test-local:
	PYTHON="$(PYTHON)" bash scripts/test-local.sh
