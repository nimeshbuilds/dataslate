"""DataSlate CLI. Every resource operation uses the same API as the workspace."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

from .client import Client, DataSlateError


def _file(path):
    contents = json.loads(Path(path).read_text())
    if not isinstance(contents, dict):
        raise ValueError("Input file must contain a JSON object")
    return contents


def parser():
    result = argparse.ArgumentParser(prog="dataslate", description="DataSlate control-plane CLI")
    result.add_argument("--url", default=os.environ.get("DATASLATE_API_URL", "http://127.0.0.1:8080"))
    result.add_argument("--timeout", type=float, default=30)
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    commands.add_parser("modules")
    workspace = commands.add_parser("workspace").add_subparsers(dest="action", required=True)
    workspace.add_parser("list")
    create = workspace.add_parser("create")
    create.add_argument("--name", required=True)
    create.add_argument("--slug", required=True)
    notebooks = commands.add_parser("notebook").add_subparsers(dest="action", required=True)
    for action in ("list", "get", "create", "update"):
        command = notebooks.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"get", "update"}:
            command.add_argument("--id", required=True)
        if action in {"create", "update"}:
            command.add_argument("--file", required=True, help="JSON body; update must include the current revision")
    deployment = commands.add_parser("deployment").add_subparsers(dest="action", required=True)
    deployment.add_parser("plan").add_argument("--file", required=True)
    spark = commands.add_parser("spark").add_subparsers(dest="action", required=True)
    preview = spark.add_parser("preview")
    preview.add_argument("--workspace", required=True)
    preview.add_argument("--file", required=True)
    pipelines = commands.add_parser("pipeline", help="Versioned incremental file-to-Delta ingestion").add_subparsers(dest="action", required=True)
    for action in ("list", "get", "create", "update", "delete"):
        command = pipelines.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"get", "update", "delete"}:
            command.add_argument("--id", required=True)
        if action == "delete":
            command.add_argument("--revision", required=True, type=int, help="Expected saved revision; stale deletion is rejected")
        if action in {"create", "update"}:
            command.add_argument("--file", required=True, help="JSON body; update must include the current revision")
    runs = commands.add_parser("run", help="Granted applications, notebooks, ingestion and durable run state").add_subparsers(dest="action", required=True)
    for action in ("bindings", "list", "submit", "submit-notebook", "submit-pipeline", "get", "cancel", "events", "logs", "source", "watch"):
        command = runs.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"get", "cancel", "events", "logs", "source", "watch"}:
            command.add_argument("--id", required=True)
        if action in {"submit", "submit-notebook", "submit-pipeline"}:
            command.add_argument("--binding", required=True)
            command.add_argument("--idempotency-key", required=True, help="Persist before submission; reuse after an uncertain outcome")
        if action == "submit-notebook":
            command.add_argument("--notebook", required=True, help="Saved notebook ID in this workspace")
            command.add_argument("--revision", required=True, type=int, help="Exact saved revision; never automatically replaced with latest")
        if action == "submit-pipeline":
            command.add_argument("--pipeline", required=True, help="Saved ingestion pipeline ID in this workspace")
            command.add_argument("--revision", required=True, type=int, help="Exact saved pipeline revision")
        if action == "watch":
            command.add_argument("--interval", type=float, default=2)
            command.add_argument("--max-wait", type=float, default=300)
            command.add_argument("--wait-cleanup", action="store_true", help="Also wait for source cleanup and ingestion writer release")
    queries = commands.add_parser("query", help="Governed SQL cells and owner-scoped results").add_subparsers(dest="action", required=True)
    for action in ("bindings", "list", "submit", "get", "source", "result", "cancel", "watch"):
        command = queries.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"get", "source", "result", "cancel", "watch"}:
            command.add_argument("--id", required=True)
        if action == "submit":
            command.add_argument("--binding", required=True)
            command.add_argument("--notebook", required=True)
            command.add_argument("--revision", required=True, type=int)
            command.add_argument("--cell", required=True)
            command.add_argument("--idempotency-key", required=True, help="Persist before submission; reuse the exact selection after an uncertain outcome")
        if action == "watch":
            command.add_argument("--interval", type=float, default=2)
            command.add_argument("--max-wait", type=float, default=300)
    catalogs = commands.add_parser("catalog", help="Authorized Iceberg catalog metadata").add_subparsers(dest="action", required=True)
    for action in ("bindings", "verify", "namespaces", "tables", "table"):
        command = catalogs.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action != "bindings":
            command.add_argument("--binding", required=True)
        if action in {"tables", "table"}:
            command.add_argument("--namespace", required=True, help='JSON array, for example ["analytics","finance"]')
        if action == "table":
            command.add_argument("--name", required=True)
    search = commands.add_parser("search", help="Authorized lexical or hybrid semantic search").add_subparsers(dest="action", required=True)
    for action in ("status", "query", "index-notebook", "index-table", "job", "watch"):
        command = search.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"query", "index-notebook", "index-table"}:
            command.add_argument("--binding", required=True)
        if action == "query":
            command.add_argument("--query", required=True)
            command.add_argument("--mode", choices=("lexical", "hybrid"), default="lexical",
                                 help="Hybrid transfers query text to the configured embedding provider")
            command.add_argument("--limit", type=int, default=10)
        if action in {"index-notebook", "index-table"}:
            command.add_argument("--idempotency-key", required=True, help="Persist the selection/key before submission; reuse after an uncertain response")
        if action == "index-notebook":
            command.add_argument("--notebook", required=True)
            command.add_argument("--revision", type=int, required=True)
        if action == "index-table":
            command.add_argument("--catalog-binding", required=True)
            command.add_argument("--namespace", required=True, help="JSON array of namespace components")
            command.add_argument("--name", required=True)
        if action in {"job", "watch"}:
            command.add_argument("--id", required=True)
        if action == "watch":
            command.add_argument("--interval", type=float, default=2)
            command.add_argument("--max-wait", type=float, default=300)
    sylon = commands.add_parser("sylon", help="Private assistant conversations and explicit code proposals").add_subparsers(dest="action", required=True)
    for action in ("bindings", "conversations", "create", "conversation", "delete", "send", "get", "cancel", "apply", "watch"):
        command = sylon.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"conversation", "delete", "send"}:
            command.add_argument("--conversation", required=True)
        if action == "create":
            command.add_argument("--binding", required=True)
            command.add_argument("--title", required=True)
        if action == "send":
            command.add_argument("--file", required=True, help="JSON prompt, persisted idempotencyKey and optional exact notebook/catalog/search/memory context")
        if action in {"get", "cancel", "apply", "watch"}:
            command.add_argument("--id", required=True)
        if action == "apply":
            command.add_argument("--notebook", required=True)
            command.add_argument("--revision", type=int, required=True)
            command.add_argument("--cell", required=True)
        if action == "watch":
            command.add_argument("--interval", type=float, default=2)
            command.add_argument("--max-wait", type=float, default=300)
    memories = sylon.add_parser("memory", help="Private reviewed memory; local recall does not call a model").add_subparsers(dest="memory_action", required=True)
    for action in ("list", "get", "save", "correct", "delete", "recall"):
        command = memories.add_parser(action)
        command.add_argument("--workspace", required=True)
        if action in {"get", "correct", "delete"}:
            command.add_argument("--id", required=True)
        if action in {"save", "correct", "delete"}:
            command.add_argument("--file", required=True, help="Exact reviewed mutation JSON including a persistent idempotencyKey")
        if action == "recall":
            command.add_argument("--binding", required=True)
            command.add_argument("--query", required=True)
            command.add_argument("--limit", type=int, default=4)
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        client = Client(args.url, token=os.environ.get("DATASLATE_API_TOKEN"), timeout=args.timeout)
        if args.command == "status":
            output = client.status()
        elif args.command == "modules":
            output = client.modules()
        elif args.command == "workspace":
            output = client.workspaces() if args.action == "list" else client.create_workspace(args.name, args.slug)
        elif args.command == "notebook":
            if args.action == "list":
                output = client.notebooks(args.workspace)
            elif args.action == "get":
                output = client.notebook(args.workspace, args.id)
            elif args.action == "create":
                output = client.create_notebook(args.workspace, _file(args.file))
            else:
                output = client.update_notebook(args.workspace, args.id, _file(args.file))
        elif args.command == "deployment":
            output = client.deployment_plan(_file(args.file))
        elif args.command == "pipeline":
            if args.action == "list":
                output = client.ingestion_pipelines(args.workspace)
            elif args.action == "get":
                output = client.ingestion_pipeline(args.workspace, args.id)
            elif args.action == "create":
                output = client.create_ingestion_pipeline(args.workspace, _file(args.file))
            elif args.action == "update":
                output = client.update_ingestion_pipeline(args.workspace, args.id, _file(args.file))
            else:
                output = client.delete_ingestion_pipeline(args.workspace, args.id, args.revision)
        elif args.command == "catalog":
            if args.action == "bindings":
                output = client.catalog_bindings(args.workspace)
            elif args.action in {"verify", "namespaces"}:
                output = (client.verify_catalog if args.action == "verify" else client.catalog_namespaces)(args.workspace, args.binding)
            elif args.action == "tables":
                output = client.catalog_tables(args.workspace, args.binding, json.loads(args.namespace))
            else:
                output = client.catalog_table(args.workspace, args.binding, json.loads(args.namespace), args.name)
        elif args.command == "sylon":
            if args.action == "watch":
                for state in client.watch_sylon_job(args.workspace,args.id,interval=args.interval,max_wait=args.max_wait):
                    print(json.dumps(state,ensure_ascii=False),flush=True)
                return 0 if state["state"] == "succeeded" else 3
            if args.action == "memory":
                if args.memory_action == "list":
                    output = client.sylon_memories(args.workspace)
                elif args.memory_action == "get":
                    output = client.sylon_memory(args.workspace, args.id)
                elif args.memory_action == "recall":
                    output = client.recall_sylon_memories(args.workspace, args.binding, args.query, limit=args.limit)
                else:
                    body = _file(args.file)
                    required = {"idempotencyKey", "revision"} if args.memory_action == "delete" else {
                        "idempotencyKey", "title", "content", "expiresAt",
                        "source" if args.memory_action == "save" else "revision"}
                    if set(body) != required:
                        raise ValueError("Memory file must contain exactly: " + ", ".join(sorted(required)))
                    if args.memory_action == "save":
                        source = body["source"]
                        if not isinstance(source, dict) or set(source) != {"jobId", "revision"}:
                            raise ValueError("Memory source requires exactly jobId and revision")
                        output = client.save_sylon_memory(args.workspace, source["jobId"], source["revision"],
                            body["title"], body["content"], body["expiresAt"], idempotency_key=body["idempotencyKey"])
                    elif args.memory_action == "correct":
                        output = client.correct_sylon_memory(args.workspace, args.id, body["revision"],
                            body["title"], body["content"], body["expiresAt"], idempotency_key=body["idempotencyKey"])
                    else:
                        output = client.delete_sylon_memory(args.workspace, args.id, body["revision"],
                            idempotency_key=body["idempotencyKey"])
            elif args.action in {"bindings", "conversations"}:
                output = (client.sylon_bindings if args.action == "bindings" else client.sylon_conversations)(args.workspace)
            elif args.action == "create":
                output = client.create_sylon_conversation(args.workspace,args.binding,args.title)
            elif args.action in {"conversation", "delete"}:
                output = (client.sylon_conversation if args.action == "conversation" else client.delete_sylon_conversation)(args.workspace,args.conversation)
            elif args.action == "send":
                body = _file(args.file)
                if not {"prompt", "idempotencyKey"} <= set(body) or set(body) - {"prompt", "idempotencyKey", "notebook", "catalogs", "search", "memories"}:
                    raise ValueError("Message file requires prompt/idempotencyKey and optional notebook/catalogs/search/memories only")
                output = client.send_sylon_message(args.workspace,args.conversation,body["prompt"],idempotency_key=body["idempotencyKey"],
                                                   notebook=body.get("notebook"),catalogs=body.get("catalogs"),search=body.get("search"),memories=body.get("memories"))
            elif args.action == "apply":
                output = client.apply_sylon_proposal(args.workspace,args.id,args.notebook,args.revision,args.cell)
            else:
                output = (client.sylon_job if args.action == "get" else client.cancel_sylon_job)(args.workspace,args.id)
        elif args.command == "search":
            if args.action == "watch":
                for state in client.watch_search_index(args.workspace, args.id, interval=args.interval, max_wait=args.max_wait):
                    print(json.dumps(state, ensure_ascii=False), flush=True)
                return 0 if state["state"] == "succeeded" else 3
            if args.action == "status":
                output = client.search_status(args.workspace)
            elif args.action == "query":
                output = client.search(args.workspace, args.binding, args.query, mode=args.mode, limit=args.limit)
            elif args.action == "index-notebook":
                output = client.index_notebook(args.workspace, args.binding, args.notebook, args.revision,
                                               idempotency_key=args.idempotency_key)
            elif args.action == "index-table":
                output = client.index_catalog_table(args.workspace, args.binding, args.catalog_binding,
                                                    json.loads(args.namespace), args.name, idempotency_key=args.idempotency_key)
            else:
                output = client.search_index_job(args.workspace, args.id)
        elif args.command == "query":
            if args.action == "watch":
                for state in client.watch_query(args.workspace, args.id, interval=args.interval, max_wait=args.max_wait):
                    print(json.dumps(state, ensure_ascii=False), flush=True)
                return 0 if state["state"] == "succeeded" else 3
            if args.action == "submit":
                output = client.create_query(args.workspace, args.binding, args.notebook, args.revision,
                                             args.cell, idempotency_key=args.idempotency_key)
            elif args.action in {"bindings", "list"}:
                output = (client.sql_bindings if args.action == "bindings" else client.queries)(args.workspace)
            else:
                output = {"get": client.query, "source": client.query_source, "result": client.query_result,
                          "cancel": client.cancel_query}[args.action](args.workspace, args.id)
        elif args.command == "run":
            if args.action == "watch":
                for state in client.watch_run(args.workspace, args.id, interval=args.interval, max_wait=args.max_wait,
                                              wait_for_cleanup=args.wait_cleanup):
                    print(json.dumps(state, ensure_ascii=False), flush=True)
                return 0 if state["state"] == "succeeded" else 3
            if args.action == "submit":
                output = client.create_run(args.workspace, args.binding, idempotency_key=args.idempotency_key)
            elif args.action == "submit-notebook":
                output = client.create_notebook_run(args.workspace, args.binding, args.notebook,
                                                    args.revision, idempotency_key=args.idempotency_key)
            elif args.action == "submit-pipeline":
                output = client.create_ingestion_run(args.workspace, args.binding, args.pipeline,
                                                     args.revision, idempotency_key=args.idempotency_key)
            elif args.action in {"bindings", "list"}:
                output = (client.execution_bindings if args.action == "bindings" else client.runs)(args.workspace)
            else:
                operation = {"get": client.run, "cancel": client.cancel_run,
                             "events": client.run_events, "logs": client.run_logs, "source": client.run_source}[args.action]
                output = operation(args.workspace, args.id)
        else:
            output = client.spark_preview(args.workspace, _file(args.file))
        print(json.dumps(output, indent=2, ensure_ascii=False))
        return 0
    except DataSlateError as exc:
        print(json.dumps({"error": {"code": exc.code, "message": str(exc), "status": exc.status}}), file=sys.stderr)
        return 1
    except (ValueError, OSError) as exc:
        print(json.dumps({"error": {"code": "invalid_input", "message": str(exc)}}), file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(json.dumps({"error": {"code": "interrupted", "message": "Client interrupted; backend runs are not cancelled"}}), file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
