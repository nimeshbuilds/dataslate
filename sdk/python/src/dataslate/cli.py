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


if __name__ == "__main__":
    raise SystemExit(main())
