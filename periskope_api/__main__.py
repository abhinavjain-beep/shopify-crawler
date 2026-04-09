"""Command-line interface for editing Periskope resources.

Run::

    python -m periskope_api --help

The CLI reads credentials from the ``PERISKOPE_API_KEY`` and
``PERISKOPE_PHONE`` environment variables (or the ``--api-key`` / ``--phone``
flags). Updates are supplied as JSON either inline (``--data '{...}'``) or
from a file (``--data-file updates.json``).

Examples::

    # Reassign a chat
    python -m periskope_api edit chat chat_abc \\
        --data '{"assigned_to": "alice@example.com"}'

    # Update a ticket's status from a file
    python -m periskope_api edit ticket tkt_123 --data-file updates.json

    # Fetch a contact
    python -m periskope_api get contact ctc_999

    # List all editable resource types
    python -m periskope_api resources
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from .client import PeriskopeAPIError
from .editor import PeriskopeEditor


def _load_updates(args: argparse.Namespace) -> dict[str, Any]:
    if args.data and args.data_file:
        raise SystemExit("Use either --data or --data-file, not both.")
    if args.data:
        return json.loads(args.data)
    if args.data_file:
        with open(args.data_file, "r", encoding="utf-8") as fh:
            return json.load(fh)
    raise SystemExit("Provide updates via --data or --data-file.")


def _print_json(value: Any) -> None:
    json.dump(value, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


def _make_editor(args: argparse.Namespace) -> PeriskopeEditor:
    api_key = args.api_key or os.environ.get("PERISKOPE_API_KEY")
    phone = args.phone or os.environ.get("PERISKOPE_PHONE")
    if not api_key or not phone:
        raise SystemExit(
            "Missing credentials. Set PERISKOPE_API_KEY and PERISKOPE_PHONE "
            "environment variables or pass --api-key and --phone."
        )
    return PeriskopeEditor(api_key=api_key, phone=phone)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="periskope_api",
        description="Edit anything on Periskope through its REST API.",
    )
    parser.add_argument("--api-key", help="Periskope API key (overrides env var).")
    parser.add_argument("--phone", help="Organization phone number (overrides env).")

    sub = parser.add_subparsers(dest="command", required=True)

    # resources
    sub.add_parser("resources", help="List editable resource names.")

    # edit
    edit_p = sub.add_parser("edit", help="Patch a resource with a JSON update.")
    edit_p.add_argument("resource", help="Resource type, e.g. chat, ticket, contact.")
    edit_p.add_argument("resource_id", help="ID (or email for members) of the object.")
    edit_p.add_argument("--data", help="Inline JSON object with the updates.")
    edit_p.add_argument("--data-file", help="Path to a JSON file with the updates.")

    # get
    get_p = sub.add_parser("get", help="Fetch a single resource.")
    get_p.add_argument("resource")
    get_p.add_argument("resource_id")

    # delete
    del_p = sub.add_parser("delete", help="Delete a resource.")
    del_p.add_argument("resource")
    del_p.add_argument("resource_id")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "resources":
        editor = _make_editor(args)
        try:
            for name in editor.resources():
                print(name)
        finally:
            editor.close()
        return 0

    editor = _make_editor(args)
    try:
        if args.command == "edit":
            updates = _load_updates(args)
            _print_json(editor.edit(args.resource, args.resource_id, updates))
        elif args.command == "get":
            _print_json(editor.get(args.resource, args.resource_id))
        elif args.command == "delete":
            _print_json(editor.delete(args.resource, args.resource_id))
        else:
            parser.error(f"Unknown command: {args.command}")
    except PeriskopeAPIError as exc:
        print(f"API error: {exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2
    finally:
        editor.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
