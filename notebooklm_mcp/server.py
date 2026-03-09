"""
NotebookLM MCP Server

An MCP (Model Context Protocol) server that connects Claude to Google NotebookLM,
enabling Claude to list notebooks, query sources, and generate content from your
stored documents.

Usage:
    python -m notebooklm_mcp.server

Configuration:
    Set up credentials via `python -m notebooklm_mcp.auth` before running.
"""

import json
import asyncio
import logging
from pathlib import Path
from typing import Any

import mcp.server.stdio
import mcp.types as types
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions

from .auth import NotebookLMAuth
from .client import NotebookLMClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server = Server("notebooklm-mcp")
_client: NotebookLMClient | None = None


def get_client() -> NotebookLMClient:
    global _client
    if _client is None:
        auth = NotebookLMAuth()
        credentials = auth.load_credentials()
        if not credentials:
            raise RuntimeError(
                "No credentials found. Run `python -m notebooklm_mcp.auth` first."
            )
        _client = NotebookLMClient(credentials)
    return _client


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    return [
        types.Tool(
            name="list_notebooks",
            description=(
                "List all notebooks in your NotebookLM account. "
                "Returns notebook IDs, titles, and creation dates."
            ),
            inputSchema={
                "type": "object",
                "properties": {},
                "required": [],
            },
        ),
        types.Tool(
            name="get_notebook",
            description=(
                "Get details about a specific NotebookLM notebook including its "
                "sources, notes, and metadata."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "notebook_id": {
                        "type": "string",
                        "description": "The unique identifier of the notebook.",
                    }
                },
                "required": ["notebook_id"],
            },
        ),
        types.Tool(
            name="list_sources",
            description=(
                "List all sources (documents, URLs, text) within a specific notebook."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "notebook_id": {
                        "type": "string",
                        "description": "The notebook ID to list sources from.",
                    }
                },
                "required": ["notebook_id"],
            },
        ),
        types.Tool(
            name="query_notebook",
            description=(
                "Query a NotebookLM notebook with a question. NotebookLM will "
                "generate a grounded answer based on the notebook's sources."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "notebook_id": {
                        "type": "string",
                        "description": "The notebook ID to query.",
                    },
                    "query": {
                        "type": "string",
                        "description": "The question or query to ask the notebook.",
                    },
                },
                "required": ["notebook_id", "query"],
            },
        ),
        types.Tool(
            name="add_source",
            description=(
                "Add a new source to a NotebookLM notebook. Supports URLs, "
                "plain text content, or Google Drive file IDs."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "notebook_id": {
                        "type": "string",
                        "description": "The notebook ID to add the source to.",
                    },
                    "source_type": {
                        "type": "string",
                        "enum": ["url", "text", "drive_file"],
                        "description": "The type of source to add.",
                    },
                    "content": {
                        "type": "string",
                        "description": (
                            "The URL, text content, or Google Drive file ID "
                            "depending on source_type."
                        ),
                    },
                    "title": {
                        "type": "string",
                        "description": "Optional title for the source.",
                    },
                },
                "required": ["notebook_id", "source_type", "content"],
            },
        ),
        types.Tool(
            name="create_notebook",
            description="Create a new NotebookLM notebook.",
            inputSchema={
                "type": "object",
                "properties": {
                    "title": {
                        "type": "string",
                        "description": "The title for the new notebook.",
                    },
                    "description": {
                        "type": "string",
                        "description": "Optional description for the notebook.",
                    },
                },
                "required": ["title"],
            },
        ),
        types.Tool(
            name="get_notes",
            description="Retrieve all notes saved in a specific NotebookLM notebook.",
            inputSchema={
                "type": "object",
                "properties": {
                    "notebook_id": {
                        "type": "string",
                        "description": "The notebook ID to retrieve notes from.",
                    }
                },
                "required": ["notebook_id"],
            },
        ),
    ]


@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any]
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    try:
        client = get_client()
    except RuntimeError as e:
        return [types.TextContent(type="text", text=f"Error: {e}")]

    try:
        if name == "list_notebooks":
            result = await client.list_notebooks()
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_notebook":
            notebook_id = arguments["notebook_id"]
            result = await client.get_notebook(notebook_id)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "list_sources":
            notebook_id = arguments["notebook_id"]
            result = await client.list_sources(notebook_id)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "query_notebook":
            notebook_id = arguments["notebook_id"]
            query = arguments["query"]
            result = await client.query_notebook(notebook_id, query)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "add_source":
            notebook_id = arguments["notebook_id"]
            source_type = arguments["source_type"]
            content = arguments["content"]
            title = arguments.get("title")
            result = await client.add_source(notebook_id, source_type, content, title)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "create_notebook":
            title = arguments["title"]
            description = arguments.get("description")
            result = await client.create_notebook(title, description)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        elif name == "get_notes":
            notebook_id = arguments["notebook_id"]
            result = await client.get_notes(notebook_id)
            return [types.TextContent(type="text", text=json.dumps(result, indent=2))]

        else:
            return [types.TextContent(type="text", text=f"Unknown tool: {name}")]

    except Exception as e:
        logger.exception(f"Error calling tool {name}")
        return [types.TextContent(type="text", text=f"Error: {e}")]


async def main():
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="notebooklm-mcp",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )


if __name__ == "__main__":
    asyncio.run(main())
