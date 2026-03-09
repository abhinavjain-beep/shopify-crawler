"""
NotebookLM API client.

Wraps Google's NotebookLM REST API with async methods for use by the MCP server.

API reference: https://notebooklm.google.com/
"""

import asyncio
from typing import Any

import httpx
from google.oauth2.credentials import Credentials

NOTEBOOKLM_API_BASE = "https://notebooklm.googleapis.com/v1"


class NotebookLMClient:
    """Async client for the NotebookLM API."""

    def __init__(self, credentials: Credentials) -> None:
        self._credentials = credentials

    def _get_headers(self) -> dict[str, str]:
        """Return authorization headers, refreshing the token if needed."""
        from google.auth.transport.requests import Request

        if self._credentials.expired and self._credentials.refresh_token:
            self._credentials.refresh(Request())

        return {
            "Authorization": f"Bearer {self._credentials.token}",
            "Content-Type": "application/json",
        }

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: Any = None,
    ) -> Any:
        url = f"{NOTEBOOKLM_API_BASE}{path}"
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.request(
                method,
                url,
                headers=self._get_headers(),
                params=params,
                json=json,
            )
            response.raise_for_status()
            return response.json()

    # ------------------------------------------------------------------
    # Notebook operations
    # ------------------------------------------------------------------

    async def list_notebooks(self) -> list[dict]:
        """List all notebooks in the account."""
        data = await self._request("GET", "/notebooks")
        notebooks = data.get("notebooks", [])
        return [
            {
                "id": nb.get("name", "").split("/")[-1],
                "title": nb.get("title", "Untitled"),
                "created": nb.get("createTime", ""),
                "updated": nb.get("updateTime", ""),
                "source_count": nb.get("sourceCount", 0),
            }
            for nb in notebooks
        ]

    async def get_notebook(self, notebook_id: str) -> dict:
        """Get details about a specific notebook."""
        data = await self._request("GET", f"/notebooks/{notebook_id}")
        return {
            "id": notebook_id,
            "title": data.get("title", "Untitled"),
            "description": data.get("description", ""),
            "created": data.get("createTime", ""),
            "updated": data.get("updateTime", ""),
            "source_count": data.get("sourceCount", 0),
        }

    async def create_notebook(
        self, title: str, description: str | None = None
    ) -> dict:
        """Create a new notebook."""
        body: dict[str, Any] = {"title": title}
        if description:
            body["description"] = description
        data = await self._request("POST", "/notebooks", json=body)
        notebook_id = data.get("name", "").split("/")[-1]
        return {"id": notebook_id, "title": data.get("title", title)}

    # ------------------------------------------------------------------
    # Source operations
    # ------------------------------------------------------------------

    async def list_sources(self, notebook_id: str) -> list[dict]:
        """List all sources within a notebook."""
        data = await self._request("GET", f"/notebooks/{notebook_id}/sources")
        sources = data.get("sources", [])
        return [
            {
                "id": s.get("name", "").split("/")[-1],
                "title": s.get("title", "Untitled"),
                "type": s.get("sourceType", "unknown"),
                "created": s.get("createTime", ""),
            }
            for s in sources
        ]

    async def add_source(
        self,
        notebook_id: str,
        source_type: str,
        content: str,
        title: str | None = None,
    ) -> dict:
        """Add a source to a notebook."""
        body: dict[str, Any] = {}

        if source_type == "url":
            body["webUrl"] = {"uri": content}
        elif source_type == "text":
            body["passtedText"] = {"text": content, "title": title or "Pasted text"}
        elif source_type == "drive_file":
            body["googleDriveDoc"] = {"docId": content}
        else:
            raise ValueError(f"Unsupported source_type: {source_type!r}")

        if title and source_type != "text":
            body["title"] = title

        data = await self._request(
            "POST", f"/notebooks/{notebook_id}/sources", json=body
        )
        source_id = data.get("name", "").split("/")[-1]
        return {
            "id": source_id,
            "title": data.get("title", title or content[:80]),
            "type": source_type,
        }

    # ------------------------------------------------------------------
    # Query / generation
    # ------------------------------------------------------------------

    async def query_notebook(self, notebook_id: str, query: str) -> dict:
        """Send a query to a notebook and get a grounded response."""
        body = {"query": query}
        data = await self._request(
            "POST", f"/notebooks/{notebook_id}:query", json=body
        )
        return {
            "answer": data.get("answer", ""),
            "citations": [
                {
                    "source_id": c.get("sourceId", ""),
                    "source_title": c.get("sourceTitle", ""),
                    "snippet": c.get("snippet", ""),
                }
                for c in data.get("citations", [])
            ],
        }

    # ------------------------------------------------------------------
    # Notes
    # ------------------------------------------------------------------

    async def get_notes(self, notebook_id: str) -> list[dict]:
        """Retrieve all notes from a notebook."""
        data = await self._request("GET", f"/notebooks/{notebook_id}/notes")
        notes = data.get("notes", [])
        return [
            {
                "id": n.get("name", "").split("/")[-1],
                "title": n.get("title", ""),
                "content": n.get("content", ""),
                "created": n.get("createTime", ""),
                "updated": n.get("updateTime", ""),
            }
            for n in notes
        ]
