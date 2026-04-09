"""Low-level HTTP client for the Periskope platform API.

Wraps every endpoint documented at
https://docs.periskope.app/api-reference/introduction with thin Python
methods. Every request is authenticated with your API key and the
organization phone number.

Usage::

    from periskope_api import PeriskopeClient

    client = PeriskopeClient(api_key="sk_live_...", phone="919876543210")
    client.update_chat("chat_abc", {"assigned_to": "alice@example.com"})

All methods return the parsed JSON body of the response. Errors raise
``PeriskopeAPIError`` with the HTTP status and the server's message.
"""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import httpx

PERISKOPE_API_BASE = "https://api.periskope.app/v1"


class PeriskopeAPIError(RuntimeError):
    """Raised when the Periskope API returns a non-2xx response."""

    def __init__(self, status_code: int, message: str, payload: Any = None) -> None:
        super().__init__(f"[{status_code}] {message}")
        self.status_code = status_code
        self.message = message
        self.payload = payload


class PeriskopeClient:
    """Synchronous client for the Periskope REST API.

    Parameters
    ----------
    api_key:
        The API key generated in the Periskope console under
        *Settings → Integrations → API*.
    phone:
        Your organization phone number with country code, digits only
        (e.g. ``"919876543210"``).
    base_url:
        Override the API base URL. Defaults to ``https://api.periskope.app/v1``.
    timeout:
        Per-request timeout in seconds.
    """

    def __init__(
        self,
        api_key: str,
        phone: str,
        *,
        base_url: str = PERISKOPE_API_BASE,
        timeout: float = 30.0,
    ) -> None:
        if not api_key:
            raise ValueError("api_key is required")
        if not phone:
            raise ValueError("phone is required")
        self._api_key = api_key
        self._phone = phone
        self._base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
                "x-phone": phone,
            },
        )

    # ------------------------------------------------------------------
    # Context-manager support so ``with PeriskopeClient(...) as c:`` works.
    # ------------------------------------------------------------------
    def __enter__(self) -> "PeriskopeClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # ------------------------------------------------------------------
    # Core request helper
    # ------------------------------------------------------------------
    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, Any] | None = None,
        json: Any = None,
    ) -> Any:
        url = f"{self._base_url}{path}"
        response = self._client.request(method, url, params=params, json=json)
        if response.status_code >= 400:
            try:
                payload = response.json()
                message = payload.get("message") or payload.get("error") or response.text
            except ValueError:
                payload = None
                message = response.text or response.reason_phrase
            raise PeriskopeAPIError(response.status_code, message, payload)
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # ==================================================================
    # Chat management
    # ==================================================================
    def list_chats(self, **params: Any) -> Any:
        return self._request("GET", "/chat", params=params)

    def get_chat(self, chat_id: str) -> Any:
        return self._request("GET", f"/chat/{chat_id}")

    def update_chat(self, chat_id: str, updates: Mapping[str, Any]) -> Any:
        """Patch arbitrary properties on a chat (e.g. ``assigned_to``, ``status``)."""
        return self._request("PATCH", f"/chat/{chat_id}", json=dict(updates))

    def update_chat_labels(self, chat_id: str, labels: Iterable[str]) -> Any:
        return self._request(
            "PATCH", f"/chat/{chat_id}/labels", json={"labels": list(labels)}
        )

    def update_chat_access(self, chat_id: str, access: Mapping[str, Any]) -> Any:
        return self._request("PATCH", f"/chat/{chat_id}/access", json=dict(access))

    def update_chat_settings(self, chat_id: str, settings: Mapping[str, Any]) -> Any:
        return self._request(
            "PATCH", f"/chat/{chat_id}/settings", json=dict(settings)
        )

    def mark_chat_read(self, chat_id: str) -> Any:
        return self._request("PATCH", f"/chat/{chat_id}/mark-read")

    def create_group(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/chat/create-group", json=dict(payload))

    def refresh_chat_invite(self, chat_id: str) -> Any:
        return self._request("POST", f"/chat/{chat_id}/invite/refresh")

    def leave_chat(self, chat_id: str) -> Any:
        return self._request("POST", f"/chat/{chat_id}/leave")

    def accept_chat_invite(self, chat_id: str) -> Any:
        return self._request("POST", f"/chat/{chat_id}/accept-invite")

    def list_chat_messages(self, chat_id: str, **params: Any) -> Any:
        return self._request("GET", f"/chat/{chat_id}/messages", params=params)

    def list_chat_notifications(self, chat_id: str, **params: Any) -> Any:
        return self._request("GET", f"/chat/{chat_id}/notifications", params=params)

    def list_chat_notes(self, chat_id: str, **params: Any) -> Any:
        return self._request("GET", f"/chat/{chat_id}/notes", params=params)

    # ------------------------------------------------------------------
    # Group participants
    # ------------------------------------------------------------------
    def add_participant(self, chat_id: str, user_id: str, **extra: Any) -> Any:
        payload = {"user_id": user_id, **extra}
        return self._request("POST", f"/chat/{chat_id}/participants", json=payload)

    def remove_participant(self, chat_id: str, user_id: str) -> Any:
        return self._request(
            "DELETE", f"/chat/{chat_id}/participants/{user_id}"
        )

    def promote_participant(self, chat_id: str, user_id: str) -> Any:
        return self._request(
            "PATCH", f"/chat/{chat_id}/participants/{user_id}/promote"
        )

    def demote_participant(self, chat_id: str, user_id: str) -> Any:
        return self._request(
            "PATCH", f"/chat/{chat_id}/participants/{user_id}/demote"
        )

    # ==================================================================
    # Messages
    # ==================================================================
    def send_message(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/message/send", json=dict(payload))

    def broadcast_message(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/message/broadcast", json=dict(payload))

    def list_messages(self, **params: Any) -> Any:
        return self._request("GET", "/messages", params=params)

    def get_message(self, message_id: str) -> Any:
        return self._request("GET", f"/message/{message_id}")

    def edit_message(self, message_id: str, updates: Mapping[str, Any]) -> Any:
        return self._request("PATCH", f"/message/{message_id}", json=dict(updates))

    def delete_message(self, message_id: str) -> Any:
        return self._request("DELETE", f"/message/{message_id}")

    def forward_message(self, message_id: str, chat_ids: Iterable[str]) -> Any:
        return self._request(
            "POST",
            f"/message/{message_id}/forward",
            json={"chat_ids": list(chat_ids)},
        )

    def pin_message(self, message_id: str, pinned: bool = True) -> Any:
        return self._request(
            "POST", f"/message/{message_id}/pin", json={"pinned": pinned}
        )

    def react_to_message(self, message_id: str, emoji: str) -> Any:
        return self._request(
            "POST", f"/message/{message_id}/react", json={"emoji": emoji}
        )

    def get_message_status(self, message_id: str) -> Any:
        return self._request("GET", f"/message/{message_id}/status")

    # ==================================================================
    # Message queue
    # ==================================================================
    def list_queue_jobs(self, **params: Any) -> Any:
        return self._request("GET", "/queue/jobs", params=params)

    def purge_queue_jobs(self, **params: Any) -> Any:
        return self._request("DELETE", "/queue/jobs", params=params)

    def queue_status(self) -> Any:
        return self._request("GET", "/queue/status")

    def broadcast_status(self, broadcast_id: str) -> Any:
        return self._request("GET", f"/queue/broadcast/{broadcast_id}/status")

    # ==================================================================
    # Contacts
    # ==================================================================
    def create_contact(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/contact", json=dict(payload))

    def get_contact(self, contact_id: str) -> Any:
        return self._request("GET", f"/contact/{contact_id}")

    def list_contacts(self, **params: Any) -> Any:
        return self._request("GET", "/contacts", params=params)

    def update_contact(self, contact_id: str, updates: Mapping[str, Any]) -> Any:
        return self._request("PATCH", f"/contact/{contact_id}", json=dict(updates))

    def update_contact_labels(self, contact_id: str, labels: Iterable[str]) -> Any:
        return self._request(
            "PATCH", f"/contact/{contact_id}/labels", json={"labels": list(labels)}
        )

    def check_contacts_exist(self, phones: Iterable[str]) -> Any:
        return self._request(
            "POST", "/contact/check-exists", json={"phones": list(phones)}
        )

    # ==================================================================
    # Private notes
    # ==================================================================
    def create_note(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/notes", json=dict(payload))

    def get_note(self, note_id: str) -> Any:
        return self._request("GET", f"/notes/{note_id}")

    def list_notes(self, **params: Any) -> Any:
        return self._request("GET", "/notes", params=params)

    # ==================================================================
    # Tickets
    # ==================================================================
    def create_ticket(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/ticket", json=dict(payload))

    def get_ticket(self, ticket_id: str) -> Any:
        return self._request("GET", f"/ticket/{ticket_id}")

    def list_tickets(self, **params: Any) -> Any:
        return self._request("GET", "/tickets", params=params)

    def update_ticket(self, ticket_id: str, updates: Mapping[str, Any]) -> Any:
        return self._request("PATCH", f"/ticket/{ticket_id}", json=dict(updates))

    # ==================================================================
    # Phones
    # ==================================================================
    def create_phone(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/phone", json=dict(payload))

    def get_phone(self) -> Any:
        return self._request("GET", "/phone")

    def list_phones(self, **params: Any) -> Any:
        return self._request("GET", "/phones", params=params)

    def update_phone(self, updates: Mapping[str, Any]) -> Any:
        return self._request("PATCH", "/phone", json=dict(updates))

    def delete_phone(self) -> Any:
        return self._request("DELETE", "/phone")

    def get_phone_qr(self) -> Any:
        return self._request("GET", "/phone/qr")

    def reset_phone(self) -> Any:
        return self._request("POST", "/phone/reset")

    def restart_phone(self) -> Any:
        return self._request("POST", "/phone/restart")

    def resync_phone(self) -> Any:
        return self._request("POST", "/phone/resync")

    # ==================================================================
    # Members
    # ==================================================================
    def invite_member(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/member/invite", json=dict(payload))

    def update_member_access(self, email: str, access: Mapping[str, Any]) -> Any:
        return self._request("PATCH", f"/member/{email}/access", json=dict(access))

    def delete_member(self, email: str) -> Any:
        return self._request("DELETE", f"/member/{email}")

    # ==================================================================
    # Knowledge base
    # ==================================================================
    def create_kb_document(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/knowledge-base/document", json=dict(payload))

    def create_kb_faq(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/knowledge-base/faq", json=dict(payload))

    def get_kb_document(self, document_id: str) -> Any:
        return self._request("GET", f"/knowledge-base/document/{document_id}")

    def get_kb_faq(self, context_id: str) -> Any:
        return self._request("GET", f"/knowledge-base/faq/{context_id}")

    def list_knowledge_base(self, **params: Any) -> Any:
        return self._request("GET", "/knowledge-base", params=params)

    def update_kb_faq(self, context_id: str, updates: Mapping[str, Any]) -> Any:
        return self._request(
            "PATCH", f"/knowledge-base/faq/{context_id}", json=dict(updates)
        )

    def delete_kb_document(self, document_id: str) -> Any:
        return self._request("DELETE", f"/knowledge-base/document/{document_id}")

    def delete_kb_faq(self, context_id: str) -> Any:
        return self._request("DELETE", f"/knowledge-base/faq/{context_id}")

    # ==================================================================
    # Reactions
    # ==================================================================
    def list_reactions(self, **params: Any) -> Any:
        return self._request("GET", "/reactions", params=params)

    # ==================================================================
    # Webhooks
    # ==================================================================
    def create_webhook(self, payload: Mapping[str, Any]) -> Any:
        return self._request("POST", "/webhook", json=dict(payload))

    def get_webhook(self, webhook_id: str) -> Any:
        return self._request("GET", f"/webhook/{webhook_id}")

    def list_webhooks(self, **params: Any) -> Any:
        return self._request("GET", "/webhooks", params=params)

    def update_webhook(self, webhook_id: str, updates: Mapping[str, Any]) -> Any:
        return self._request("PATCH", f"/webhook/{webhook_id}", json=dict(updates))

    def delete_webhook(self, webhook_id: str) -> Any:
        return self._request("DELETE", f"/webhook/{webhook_id}")
