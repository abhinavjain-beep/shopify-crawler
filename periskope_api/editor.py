"""High-level editor facade over :class:`PeriskopeClient`.

``PeriskopeEditor`` exposes every *editable* resource on Periskope behind a
single ``edit(resource, resource_id, updates)`` method, plus convenience
``create`` / ``delete`` / ``list`` / ``get`` helpers. This lets you mutate
any object on the platform with a uniform interface, which is handy when
you don't want to remember each endpoint's specific method name.

Example::

    from periskope_api import PeriskopeEditor

    editor = PeriskopeEditor(api_key="sk_live_...", phone="919876543210")

    # Rename a chat and reassign it.
    editor.edit("chat", "chat_abc", {
        "name": "VIP Customer",
        "assigned_to": "alice@example.com",
    })

    # Update a ticket's status.
    editor.edit("ticket", "tkt_123", {"status": "resolved"})

    # Change contact labels.
    editor.edit("contact_labels", "ctc_999", {"labels": ["vip", "paid"]})
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from .client import PeriskopeClient


# Each entry maps a resource name to the ``(edit, get, delete)`` callables
# on ``PeriskopeClient``. ``None`` means the resource doesn't support that
# operation.
_ResourceOps = tuple[
    Callable[..., Any] | None,  # edit/update
    Callable[..., Any] | None,  # get/fetch one
    Callable[..., Any] | None,  # delete
]


class PeriskopeEditor:
    """Uniform edit/get/delete interface for every Periskope resource."""

    def __init__(
        self,
        api_key: str | None = None,
        phone: str | None = None,
        *,
        client: PeriskopeClient | None = None,
    ) -> None:
        if client is None:
            if api_key is None or phone is None:
                raise ValueError(
                    "Either pass a PeriskopeClient or both api_key and phone."
                )
            client = PeriskopeClient(api_key=api_key, phone=phone)
        self.client = client
        self._ops = self._build_ops()

    # ------------------------------------------------------------------
    # Context manager support.
    # ------------------------------------------------------------------
    def __enter__(self) -> "PeriskopeEditor":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.client.close()

    def close(self) -> None:
        self.client.close()

    # ------------------------------------------------------------------
    # Resource dispatch table
    # ------------------------------------------------------------------
    def _build_ops(self) -> dict[str, _ResourceOps]:
        c = self.client
        return {
            # Chats
            "chat": (c.update_chat, c.get_chat, None),
            "chat_labels": (
                lambda cid, updates: c.update_chat_labels(cid, updates["labels"]),
                None,
                None,
            ),
            "chat_access": (c.update_chat_access, None, None),
            "chat_settings": (c.update_chat_settings, None, None),
            # Messages
            "message": (c.edit_message, c.get_message, c.delete_message),
            # Contacts
            "contact": (c.update_contact, c.get_contact, None),
            "contact_labels": (
                lambda cid, updates: c.update_contact_labels(cid, updates["labels"]),
                None,
                None,
            ),
            # Tickets
            "ticket": (c.update_ticket, c.get_ticket, None),
            # Phone (singleton — resource_id is ignored)
            "phone": (
                lambda _id, updates: c.update_phone(updates),
                lambda _id: c.get_phone(),
                lambda _id: c.delete_phone(),
            ),
            # Members (identified by email)
            "member_access": (
                lambda email, updates: c.update_member_access(email, updates),
                None,
                c.delete_member,
            ),
            # Knowledge base
            "kb_faq": (c.update_kb_faq, c.get_kb_faq, c.delete_kb_faq),
            "kb_document": (None, c.get_kb_document, c.delete_kb_document),
            # Webhooks
            "webhook": (c.update_webhook, c.get_webhook, c.delete_webhook),
        }

    # ------------------------------------------------------------------
    # Uniform mutation API
    # ------------------------------------------------------------------
    def resources(self) -> list[str]:
        """Return the list of resource names that can be edited."""
        return sorted(self._ops.keys())

    def edit(
        self, resource: str, resource_id: str, updates: Mapping[str, Any]
    ) -> Any:
        """Patch ``updates`` onto ``resource`` identified by ``resource_id``.

        Raises ``ValueError`` if the resource isn't editable.
        """
        edit_fn, _, _ = self._get_ops(resource)
        if edit_fn is None:
            raise ValueError(f"Resource '{resource}' does not support editing.")
        return edit_fn(resource_id, updates)

    def get(self, resource: str, resource_id: str) -> Any:
        _, get_fn, _ = self._get_ops(resource)
        if get_fn is None:
            raise ValueError(f"Resource '{resource}' does not support fetching.")
        return get_fn(resource_id)

    def delete(self, resource: str, resource_id: str) -> Any:
        _, _, delete_fn = self._get_ops(resource)
        if delete_fn is None:
            raise ValueError(f"Resource '{resource}' does not support deletion.")
        return delete_fn(resource_id)

    def _get_ops(self, resource: str) -> _ResourceOps:
        try:
            return self._ops[resource]
        except KeyError as exc:
            available = ", ".join(self.resources())
            raise ValueError(
                f"Unknown resource '{resource}'. Available: {available}"
            ) from exc
