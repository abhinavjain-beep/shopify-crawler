"""Periskope API editor package.

Provides a synchronous Python client (``PeriskopeClient``) and a high-level
``PeriskopeEditor`` that exposes every editable resource on the Periskope
platform (chats, messages, contacts, tickets, phones, members, webhooks,
knowledge base entries, private notes, and group participants).

API reference: https://docs.periskope.app/api-reference/introduction
"""

from .client import PeriskopeClient, PeriskopeAPIError
from .editor import PeriskopeEditor

__all__ = ["PeriskopeClient", "PeriskopeEditor", "PeriskopeAPIError"]
