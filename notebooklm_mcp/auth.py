"""
Google OAuth2 authentication for NotebookLM MCP Server.

Run this module directly to authenticate:
    python -m notebooklm_mcp.auth
"""

import json
import sys
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# NotebookLM requires these Google OAuth scopes
SCOPES = [
    "https://www.googleapis.com/auth/notebooklm",
    "https://www.googleapis.com/auth/drive.readonly",
]

CREDENTIALS_DIR = Path.home() / ".config" / "notebooklm-mcp"
TOKEN_FILE = CREDENTIALS_DIR / "token.json"
CLIENT_SECRETS_FILE = CREDENTIALS_DIR / "client_secrets.json"


class NotebookLMAuth:
    """Handles Google OAuth2 authentication for NotebookLM access."""

    def load_credentials(self) -> Credentials | None:
        """
        Load stored credentials, refreshing them if expired.

        Returns:
            Valid Credentials object, or None if no credentials exist.
        """
        if not TOKEN_FILE.exists():
            return None

        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            self._save_credentials(creds)

        return creds if creds and creds.valid else None

    def authenticate(self) -> Credentials:
        """
        Run the OAuth2 flow to obtain new credentials.

        Requires client_secrets.json to be present in the config directory.
        Download it from Google Cloud Console → APIs & Services → Credentials.

        Returns:
            Valid Credentials object.

        Raises:
            FileNotFoundError: If client_secrets.json is not found.
        """
        if not CLIENT_SECRETS_FILE.exists():
            raise FileNotFoundError(
                f"client_secrets.json not found at {CLIENT_SECRETS_FILE}\n\n"
                "To set up authentication:\n"
                "1. Go to https://console.cloud.google.com/\n"
                "2. Create a project and enable the NotebookLM API\n"
                "3. Create OAuth 2.0 credentials (Desktop application)\n"
                f"4. Download and save as {CLIENT_SECRETS_FILE}"
            )

        flow = InstalledAppFlow.from_client_secrets_file(
            str(CLIENT_SECRETS_FILE), SCOPES
        )
        creds = flow.run_local_server(port=0)
        self._save_credentials(creds)
        print(f"Authentication successful. Token saved to {TOKEN_FILE}")
        return creds

    def _save_credentials(self, creds: Credentials) -> None:
        """Persist credentials to disk."""
        CREDENTIALS_DIR.mkdir(parents=True, exist_ok=True)
        TOKEN_FILE.write_text(creds.to_json())


def main():
    """Interactive authentication flow."""
    auth = NotebookLMAuth()

    existing = auth.load_credentials()
    if existing:
        print("Already authenticated with valid credentials.")
        print(f"Token file: {TOKEN_FILE}")
        answer = input("Re-authenticate? [y/N]: ").strip().lower()
        if answer != "y":
            print("Using existing credentials.")
            return

    print("Starting Google OAuth2 authentication...")
    print(f"Config directory: {CREDENTIALS_DIR}")
    print()

    try:
        auth.authenticate()
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
