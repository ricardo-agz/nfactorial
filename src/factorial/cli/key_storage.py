from __future__ import annotations

import json
import os
from pathlib import Path

try:
    import keyring
    from keyring import errors as keyring_errors  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    keyring = None  # type: ignore
    keyring_errors = None  # type: ignore


SERVICE_NAME = "nfactorial"
CONFIG_DIR = Path.home() / ".nfactorial"
CONFIG_FILE = CONFIG_DIR / "keys.json"


def _ensure_secure_permissions() -> None:
    """Set secure permissions on the config directory and file (Unix)."""
    if os.name != "nt":
        CONFIG_DIR.mkdir(mode=0o700, exist_ok=True)
        if CONFIG_FILE.exists():
            os.chmod(CONFIG_FILE, 0o600)


class KeyStorage:
    """API-key storage with optional system keyring integration."""

    def __init__(self) -> None:
        self._use_keyring = keyring is not None

    @staticmethod
    def _load_file() -> dict[str, str | dict[str, str]]:
        if CONFIG_FILE.exists():
            try:
                return json.loads(CONFIG_FILE.read_text())
            except Exception:
                return {}
        return {}

    @staticmethod
    def _save_file(data: dict[str, str | dict[str, str]]) -> None:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        CONFIG_FILE.write_text(json.dumps(data, indent=2))
        _ensure_secure_permissions()

    # ------------------------------------------------------------------ #
    # Keyring helpers
    # ------------------------------------------------------------------ #
    def _kr_set(self, key_name: str, value: str) -> None:
        if keyring is None:
            return
        keyring.set_password(SERVICE_NAME, key_name, value)

    def _kr_get(self, key_name: str) -> str | None:
        if keyring is None:
            return None
        return keyring.get_password(SERVICE_NAME, key_name)

    def _kr_delete(self, key_name: str) -> None:
        if keyring is None:
            return
        try:
            keyring.delete_password(SERVICE_NAME, key_name)
        except Exception:
            pass

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #
    def set_key(self, provider: str, api_key: str) -> None:
        key_name = f"{provider}_api_key"
        if self._use_keyring:
            self._kr_set(key_name, api_key)
            meta: dict[str, str] = {"stored_in": "keyring"}
            data = self._load_file()
            data[key_name] = meta
            self._save_file(data)
        else:
            data = self._load_file()
            data[key_name] = api_key
            self._save_file(data)

    def get_key(self, provider: str) -> str | None:
        key_name = f"{provider}_api_key"
        data = self._load_file()
        if key_name not in data:
            return None
        value = data[key_name]
        if self._use_keyring and isinstance(value, dict):
            return self._kr_get(key_name)
        if isinstance(value, str):
            return value
        return None

    def remove_key(self, provider: str) -> None:
        key_name = f"{provider}_api_key"
        if self._use_keyring:
            self._kr_delete(key_name)
        data = self._load_file()
        if key_name in data:
            del data[key_name]
            self._save_file(data)

    def all_keys(self) -> dict[str, str | None]:
        """Return mapping provider→key (``None`` if not configured)."""
        from .constants import PROVIDERS

        return {p: self.get_key(p) for p in PROVIDERS}


key_storage = KeyStorage()
