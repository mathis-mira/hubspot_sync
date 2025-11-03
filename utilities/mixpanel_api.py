"""
Mixpanel API Connector class.

Provides a small wrapper around the Mixpanel Export and Property APIs with
shared auth, retry, and logging behaviour.
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Union

import requests
from dotenv import load_dotenv

# --- Logging setup -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Environment setup -------------------------------------------------------
load_dotenv()

USERNAME = os.getenv("MIXPANEL_SERVICE_ACCOUNT", "").strip()
SECRET = os.getenv("MIXPANEL_SERVICE_SECRET", "").strip()
PROJECT_ID = os.getenv("MIXPANEL_PROJECT_ID", "").strip()

EXPORT_URL = "https://data-eu.mixpanel.com/api/2.0/export/"
PROPERTY_URL = "https://eu.mixpanel.com/api/2.0/events/properties/values"


class MixpanelConnector:
    """Class to interact with Mixpanel Export and Property APIs."""

    def __init__(
        self,
        *,
        username: str = USERNAME,
        secret: str = SECRET,
        project_id: str = PROJECT_ID,
        max_retries: int = 3,
        backoff_base: float = 1.5,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not username or not secret or not project_id:
            raise ValueError("username, secret, and project_id are required.")

        self.username = username
        self.secret = secret
        self.project_id = project_id
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        self.session = session or requests.Session()
        self.session.auth = (self.username, self.secret)

    # --- Internal request helper -------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Union[Dict[str, object], Sequence]] = None,
        timeout: int = 60,
        stream: bool = False,
    ) -> requests.Response:
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                resp = self.session.request(
                    method.upper(),
                    url,
                    params=params,
                    timeout=timeout,
                    stream=stream,
                )
                resp.raise_for_status()
                return resp
            except requests.HTTPError as exc:
                if resp.status_code == 429 and attempt < self.max_retries:
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else (self.backoff_base ** attempt)
                    logger.warning("Mixpanel rate limited. Retrying in %.1fs...", delay)
                    time.sleep(delay)
                    continue
                logger.error("Mixpanel request failed: %s", resp.text)
                raise
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    delay = self.backoff_base ** attempt
                    logger.warning("Mixpanel request error: %s. Retrying in %.1fs...", exc, delay)
                    time.sleep(delay)
                    continue
                break

        raise RuntimeError(f"Mixpanel request failed: {last_exc}")

    # --- Public API ---------------------------------------------------------

    def get_property_values(
        self,
        property_name: str,
        *,
        limit: int = 100000,
        timeout: int = 60,
    ) -> List[str]:
        """Retrieve unique values for a Mixpanel event property."""
        params = {
            "project_id": self.project_id,
            "name": property_name,
            "limit": limit,
        }
        resp = self._request("GET", PROPERTY_URL, params=params, timeout=timeout)

        values = resp.json()
        cleaned = [
            str(value)
            for value in values
            if value not in (None, "", "UNKNOWN")
        ]
        logger.info("Retrieved %d values for property %s", len(cleaned), property_name)
        return cleaned

    def export_events(
        self,
        event_names: Iterable[str],
        *,
        start_date: str,
        end_date: str,
        timeout: int = 120,
    ) -> Iterator[Dict]:
        """Stream events for the given names within a date range."""
        event_list = list(event_names)
        if not event_list:
            raise ValueError("event_names must contain at least one event.")

        params = [
            ("from_date", start_date),
            ("to_date", end_date),
            ("event", json.dumps(event_list)),
            ("project_id", self.project_id),
        ]

        resp = self._request("GET", EXPORT_URL, params=params, timeout=timeout, stream=True)

        for line in resp.iter_lines():
            if not line:
                continue
            try:
                yield json.loads(line.decode("utf-8"))
            except json.JSONDecodeError:
                logger.debug("Skipping invalid JSON line from Mixpanel export.")
                continue

    def close(self) -> None:
        """Close the underlying requests session."""
        self.session.close()


__all__ = ["MixpanelConnector"]
