
"""
HubSpot API Connector class.

Encapsulates HubSpot API calls in a single class, with one method per endpoint.
Handles authentication, retries, and consistent error messaging.
"""
from __future__ import annotations

import os
import time
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Union
import requests
from dotenv import load_dotenv

# --- Logging setup -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)  

# --- Environment setup ---------------------------------
load_dotenv()

ACCESS_TOKEN = os.getenv("HUBSPOT_ACCESS_TOKEN", "").strip()
BASE_URL = "https://api.hubapi.com"
DEFAULT_HEADERS = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
}
DEFAULT_TIMEOUT = 30
DEFAULT_SEARCH_LIMIT = 100
DEFAULT_DEAL_PROPERTIES = [
    "dealname",
    "company_name",
    "amount",
    "icp_sync",
    "date_entered_upcoming_churn_sync",
    "cs_active_sync",
    "id",
    "dealtype",
    "contract_start_date",
    "contract_end_date",
    "contract_length",
    "hs_is_closed_won",
    "hs_is_closed",
    "closedate",
    "dealstage",
    "pipeline",
    "lifecycle_stage",
    "admin___ready_for_deletions___2506",
    "company_id",
    "hs_v2_date_entered_28032678",
]
DEFAULT_LINE_ITEM_PROPERTIES = [
    "name",
    "quantity",
    "discount",
    "recurringbillingfrequency",
    "hs_recurring_billing_period",
    "hs_recurring_billing_terms",
    "hs_billing_start_delay_type",
    "hs_recurring_billing_start_date",
    "hs_post_tax_amount",
    "hs_object_id",
]


class HubSpotConnector:
    """Class to interact with HubSpot CRM API."""

    def __init__(self, max_retries: int = 3, backoff_base: float = 1.5):
        self.max_retries = max_retries
        self.backoff_base = backoff_base

    # --- Contact methods ----------------------------------------------------

    def upsert_hubspot_contact(
        self,
        email: str,
        properties: Optional[Dict[str, Any]] = None,
        timeout: int = 15,
    ) -> Dict[str, Any]:
        """Create or update a contact identified by email."""
        if not email or "@" not in email:
            raise ValueError(f"Invalid or missing email for HubSpot upsert: {email}")

        if properties is None:
            properties = {}
        elif not isinstance(properties, dict):
            raise TypeError("properties must be a dict mapping HubSpot internal names to values.")

        payload = {
            "inputs": [
                {
                    "id": email.lower().strip(),
                    "idProperty": "email",
                    "properties": properties,
                }
            ]
        }

        resp = self._request(
            "POST",
            "/crm/v3/objects/contacts/batch/upsert",
            json_body=payload,
            timeout=timeout,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            logger.error("HubSpot contact upsert failed: %s", resp.text)
            raise

        return resp.json()

    # --- Generic object helpers -------------------------------------------

    def update_object(
        self,
        object_type: str,
        object_id: Union[str, int],
        properties: Dict[str, Any],
        timeout: int = 15,
    ) -> Optional[Dict[str, Any]]:
        """Update an object's properties."""
        if not properties:
            raise ValueError("properties must contain at least one field to update.")

        path = f"/crm/v3/objects/{object_type}/{object_id}"
        payload = {"properties": properties}
        resp = self._request("PATCH", path, json_body=payload, timeout=timeout)

        if resp.status_code == 200:
            logger.info(
                "%s %s updated with properties: %s",
                object_type,
                object_id,
                list(properties.keys()),
            )
            return resp.json()

        logger.error(
            "Failed to update %s %s. Status: %s, Response: %s",
            object_type,
            object_id,
            resp.status_code,
            resp.text,
        )
        return None

    def get_object(
        self,
        object_type: str,
        object_id: Union[str, int],
        *,
        properties: Optional[Iterable[str]] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve a single object and optionally limit the returned properties."""
        params: Optional[Dict[str, Any]] = None
        if properties:
            params = {"properties": list(properties)}

        resp = self._request(
            "GET",
            f"/crm/v3/objects/{object_type}/{object_id}",
            params=params,
            timeout=timeout,
        )
        if resp.status_code == 200:
            logger.info("Retrieved %s %s", object_type, object_id)
            return resp.json()

        logger.error(
            "Failed to retrieve %s %s. Status: %s, Response: %s",
            object_type,
            object_id,
            resp.status_code,
            resp.text,
        )
        return None

    def search_objects(
        self,
        object_type: str,
        filter_groups: Sequence[Dict[str, Any]],
        *,
        properties: Optional[Iterable[str]] = None,
        limit: int = DEFAULT_SEARCH_LIMIT,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> Dict[str, Any]:
        """Run a paginated search against a HubSpot object type."""
        if not filter_groups:
            raise ValueError("At least one filter group is required.")

        all_results: List[Dict[str, Any]] = []
        after: Optional[str] = None

        while True:
            payload: Dict[str, Any] = {
                "filterGroups": list(filter_groups),
                "limit": limit,
            }
            if properties:
                payload["properties"] = list(properties)
            if after:
                payload["after"] = after

            resp = self._request(
                "POST",
                f"/crm/v3/objects/{object_type}/search",
                json_body=payload,
                timeout=timeout,
            )
            if resp.status_code != 200:
                logger.error(
                    "Failed to search %s. Status: %s, Response: %s",
                    object_type,
                    resp.status_code,
                    resp.text,
                )
                break

            data = resp.json()
            results = data.get("results", [])
            all_results.extend(results)
            logger.info(
                "Retrieved %d %s (total so far: %d)",
                len(results),
                object_type,
                len(all_results),
            )

            after = data.get("paging", {}).get("next", {}).get("after")
            if not after:
                break

        return {"results": all_results}

    # --- Internal Request Handler ------------------------------------------

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> requests.Response:
        url = f"{BASE_URL}{path}"
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                resp = requests.request(
                    method.upper(),
                    url,
                    headers=DEFAULT_HEADERS,
                    params=params,
                    json=json_body,
                    timeout=timeout,
                )

                if resp.status_code == 429 and attempt < self.max_retries:
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else (self.backoff_base ** attempt)
                    logger.warning("Rate limited (429). Retrying in %.1fs...", delay)
                    time.sleep(delay)
                    continue

                return resp
            except requests.RequestException as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    delay = self.backoff_base ** attempt
                    logger.warning("Request error: %s. Retrying in %.1fs...", exc, delay)
                    time.sleep(delay)
                    continue
                break

        raise RuntimeError(f"HubSpot request failed: {last_exc}")

    # --- Company methods ------------------------------------------------------
    def search_company(
            self,
            filters: Dict[str, Any],
            properties: Optional[List[str]] = None,
            limit: int = 100,
            operator: str = "EQ",
        ) -> Dict[str, Any]:
            if not filters:
                raise ValueError("At least one search filter must be provided.")

            filter_list: List[Dict[str, Any]] = [
                {"propertyName": prop, "operator": operator, "value": str(value)}
                for prop, value in filters.items()
            ]

            return self.search_objects(
                "companies",
                [{"filters": filter_list}],
                properties=properties,
                limit=limit,
            )

    def update_company_properties(
        self,
        company_id: Union[str, int],
        properties: Dict[str, Any],
        timeout: int = 15,
    ) -> Optional[Dict[str, Any]]:
        return self.update_object("companies", company_id, properties, timeout=timeout)

    # --- Deal methods ---------------------------------------------------------

    def search_deals_stage_id(self, stage_ids: Iterable[str]) -> Dict[str, Any]:
        filter_groups = [
            {
                "filters": [
                    {
                        "propertyName": "dealstage",
                        "operator": "IN",
                        "values": list(stage_ids),
                    }
                ]
            }
        ]
        return self.search_objects(
            "deals",
            filter_groups,
            properties=DEFAULT_DEAL_PROPERTIES,
            limit=DEFAULT_SEARCH_LIMIT,
        )

    def get_associations(
        self,
        from_object_type: str,
        object_id: Union[str, int],
        to_object_type: str,
        timeout: int = 15,
    ) -> Optional[Dict[str, Any]]:
        """
        Retrieve associations between two HubSpot CRM object types.

        Args:
            from_object_type: Source object type (e.g., "deals", "line_items").
            object_id: Identifier of the source object.
            to_object_type: Target object type to fetch associations for.
            timeout: Optional request timeout in seconds.
        """
        path = f"/crm/v3/objects/{from_object_type}/{object_id}/associations/{to_object_type}"
        resp = self._request("GET", path, timeout=timeout)
        if resp.status_code == 200:
            logger.info(
                "Associations retrieved: %s %s -> %s",
                from_object_type,
                object_id,
                to_object_type,
            )
            return resp.json()

        logger.error(
            "Failed to retrieve associations %s %s -> %s. Status: %s, Response: %s",
            from_object_type,
            object_id,
            to_object_type,
            resp.status_code,
            resp.text,
        )
        return None

    # --- Line item methods ----------------------------------------------------

    def search_line_items(
        self,
        line_item_ids: Union[str, Iterable[str]],
        properties: Optional[Iterable[str]] = None,
    ) -> Dict[str, Any]:
        ids = [line_item_ids] if isinstance(line_item_ids, str) else list(line_item_ids)

        filter_groups = [
            {
                "filters": [
                    {
                        "propertyName": "hs_object_id",
                        "operator": "IN",
                        "values": ids,
                    }
                ]
            }
        ]
        return self.search_objects(
            "line_items",
            filter_groups,
            properties=properties,
            limit=DEFAULT_SEARCH_LIMIT,
        )

    def get_line_item_by_id(
        self,
        line_item_id: Union[str, int],
        *,
        properties: Optional[Iterable[str]] = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> Optional[Dict[str, Any]]:
        if properties is None:
            properties = DEFAULT_LINE_ITEM_PROPERTIES
        return self.get_object(
            "line_items",
            line_item_id,
            properties=properties,
            timeout=timeout,
        )


  
