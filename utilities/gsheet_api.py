"""
Google Sheets API Connector class.

Encapsulates Sheets API calls using a service account.
Provides helper methods for:
    - batch_get_values
    - clear_selected_columns
    - update_single_row
    - batch_update_values
"""

from __future__ import annotations
import logging
from pathlib import Path
from typing import List, Dict, Any

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


# --- Logging setup -----------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Google Sheets scope
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SERVICE_ACCOUNT_FILE = Path(__file__).resolve().parent / "service-account-gsheet.json"


class GSheetConnector:
    """Connector for Google Sheets API using the bundled service account file."""

    def __init__(self):
        try:
            if not SERVICE_ACCOUNT_FILE.exists():
                raise FileNotFoundError(
                    f"Service account file not found at {SERVICE_ACCOUNT_FILE}"
                )

            self.creds = service_account.Credentials.from_service_account_file(
                str(SERVICE_ACCOUNT_FILE), scopes=SCOPES
            )
            self.service = build("sheets", "v4", credentials=self.creds)
            logger.info("Google Sheets API client initialized successfully.")
        except Exception as e:
            logger.critical("Failed to initialize Google Sheets API client: %s", e)
            raise RuntimeError(f"Failed to initialize Google Sheets API client: {e}")

    # --- Internal helper -------------------------------------------------------

    def _values_api(self):
        return self.service.spreadsheets().values()

    # --- Public methods --------------------------------------------------------

    def batch_get_values(self, spreadsheet_id: str, range_names: List[str]) -> Dict[str, Any]:
        """Retrieve values from one or more ranges in a spreadsheet."""
        try:
            result = (
                self._values_api()
                .batchGet(spreadsheetId=spreadsheet_id, ranges=range_names)
                .execute()
            )
            ranges = result.get("valueRanges", [])
            logger.info("Retrieved %d ranges from spreadsheet %s", len(ranges), spreadsheet_id)
            return result
        except HttpError as error:
            logger.error("Failed to batch get values from %s: %s", spreadsheet_id, error)
            return {}

    def clear_selected_columns(
        self, spreadsheet_id: str, sheet_name: str, columns_to_clear: str
    ) -> bool:
        """
        Clear all values in the specified column range of a sheet.
        Example: clear_selected_columns(..., "Sheet1", "A:AG")
        """
        try:
            range_to_clear = f"{sheet_name}!{columns_to_clear}"
            self._values_api().clear(
                spreadsheetId=spreadsheet_id, range=range_to_clear, body={}
            ).execute()
            logger.info("Cleared values in %s!%s", sheet_name, columns_to_clear)
            return True
        except HttpError as error:
            logger.error("Failed to clear columns %s in %s: %s", columns_to_clear, sheet_name, error)
            return False

    def update_single_row(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        row_index: int,
        values: List[Any],
        first_col: str,
        last_col: str,
    ) -> bool:
        """
        Update a single row in the specified sheet.
        Example: update_single_row(..., "Sheet1", 2, ["A", "B"], "A", "AG")
        """
        try:
            range_to_update = f"{sheet_name}!{first_col}{row_index}:{last_col}{row_index}"
            body = {"values": [values]}
            self._values_api().update(
                spreadsheetId=spreadsheet_id,
                range=range_to_update,
                valueInputOption="RAW",
                body=body,
            ).execute()
            logger.info("Updated row %d (%s:%s) in %s", row_index, first_col, last_col, sheet_name)
            return True
        except HttpError as error:
            logger.error("Failed to update row %d in %s: %s", row_index, sheet_name, error)
            return False

    def batch_update_values(
        self,
        spreadsheet_id: str,
        data: List[Dict[str, Any]],
        value_input_option: str = "RAW",
    ) -> bool:
        """
        Batch update multiple ranges in a spreadsheet.

        Args:
            spreadsheet_id: The target spreadsheet ID.
            data: List of dictionaries with "range" and "values" keys, suitable for
                  the Google Sheets batchUpdate endpoint.
            value_input_option: How the input data should be interpreted (default RAW).
        """
        if not data:
            logger.info("No Google Sheet updates to apply.")
            return True

        body = {
            "valueInputOption": value_input_option,
            "data": data,
        }

        try:
            self._values_api().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body,
            ).execute()
            logger.info("Batch updated %d Google Sheet ranges.", len(data))
            return True
        except HttpError as error:
            logger.error("Failed to batch update spreadsheet %s: %s", spreadsheet_id, error)
            return False
