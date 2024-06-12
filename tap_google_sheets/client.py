"""REST client handling, including GoogleSheetsStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterable

import gspread
import requests
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GoogleSheetsBaseStream(RESTStream):
    """GoogleSheets stream class."""

    url_base = ""

    @property
    def gc(self) -> gspread.Client:
        """Return a gspread client."""
        if not hasattr(self, "_gc"):
            self._gc = gspread.service_account(
                filename=self.config["service_account_path"],
            )
        return self._gc

    def get_records(self, context) -> Iterable[dict]:  # noqa: ANN001, ARG002
        """Return a generator of row-type dictionary objects.

        Args:
            context: The stream context.

        Yields:
            A row-type dictionary object.
        """
        sheet = self.gc.open_by_key(self.config["sheet_id"])
        if "child_sheet_name" in self.config:
            worksheet = sheet.worksheet(self.config["child_sheet_name"])
        elif "gid" in self.config:
            worksheet_list = sheet.worksheets()
            target_gid = self.config["gid"]
            worksheet = None
            for worksheet_ in worksheet_list:
                if worksheet_.id == int(float(target_gid)):
                    worksheet = worksheet_
                    break
        else:
            worksheet = sheet.sheet1
        expected_headers = worksheet.row_values(self.config["column_index"] if "column_index" in self.config else 1)
        return worksheet.get_all_records(expected_headers=expected_headers)
