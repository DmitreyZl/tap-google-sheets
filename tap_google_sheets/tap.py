"""GoogleSheets tap class."""

from __future__ import annotations

import gspread
from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_google_sheets.streams import GoogleSheetsStream


class TapGoogleSheets(Tap):
    """GoogleSheets tap class."""

    name = "tap-google-sheets"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "service_account_path",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="path to service account json file",
        ),
        th.Property(
            "sheet_id",
            th.StringType,
            required=True,
            description="ID of the sheet to sync",
        ),
        th.Property(
            "child_sheet_name",
            th.StringType,
            required=False,
            description="Name of the child sheet to sync.",
        ),
        th.Property(
            "gid",
            th.StringType,
            required=False,
            description="id of the child sheet to sync.",
        ),
        th.Property(
            "output_name",
            th.StringType,
            required=False,
            description="id of the child sheet to sync.",
        ),
        th.Property(
            "column_index",
            th.IntegerType,
            required=False,
            description="index value column",
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
    ).to_dict()

    def discover_streams(self) -> list[GoogleSheetsStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        streams = []
        stream_schema = self.get_schema(self.get_sheet_data())
        if not self.config.get("output_name"):
            name = self.config.get("child_sheet_name")
        else:
            name = self.config.get("output_name")
        streams.append(
            GoogleSheetsStream(
                tap=self,
                name=name,
                schema=stream_schema,
            ),
        )
        return streams

    def get_sheet_data(self) -> list[dict]:
        """Return a generator of row-type dictionary objects."""
        gc = gspread.service_account(
            filename=self.config["service_account_path"],
        )
        sheet = gc.open_by_key(self.config["sheet_id"])
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
        return worksheet.get_all_records(head=self.config["column_index"] if "column_index" in self.config else 1,
                                         expected_headers=expected_headers)

    def get_schema(self, google_sheet_data: list[dict]) -> dict:
        """Build the schema from the data returned by the google sheet."""
        headings, *data = google_sheet_data

        schema = th.PropertiesList()
        for column in headings:
            if column:
                schema.append(th.Property(column.replace(" ", "_"), th.StringType))

        return schema.to_dict()


if __name__ == "__main__":
    TapGoogleSheets.cli()
