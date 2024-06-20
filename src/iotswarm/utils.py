"""Module for handling commonly reused utility functions."""

from datetime import date, datetime
from pathlib import Path
import pandas
import sqlite3
from glob import glob


def json_serial(obj: object):
    """Serializes an unknown object into a json format."""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat(timespec="microseconds")

    if obj.__class__.__module__ != "builtins":
        return obj.__json__()

    raise TypeError(f"Type {type(obj)} is not serializable.")


def build_database_from_csv(
    csv_file: str | Path,
    database: str | Path,
    table_name: str,
    sort_by: str | None = None,
    date_time_format: str = r"%d-%b-%y %H.%M.%S",
) -> None:
    """Adds a database table using a csv file with headers.

    Args:
        csv_file: A path to the csv.
        database: Output destination of the database. File is created if not
        existing.
        table_name: Name of the table to add into database.
        sort_by: Column to sort by
        date_time_format: Format of datetime column
    """

    if not isinstance(csv_file, Path):
        csv_file = Path(csv_file)

    if not isinstance(database, Path):
        database = Path(database)

    if not csv_file.exists():
        raise FileNotFoundError(f'csv_file does not exist: "{csv_file}"')

    if not database.parent.exists():
        raise NotADirectoryError(f'Database directory not found: "{database.parent}"')

    with sqlite3.connect(database) as conn:
        print(
            f'Writing table: "{table_name}" from csv_file: "{csv_file}" to db: "{database}"'
        )
        print("Loading csv")
        df = pandas.read_csv(csv_file)
        print("Done")
        print("Formatting dates")
        df["DATE_TIME"] = pandas.to_datetime(df["DATE_TIME"], format=date_time_format)
        print("Done")
        if sort_by is not None:
            print("Sorting.")
            df = df.sort_values(by=sort_by)
            print("Done")

        print("Writing to db.")
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print("Writing complete.")
