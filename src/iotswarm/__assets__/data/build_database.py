"""This script is responsible for building an SQL data file from the CSV files used
by the cosmos network.

The files are stored in AWS S3 and should be downloaded into this directory before continuing.
They are:
    * LEVEL_1_NMDB_1HOUR_DATA_TABLE.csv
    * LEVEL_1_SOILMET_30MIN_DATA_TABLE.csv
    * LEVEL_1_PRECIP_1MIN_DATA_TABLE.csv
    * LEVEL_1_PRECIP_RAINE_1MIN_DATA_TABLE.csv

Once installed, run this script to generate the .db file.
"""

from iotswarm.utils import build_database_from_csv
from pathlib import Path
from glob import glob
from iotswarm.queries import CosmosTable


def main():
    data_dir = Path(__file__).parent
    csv_files = glob("*.csv", root_dir=data_dir)

    tables = [CosmosTable[x.removesuffix("_DATA_TABLE.csv")] for x in csv_files]

    database_file = Path(data_dir, "cosmos.db")

    for table, file in zip(tables, csv_files):
        file = Path(data_dir, file)
        build_database_from_csv(file, database_file, table.value, sort_by="DATE_TIME")


if __name__ == "__main__":
    main()
