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
