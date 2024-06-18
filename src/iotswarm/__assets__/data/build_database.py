from iotswarm.utils import build_database_from_csv
from pathlib import Path
from glob import glob


def main():
    data_dir = Path(__file__).parent
    data_file = Path(data_dir, "LEVEL_1_SOILMET_30MIN_DATA_TABLE.csv")
    csv_files = glob("*.csv", root_dir=data_dir)

    tables = [x.removesuffix("_DATA_TABLE.csv") for x in csv_files]

    database_file = Path(data_dir, "cosmos.db")

    for table, file in zip(tables, csv_files):
        file = Path(data_dir, file)
        build_database_from_csv(data_file, database_file, table, sort_by="DATE_TIME")


if __name__ == "__main__":
    main()
