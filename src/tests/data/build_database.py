from iotswarm.utils import build_database_from_csv
from pathlib import Path


def main():
    data_dir = Path(__file__).parent
    data_file = Path(data_dir, "ALIC1_4_ROWS.csv")
    database_file = Path(data_dir, "database.db")

    data_table = "LEVEL_1_SOILMET_30MIN"

    build_database_from_csv(data_file, database_file, data_table)


if __name__ == "__main__":
    main()
