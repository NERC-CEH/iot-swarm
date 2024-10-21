from pathlib import Path
from typing import List, Optional
import pandas as pd
import sqlite3
from glob import glob
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm 

def build_database_from_csv(
    csv_file: str | Path,
    database: str | Path,
    table_name: str,
    timestamp_header: str,
    sort_by: str | None = None,
    date_time_format: str = r"%d-%b-%y %H.%M.%S",
) -> None:
    """Adds a database table using a csv file with headers.

    Args:
        csv_file: A path to the csv.
        database: Output destination of the database. File is created if not
        existing.
        table_name: Name of the table to add into database.
        timestamp_header: Name of the column with a timestamp
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
        df = pd.read_csv(csv_file)
        print("Done")
        print("Formatting dates")
        # print(df.loc[782794])
        df[timestamp_header] = pd.to_datetime(df[timestamp_header], format=date_time_format)
        print("Done")
        if sort_by is not None:
            print("Sorting.")
            df = df.sort_values(by=sort_by)
            print("Done")

        print("Writing to db.")
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print("Writing complete.")


def _read_csv_file(file_path):
    return pd.read_csv(file_path, delimiter=",", skiprows=[0,2,3])

def _write_batch_to_file(batch_df, dst, mode='a', header=False):
    batch_df.to_csv(dst, mode=mode, index=False, header=header)

def process_csv_files_parallel(src, dst, batch_size=1000, sort_columns: Optional[List[str]|str]=None, extension:str=".dat"):
    """Converts a directory of .dat files into a combined .csv file
    Args:
        src: The source directory
        dst: The output file
        sort_columns: Column to sort the values by
        extension: The file extension to match
    """

    if not isinstance(src, Path):
        src = Path(src)
    if not isinstance(dst, Path):
        dst = Path(dst)
    if not isinstance(sort_columns, list) and sort_columns is not None:
        sort_columns = [sort_columns]

    # Get the list of all CSV files
    files = [Path(x) for x in glob(f"{src}/**/*{extension}", recursive=True)]
    # Create the output file and write the header from the first file
    header_written = False
    total_files = len(files)
    
    # Use a ProcessPoolExecutor to parallelize the loading of files
    with ProcessPoolExecutor() as executor, tqdm(total=total_files, desc="Processing files") as progress_bar:
        # Process in batches
        for i in range(0, total_files, batch_size):
            # Select a batch of files
            batch_files = files[i:i + batch_size]
            
            # Read the files in parallel
            batch_dfs = list(executor.map(_read_csv_file, batch_files))
            
            # Concatenate the batch into one DataFrame
            combined_batch_df = pd.concat(batch_dfs, ignore_index=True)
            
            # Write the batch to the output file (only write header once)
            _write_batch_to_file(combined_batch_df, dst, mode='a', header=not header_written)
            header_written = True  # Header written after the first batch
            
            # Update the progress bar
            progress_bar.update(len(batch_files))
            
            # Optionally clear memory if batches are large
            del combined_batch_df, batch_dfs

    if sort_columns is not None:
        print(f"Sorting by {sort_columns}")
        df = pd.read_csv(dst)
        df = df.sort_values(by=sort_columns)
        df.to_csv(dst, index=False, header=True, mode="w")