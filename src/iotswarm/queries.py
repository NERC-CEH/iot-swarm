"""This module contains query constants for retrieving data from the COSMOS database."""

import enum
from enum import StrEnum


@enum.unique
class CosmosTable(StrEnum):
    """Enums of approved COSMOS database tables."""

    LEVEL_1_SOILMET_30MIN = "LEVEL1_SOILMET_30MIN"
    LEVEL_1_NMDB_1HOUR = "LEVEL1_NMDB_1HOUR"
    LEVEL_1_PRECIP_1MIN = "LEVEL1_PRECIP_1MIN"
    LEVEL_1_PRECIP_RAINE_1MIN = "LEVEL1_PRECIP_RAINE_1MIN"
    COSMOS_STATUS = "COSMOS_STATUS_1HOUR"


@enum.unique
class CosmosQuery(StrEnum):
    """Enums of common queries in each databasing language."""

    SQLITE_LOOPED_DATA = """SELECT * FROM {table}
WHERE site_id = :site_id
LIMIT 1 OFFSET :offset"""

    """Query for retreiving data from a given table in sqlite format.
    
    .. code-block:: sql

        SELECT * FROM <table>
        WHERE site_id = :site_id 
        LIMIT 1 OFFSET :offset
    """

    ORACLE_LATEST_DATA = """SELECT * FROM COSMOS.{table}
WHERE site_id = :site_id 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from a given table in oracle format.
    
    .. code-block:: sql

        SELECT * FROM <table>
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    SQLITE_SITE_IDS = "SELECT DISTINCT(site_id) FROM {table}"

    """Queries unique `site_id `s from a given table.
    
    .. code-block:: sql

        SELECT DISTINCT(site_id) FROM <table>
    """

    ORACLE_SITE_IDS = "SELECT UNIQUE(site_id) FROM COSMOS.{table}"

    """Queries unique `site_id `s from a given table.
    
    .. code-block:: sql

        SELECT UNQIUE(site_id) FROM <table>
    """
