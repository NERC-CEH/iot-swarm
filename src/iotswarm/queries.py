"""This module contains query constants for retrieving data from the COSMOS database."""

import enum
from enum import StrEnum


@enum.unique
class CosmosQuery(StrEnum):
    """Class containing permitted SQL queries for retrieving sensor data."""

    LEVEL_1_SOILMET_30MIN = """SELECT * FROM COSMOS.LEVEL1_SOILMET_30MIN
WHERE site_id = :site_id 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the LEVEL1_SOILMET_30MIN table, containing
    calibration the core telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_SOILMET_30MIN
        WHERE site_id = :site_id 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    LEVEL_1_NMDB_1HOUR = """SELECT * FROM COSMOS.LEVEL1_NMDB_1HOUR
WHERE site_id = :site_id 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the Level1_NMDB_1HOUR table, containing
    calibration data from the Neutron Monitor DataBase.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_NMDB_1HOUR
        WHERE site_id = :site_id 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    LEVEL_1_PRECIP_1MIN = """SELECT * FROM COSMOS.LEVEL1_PRECIP_1MIN
WHERE site_id = :site_id 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the LEVEL1_PRECIP_1MIN table, containing
    the standard precipitation telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_PRECIP_1MIN
        WHERE site_id = :site_id 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    LEVEL_1_PRECIP_RAINE_1MIN = """SELECT * FROM COSMOS.LEVEL1_PRECIP_RAINE_1MIN
WHERE site_id = :site_id 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the LEVEL1_PRECIP_RAINE_1MIN table, containing
    the rain[e] precipitation telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_PRECIP_RAINE_1MIN
        WHERE site_id = :site_id 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """


@enum.unique
class CosmosSiteQuery(StrEnum):
    """Contains permitted SQL queries for extracting site IDs from database."""

    LEVEL_1_SOILMET_30MIN = "SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_SOILMET_30MIN"

    """Queries unique site IDs from LEVEL1_SOILMET_30MIN.
    
    .. code-block:: sql

        SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_SOILMET_30MIN
    """

    LEVEL_1_NMDB_1HOUR = "SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_NMDB_1HOUR"

    """Queries unique site IDs from LEVEL1_NMDB_1HOUR table.
    
    .. code-block:: sql

        SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_NMDB_1HOUR
    """

    LEVEL_1_PRECIP_1MIN = "SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_PRECIP_1MIN"

    """Queries unique site IDs from the LEVEL1_PRECIP_1MIN table.
    
    .. code-block:: sql

        SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_PRECIP_1MIN
    """

    LEVEL_1_PRECIP_RAINE_1MIN = (
        "SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_PRECIP_RAINE_1MIN"
    )

    """Queries unique site IDs from the LEVEL1_PRECIP_RAINE_1MIN table.
    
    .. code-block:: sql

        SELECT UNIQUE(site_id) FROM COSMOS.LEVEL1_PRECIP_RAINE_1MIN
    """


@enum.unique
class CosmosSqliteQuery(StrEnum):
    """Class containing permitted SQL queries for retrieving sensor data from
    sqlite database."""

    LEVEL_1_SOILMET_30MIN = """SELECT * FROM LEVEL_1_SOILMET_30MIN
WHERE site_id = :site_id 
LIMIT 1 OFFSET :offset"""

    """Query for retreiving data from the LEVEL_1_SOILMET_30MIN table, containing
    calibration the core telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM LEVEL_1_SOILMET_30MIN
        WHERE site_id = :site_id 
        LIMIT 1 OFFSET :offset
    """

    LEVEL_1_NMDB_1HOUR = """SELECT * FROM LEVEL_1_NMDB_1HOUR
WHERE site_id = :site_id 
LIMIT 1 OFFSET :offset"""

    """Query for retreiving data from the LEVEL_1_NMDB_1HOUR table, containing
    calibration data from the Neutron Monitor DataBase.
    
    .. code-block:: sql

        SELECT * FROM LEVEL_1_NMDB_1HOUR
        WHERE site_id = :site_id 
        LIMIT 1 OFFSET :offset
    """

    LEVEL_1_PRECIP_1MIN = """SELECT * FROM LEVEL_1_PRECIP_1MIN
WHERE site_id = :site_id 
LIMIT 1 OFFSET :offset"""

    """Query for retreiving data from the LEVEL_1_PRECIP_1MIN table, containing
    the standard precipitation telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM LEVEL_1_PRECIP_1MIN
        WHERE site_id = :site_id 
        LIMIT 1 OFFSET :offset
    """

    LEVEL_1_PRECIP_RAINE_1MIN = """SELECT * FROM LEVEL_1_PRECIP_RAINE_1MIN
WHERE site_id = :site_id 
LIMIT 1 OFFSET :offset"""

    """Query for retreiving data from the LEVEL_1_PRECIP_RAINE_1MIN table, containing
    the rain[e] precipitation telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM LEVEL_1_PRECIP_RAINE_1MIN
        WHERE site_id = :site_id
        LIMIT 1 OFFSET :offset
    """


@enum.unique
class CosmosSiteSqliteQuery(StrEnum):
    """Contains permitted SQL queries for extracting site IDs from database."""

    LEVEL_1_SOILMET_30MIN = "SELECT DISTINCT(site_id) FROM LEVEL_1_SOILMET_30MIN"

    """Queries unique site IDs from LEVEL_1_SOILMET_30MIN.
    
    .. code-block:: sql

        SELECT DISTINCT(site_id) FROM LEVEL_1_SOILMET_30MIN
    """

    LEVEL_1_NMDB_1HOUR = "SELECT DISTINCT(site_id) FROM LEVEL_1_NMDB_1HOUR"

    """Queries unique site IDs from LEVEL_1_NMDB_1HOUR table.
    
    .. code-block:: sql

        SELECT DISTINCT(site_id) FROM LEVEL_1_NMDB_1HOUR
    """

    LEVEL_1_PRECIP_1MIN = "SELECT DISTINCT(site_id) FROM LEVEL_1_PRECIP_1MIN"

    """Queries unique site IDs from the LEVEL_1_PRECIP_1MIN table.
    
    .. code-block:: sql

        SELECT DISTINCT(site_id) FROM LEVEL_1_PRECIP_1MIN
    """

    LEVEL_1_PRECIP_RAINE_1MIN = (
        "SELECT DISTINCT(site_id) FROM LEVEL_1_PRECIP_RAINE_1MIN"
    )

    """Queries unique site IDs from the LEVEL_1_PRECIP_RAINE_1MIN table.
    
    .. code-block:: sql

        SELECT DISTINCT(site_id) FROM LEVEL_1_PRECIP_RAINE_1MIN
    """
