"""This module contains query constants for retrieving data from the COSMOS database."""

import enum
from enum import StrEnum


@enum.unique
class CosmosQuery(StrEnum):
    LEVEL_1_SOILMET_30MIN = """SELECT * FROM COSMOS.LEVEL1_SOILMET_30MIN
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the LEVEL1_SOILMET_30MIN table, containing
    calibration the core telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_SOILMET_30MIN
        WHERE site_id = :mysite 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    LEVEL_1_NMDB_1HOUR = """SELECT * FROM COSMOS.LEVEL1_NMDB_1HOUR
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the Level1_NMDB_1HOUR table, containing
    calibration data from the Neutron Monitor DataBase.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_NMDB_1HOUR
        WHERE site_id = :mysite 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    LEVEL_1_PRECIP_1MIN = """SELECT * FROM COSMOS.LEVEL1_PRECIP_1MIN
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the LEVEL1_PRECIP_1MIN table, containing
    the standard precipitation telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_PRECIP_1MIN
        WHERE site_id = :mysite 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """

    LEVEL_1_PRECIP_RAINE_1MIN = """SELECT * FROM COSMOS.LEVEL1_PRECIP_RAINE_1MIN
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    """Query for retreiving data from the LEVEL1_PRECIP_RAINE_1MIN table, containing
    the rain[e] precipitation telemetry from COSMOS sites.
    
    .. code-block:: sql

        SELECT * FROM COSMOS.LEVEL1_PRECIP_RAINE_1MIN
        WHERE site_id = :mysite 
        ORDER BY date_time DESC 
        FETCH NEXT 1 ROWS ONLY
    """
