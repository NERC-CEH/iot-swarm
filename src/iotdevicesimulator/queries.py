import enum
from enum import StrEnum


@enum.unique
class CosmosQuery(StrEnum):
    LEVEL_1_SOILMET_30MIN = """SELECT * FROM COSMOS.LEVEL1_SOILMET_30MIN
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    LEVEL_1_NMDB_1HOUR = """SELECT * FROM COSMOS.LEVEL1_NMDB_1HOUR
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""

    LEVEL_1_PRECIP_1MIN = """SELECT * FROM COSMOS.LEVEL1_PRECIP_1MIN
WHERE site_id = :mysite 
ORDER BY date_time DESC 
FETCH NEXT 1 ROWS ONLY"""
