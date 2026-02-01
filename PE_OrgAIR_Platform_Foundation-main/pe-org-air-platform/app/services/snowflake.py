import snowflake.connector
from app.config import settings


def get_snowflake_connection():
    """
    Returns a Snowflake connection.
    Used by services or repositories when database access is required.
    """
    return snowflake.connector.connect(
    user=settings.SNOWFLAKE_USER,
    password=settings.SNOWFLAKE_PASSWORD.get_secret_value(),
    account=settings.SNOWFLAKE_ACCOUNT,
    warehouse=settings.SNOWFLAKE_WAREHOUSE,
    database=settings.SNOWFLAKE_DATABASE,
    schema=settings.SNOWFLAKE_SCHEMA,
    role=settings.SNOWFLAKE_ROLE,

    )
