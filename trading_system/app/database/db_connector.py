import os
from dotenv import load_dotenv
from tortoise import Tortoise


load_dotenv()


TRADING_SYSTEM_RDS_DB_URL = os.getenv('PORTFOLIO_MGR_RDS_DB_URL')

async def init_db():
    """
    Initialize the database connection and generate schemas using Tortoise ORM.

    Loads the database URL from environment variables and connects to the specified
    database. Sets up tables and relationships based on the defined models if they
    do not already exist.

    Note:
        Ensure the DATABASE_URL environment variable is properly configured in the
        .env file, and the path to the models module is accurate.
    """
    await Tortoise.init(
        db_url=TRADING_SYSTEM_RDS_DB_URL,
        modules={'models': ['models']}
    )
    await Tortoise.generate_schemas()