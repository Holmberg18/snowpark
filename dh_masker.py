import configparser
import os

from snowflake.snowpark.session import Session
import snowflake.snowpark as snowpark
from snowflake.snowpark.dataframe_reader import *
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import StringType
from snowflake.snowpark.types import *

##Get Configs
config = configparser.ConfigParser()
ini_path = os.path.join(os.getcwd(), 'config.ini')
config.read(ini_path)
#SNOWFLAKE CONFIG
sfAccount = config['Snowflake']['sfAccount']
sfUser = config['Snowflake']['sfUser']
sfPass = config['Snowflake']['sfPass']
sfDatabase = config['Snowflake']['sfDatabase']
sfSchema = config['Snowflake']['sfSchema']
sfWarehouse = config['Snowflake']['sfWarehouse']
CONNECTION_PARAMETERS = {
    "account": sfAccount,
    "user": sfUser,
    "password": sfPass,
    "database": sfDatabase,
    "schema": sfSchema,
    "warehouse": sfWarehouse,
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()
session.add_packages('regex')
session.add_import('dh_mask.py')

# @udf(imports=["dh_mask.py"], name="my_mask_f2", is_permanent=True, stage_location="@FRU_MODEL_STAGE", replace=True)
def d2_mask(utext: str) -> str:
    import dh_mask
    masked_val = dh_mask.my_mask(utext)
    return masked_val

session.udf.register(
    d2_mask,
    is_permanent=True,
    name='my_mask_f3',
    replace=True,
    stage_location='@FRU_MODEL_STAGE'
)