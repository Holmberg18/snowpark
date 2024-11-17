import configparser
import os

from snowflake.snowpark.session import Session
from snowflake.snowpark import functions as F
from snowflake.snowpark.types import *
import pandas as pd
from snowflake.snowpark.functions import udf
import datetime as dt
import numpy as np

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

session.sql('CREATE OR REPLACE STAGE  fru_model_stage').show()

def double(x: int) -> int:
    return 2 * x

double_udf = session.udf.register(double, name="demohubd", replace= True, is_permanent=True, stage_location='@fru_model_stage')

output = session.sql("SELECT demohubd(c_nationkey) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.CUSTOMER limit 10").collect()

print(output)