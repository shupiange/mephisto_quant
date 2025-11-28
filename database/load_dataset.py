from database.db_manager import MySQLManager
from mysql.connector import Error
import pandas as pd
import os
import shutil
from config.database_config import DATABASE_CONFIG
from config.work_config import WORK_DIR
import argparse


def load_dataset(code: int, start_date: str, end_date: str, table_name: str, database_name: str) -> pd.DataFrame:
    return