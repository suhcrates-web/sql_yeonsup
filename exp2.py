import mysql.connector
from database import cursor, db
from database_remote import cursor_r
import glob, os
from ToolBox import table_empty, table_checker, all_tables
from datetime import date

# cursor_r.execute(
#     f"""
#     USE corps;
#     """
# )
def check_colname(db0, table0, com):
    cursor_r.execute(
        f"""
        DESCRIBE corps.060310;
        """
    )
    result = cursor_r.fetchall()
    col_list = list(map(lambda x:x[0], result))
    print(col_list)

