import mysql.connector
from database import cursor, db
import glob, os
from ToolBox import *

# db_name = 'exp1'
# cursor.execute(
#     f"""
#     CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8';
#     """
# )
#
# cursor.execute(
#     f"""
#     CREATE TABLE IF NOT EXISTS {db_name}.table1(
#     date0 DATE,
#     회사코드 VARCHAR(6),
#     회사이름 VARCHAR(20),
#     종가 INT,
#     대비 INT,
#     등락률 DECIMAL(5,2),
#     상장주식수 BIGINT,
#     외국인보유수량 BIGINT,
#     외국인지분율 DECIMAL(5,2),
#     외국인한도수량 BIGINT,
#     외국인한도소진율 DECIMAL(5,2)
#     );
#     """
# )
#
# cursor.execute(
#     f"""
#         INSERT INTO {db_name}.table1 VALUES(0);
#         """
#     )

cursor.execute(
    """
    SELECT MIN(date0) FROM krx_stuffs.외국인보유;
    """
)
print(cursor.fetchall()[0][0])
#[(None,)]