import mysql.connector
import csv
from database import cursor, db
import glob, os
from ToolBox import table_empty, table_checker, all_tables

#### source / 전체종목

db_name = 'source'
table_name = '전체종목'
# cursor.execute(
#     f"""
#     DROP DATABASE {db_name};
#     """
# )
# cursor.execute(
#     f"""
#     CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8';
#     """
# )
cursor.execute(
    f"""
    DROP TABLE {db_name}.{table_name};
    """
)
cursor.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name}(
    corp_cd_long varchar(14) NOT NULL ,
    corp_cd varchar(6) NOT NULL PRIMARY KEY, 
    corp_nm varchar(20) NOT NULL,
    상장일 DATE NOT NULL,
    시장구분 varchar(7),
    액면가 varchar(10),
    상장주식수 BIGINT
    );
    """
)


with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/source/전체종목.csv','r')as f:
    whole0 = csv.reader(f)
    first =True
    for line in whole0:
        if first:
            header = line
            first = False
            kos_in = header.index('시장구분')
            akmyeon_in = header.index('액면가')
            jusiksu_in = header.index('상장주식수')
        else:
            date0 = line[5].replace('/', '-')
            print(f"{line[0]}, {line[1]}, {line[3]}, {date0},{line[kos_in]},{line[akmyeon_in]},{line[jusiksu_in]}")
            cursor.execute(
                f"""
                INSERT INTO {db_name}.{table_name} VALUES("{line[0]}", "{line[1]}", "{line[3]}", "{date0}","{line[kos_in]}","{line[akmyeon_in]}",{line[jusiksu_in]});
                """
            )

    db.commit()