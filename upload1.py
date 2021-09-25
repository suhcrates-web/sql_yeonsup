import mysql.connector
from database import cursor, db
import glob, os
from ToolBox import table_empty, table_checker, all_tables

db_name = 'exp2'
# cursor.execute(
#     f"""
#     DROP DATABASE {db_name};
#     """
# )
cursor.execute(
    f"""
    CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8';
    """
)

cursor.execute(
    f"""
    CREATE TABLE IF NOT EXISTS exp2.연기금_파일리스트(
    datename varchar(16) NOT NULL PRIMARY KEY
    );
    """
)

if table_empty('exp2','연기금_파일리스트'):
    ##### 파일리스트 넣기
    list_0 = glob.glob(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/date/*')
    for i in list_0:
        date_temp = os.path.basename(i).replace('.csv', '').replace('toojaja', '')
        cursor.execute(
            f"""
            INSERT INTO exp2.연기금_파일리스트 VALUES({date_temp});
            """
        )
    db.commit()


cursor.execute(
    """
    SELECT * FROM exp2.연기금_파일리스트;
    """
)
file_list = cursor.fetchall()
# db.commit()
# print(file_list)

already_list = all_tables('exp2')

for i in file_list:
    i = i[0]
    if f"yongi{i}" in already_list:
        pass
    else:
        print(i)
        with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/date/toojaja{i}.csv','r')as f:
            header = f.readline().replace('\n', '').split(',')
            cursor.execute(
                f"""
                DROP TABLE IF EXISTS exp2.yongi{i};
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE exp2.yongi{i} (
                corp_cd VARCHAR(6) PRIMARY KEY,
                corp_nm VARCHAR(20), 
                am_medo BIGINT,
                am_medu BIGINT,
                am_sunmesu BIGINT,
                num_medo BIGINT,
                num_medu BIGINT,
                num_sunmesu BIGINT
                );
                """
            )
            while True:
                line = f.readline().replace('\n', '').split(',')
                print(line)
                if line != ['']:
                    print()
                    cursor.execute(
                        f"""
                            INSERT INTO exp2.yongi{i} VALUES("{line[0]}","{line[1]}",{int(line[2])},{int(line[3])},{int(line[4])},{int(line[5])},{int(line[6])},{int(line[7])});
                            """
                    )


                else:
                    break
        db.commit()
