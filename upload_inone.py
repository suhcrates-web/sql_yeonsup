import mysql.connector
from database import cursor, db
import glob, os
from ToolBox import table_empty, table_checker, all_tables

db_name = 'test1'
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
    CREATE TABLE IF NOT EXISTS {db_name}.연기금_파일리스트(
    no BIGINT,
    date0 DATE,
    corp_cd VARCHAR(6),
    corp_nm VARCHAR(20), 
    am_medo BIGINT,
    am_mesu BIGINT,
    am_sunmesu BIGINT,
    num_medo BIGINT,
    num_mesu BIGINT,
    num_sunmesu BIGINT
    );
    """
)

date_list = []
##### 파일리스트 넣기
list_0 = glob.glob(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/date/*')
for i in list_0:
    date_temp = os.path.basename(i).replace('.csv', '').replace('toojaja', '')
    date_list.append(date_temp)

n =0
for i in date_list:
    with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/date/toojaja{i}.csv','r')as f:
        line = f.readline().replace('\n', '').split(',')
        i = str(i[0:4])+'-'+str(i[4:6])+'-'+str(i[6:8])
        print(i)
        while True:
            n+=1
            line = f.readline().replace('\n', '').split(',')
            if line != ['']:
                cursor.execute(
                    f"""
                        INSERT INTO {db_name}.연기금_파일리스트 VALUES({n},"{i}","{line[0]}","{line[1]}",{int(line[2])},{int(line[3])},{int(line[4])},{int(line[5])},{int(line[6])},{int(line[7])});
                        """
                )
            else:
                break
db.commit()
