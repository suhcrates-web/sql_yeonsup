import mysql.connector
from database import cursor, db
import glob, os
from ToolBox import table_empty, table_checker, all_tables

#### corps 테이블들을 하나로 묶음####
##근데 이건 비효율적
##실수로 내용물 비워버림.

db_name = 'test1'
cursor.execute(
    f"""
    DROP TABLE IF EXISTS {db_name}.종목시세들;
    """
)
# cursor.execute(
#     f"""
#     CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8';
#     """
# )

cursor.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {db_name}.종목시세들(
    no BIGINT PRIMARY KEY,
    corp_cd VARCHAR(6),
    date0 DATE,
    rate DECIMAL(6,2),
    jongga BIGINT,
    siga BIGINT,
    goga BIGINT,
    jeoga BIGINT,
    amount BIGINT,
    daegum BIGINT,
    sichong BIGINT,
    jusiksu BIGINT,
    made_date DATE
    );
    """
)

file_list = []
##### 파일리스트 넣기
list_0 = glob.glob(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/corps/*')
for i in list_0:
    temp0 = os.path.basename(i).replace('.csv', '').split('_')  ### [corp_cd , made_date]
    file_list.append(temp0)

n =0
for temp0 in file_list:
    with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/corps/{temp0[0]}_{temp0[1]}.csv','r')as f:
        line = f.readline()
        corp_cd = temp0[0]
        i = temp0[1]
        made_date = str(i[0:4])+'-'+str(i[4:6])+'-'+str(i[6:8])
        print(corp_cd)
        while True:
            n+=1
            line = f.readline().replace('\n', '').split(',')
            i = line[0]
            date0 = str(i[0:4])+'-'+str(i[4:6])+'-'+str(i[6:8])
            if line != ['']:
                # print(line)
                cursor.execute(
                    f"""
                        INSERT INTO {db_name}.종목시세들 VALUES({n},"{corp_cd}","{date0}",{line[1]},{line[2]},{line[3]},{line[4]},{line[5]},{line[6]},{line[7]},{line[8]},{line[8]},"{made_date}");
                        """
                )
            else:
                break
db.commit()
