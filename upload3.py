import mysql.connector
from database import cursor, db
import glob, os, csv
from ToolBox import table_empty, table_checker, all_tables

####corps  옮기기.
db_name = 'corps'

#db 지우기
# cursor.execute(
#     f"""
#     DROP DATABASE {db_name};
#     """
# )

##db 생성
cursor.execute(
    f"""
    CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET 'utf8';
    """
)

### 파일리스트 관리 ###(없어도될듯)
# cursor.execute(
#     f"""
#     CREATE TABLE IF NOT EXISTS {db_name}.corps_파일리스트(
#     datename varchar(16) NOT NULL PRIMARY KEY
#     );
#     """
# )

##### 파일리스트.  컴에 있는 것.
file_list = []
list_0 = glob.glob(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/corps/*')
for i in list_0:
    file_temp = os.path.basename(i).replace('.csv', '')
    file_list.append(file_temp)

#이미 올라간 리스트
already_list = all_tables('corps')
# print(already_list)
n=0
for i in file_list:
    n += 1
    if i.lower() in already_list: #sql 에 없으면 고고
        pass
    else:

        print(f"{i} / {n}/{len(file_list)}")
        with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/corps/{i}.csv','r')as f:
            header = f.readline().replace('\n', '').split(',')
            cursor.execute(
                f"""
                CREATE TABLE {db_name}.{i} (
                date0 DATE PRIMARY KEY,
                rate DECIMAL(5,2), 
                jongga INT,
                siga INT,
                goga INT,
                jeoga INT,
                amount INT,
                daegum BIGINT,
                sichong BIGINT,
                jusiksu BIGINT
                );
                """
            )
            while True:
                line = f.readline().replace('\n', '').split(',')
                # print(line)
                if line != ['']:
                    date0 = str(line[0])
                    date0 = date0[0:4]+'-'+date0[4:6]+'-'+date0[6:8]
                    cursor.execute(
                        f"""
                            INSERT INTO {db_name}.{i} VALUES("{date0}",{line[1]},{line[2]},{line[3]},{line[4]},{line[5]},{line[6]},{line[7]},{line[8]},{line[9]});
                            """
                    )


                else:
                    break
        db.commit()
