import mysql.connector
import requests, json, time
from database import cursor, db
from datetime import date, timedelta, datetime
from ToolBox import *



#### 2020 12 31자로 나온 기금본부 지분공시 파일 바탕으로  SQL 테이블 만들기. ######
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS 연기금_지분율보고.지분공시20201231_amend(
    번호 BIGINT,
    회사코드 VARCHAR(6) PRIMARY KEY,
    회사이름 VARCHAR(20),
    보유량 BIGINT,
    날짜 DATE
    );
    """
)
with open('C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/source/연기금_보유현황_2020말.csv') as f:
    header  = f.readline().replace('\n','').split(',')
    error_list = []
    corp_error = []
    mysql_error = []
    out_of_corpsmend = []
    n=1
    corps_mend_tables = all_tables('corps_mend')
    while True:

        line = f.readline().replace('\n','').split(',')
        print(line)
        if line != ['']:
            corp_nm = line[1]
            date0 =  datetime.strptime(line[5], '%Y-%m-%d').date() -timedelta(days=1)
            num_boyu = float(line[2]) * 100000000
            print(date0)
            print(f"{corp_nm}")
            try:
                corp_cd = corp_nm_to_cp(corp_nm)
                print(corp_nm_to_cp(corp_nm))
                if corp_cd in corps_mend_tables:
                    try:
                        jongga0 = jonmok_jongga(corp_cd, date0)['jongga']
                        am_boyu = round(num_boyu / jongga0)

                        cursor.execute(
                            f"""
                            INSERT INTO 연기금_지분율보고.지분공시20201231_amend VALUES({n},"{corp_cd}","{corp_nm}",{am_boyu},"{date0}");
                            """
                        )
                        n += 1
                    except IndexError as e:
                        corp_error.append(corp_nm)
                        print(e)
                    except (mysql.connector.Error) as e:
                        mysql_error.append(corp_nm)
                        print(e)
                else:
                    out_of_corpsmend.append(corp_nm)
            except IndexError:
                print('없음')
                error_list.append(corp_nm)




        else:
            break
db.commit()
print('::::::')
print(error_list)
print('============')
print(corp_error)
print(mysql_error)


