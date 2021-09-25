import mysql.connector
from database import cursor, db
from database_remote import cursor_r, db_r
import glob, os

#테이블 코멘트 읽고쓰기
def write_comment(db0, table0, comment, com):
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
        f"""
        ALTER TABLE {db0}.{table0} COMMENT = "{comment}";
        """
    )
def read_comment(db0, table0, com):
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
        f"""
        SELECT table_comment FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{db0}'
        AND TABLE_NAME = '{table0}';
        """
    )
    return cursor_com.fetchall()[0][0]

#col name  리스트로 가져오기
def check_colname(db0, table0, com):
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
        f"""
        DESCRIBE {db0}.{table0};
        """
    )
    result = cursor_r.fetchall()
    col_list = list(map(lambda x:x[0], result))
    return col_list

##테이블 삭제
def remove_table(db0, table0, com):# com : origin / remote
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
        f"""
        DROP TABLE IF EXISTS {db0}.{table0};
        """
    )

## 테이블이 만들어진 날짜를 출력
def made_date(db0, table0, com):
    if com == 'origin':
        com = cursor
    elif com == 'remote':
        com = cursor_r
    com.execute(
        f"""
        SELECT create_time FROM INFORMATION_SCHEMA.TABLES
        WHERE table_schema = '{db0}'
        AND table_name = '{table0}';
        """
    )
    result = com.fetchall()
    if result == []:
        return None
    else:
        return result[0][0].date()

## 가장 최근 날짜를 출력
def max_date(db0, table0, colname, com):
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
        f"""
        SELECT MAX({colname}) FROM {db0}.{table0};
        """
    )
    result = cursor_com.fetchall()
    return result[0][0]


#테이블 있는지 확인.
def table_checker(db0, table0, com):
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
       f"""
        SELECT count(*)
        FROM information_schema.TABLES
        WHERE (TABLE_SCHEMA = '{db0}') AND (TABLE_NAME = '{table0}');
        """
    )
    result = cursor_com.fetchall()[0][0]
    if result ==0:
        return False
    if result ==1:
        return True

# 테이블에 내용이 없는지 확인
def table_empty(db0, table0):

    cursor.execute(
       f"""
        SELECT count(*)
        FROM {db0}.{table0};
        """
    )
    result = cursor.fetchall()[0][0]
    if result ==0:
        return True
    if result >0:
        return False

#db 내 모든 테이블 보여줌
def all_tables(db0, com):
    if com == 'origin':
        com = cursor
    elif com == 'remote':
        com = cursor_r
    list0 = []
    com.execute(
        f"""
        USE {db0};
        """
    )
    com.execute(
        """
        SHOW TABLES;
        """
    )

    result = com.fetchall()
    for i in result:
        list0.append(i[0])
    return list0
if __name__ == '__main__':
    print(all_tables('exp1'))


# corp_nm 넣으면 corp_cp 나옴
def corp_nm_to_cp(corp_nm):
    cursor.execute(
        f"""
        SELECT corp_cd FROM source.전체종목 WHERE corp_nm = "{corp_nm}";
        """
    )
    corp_cd = cursor.fetchall()[0][0]
    return corp_cd

###### corp_cd 종목의 from_when ~ end_when  의 주가 추이 가져옴
def jonmok_jongga(corp_cd, when):

    ###### 버전2
    col_name_part = ['date0','jongga']

    cursor.execute(
        f"""
        SELECT date0, jongga FROM corps_mend.{corp_cd} WHERE date0 = "{when}";
        """
    )
    result = cursor.fetchall()[0]
    print(result)
    juga_dics = {}
    juga_dics['date0'] = result[0]
    juga_dics['jongga'] = result[1]
    return juga_dics