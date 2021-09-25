import requests, json, time
from krx_ToolBox import fullcode_finder
from ToolBox import *
from database_remote import cursor_r
from database import cursor
from corps_hitory import corps_history

### corps 데이타베이스 업데이트. remote가 기본.
cursor.execute(
        f"""
        SELECT 회사코드, 회사이름 FROM 연기금_지분율보고.지분공시20201231_amend;;
        """
    )
result = cursor.fetchall()
dics_todo = {k:v for k,v in result}
miss_list =[]
for corp_cd in dics_todo:
    try:
        corps_history(corp_cd, '20210924', 'remote')
    except:
        miss_list.append([corp_cd, dics_todo[corp_cd]])
    time.sleep(6)
print(miss_list)