import requests, json, time
from datetime import datetime, timedelta

def fullcode_finder( corp_cd=''):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'

    header = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'ko,en-US;q=0.9,en;q=0.8,ko-KR;q=0.7',
        'Connection': 'keep-alive',
        'Content-Length': '315',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Cookie': '__smVisitorID=mgX3gboq6Sf; __utmz=139639017.1612159881.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); finder_stkisu_finderCd=finder_stkisu; finder_stkisu_param1=STK; finder_stkisu_typeNo=0; JSESSIONID=K9rSGrpfJ1s7Hw4LCKMDumbkzaITTI2I08tBy9q5v8zIUswQJM4i1zICE9FYfepM.bWRjX2RvbWFpbi9tZGNvd2FwMS1tZGNhcHAwMQ==; finder_stkisu_codeVal2=KR7334970001; __utma=139639017.1458902062.1612159881.1615958375.1616638506.6; __utmc=139639017; __utmt=1; __utmb=139639017.1.10.1616638506; finder_stkisu_tbox=282330%2FBGF%EB%A6%AC%ED%85%8C%EC%9D%BC; finder_stkisu_codeNm=BGF%EB%A6%AC%ED%85%8C%EC%9D%BC; finder_stkisu_codeVal=KR7282330000',
        'Host': 'data.krx.co.kr',
        'Origin': 'http://data.krx.co.kr',
        'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/89.0.4389.90 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }

    data = {
        'mktsel': 'ALL',
        'typeNo': '0',
        'searchText':corp_cd,
        'bld': """dbms/comm/finder/finder_stkisu"""
    }
    # print(data)
    temp = requests.post(url, data=data, headers = header)
    temp = json.loads(temp.content.decode('utf-8'))
    # print(temp)
    # time.sleep(3)
    return temp['block1'][0]['full_code']

