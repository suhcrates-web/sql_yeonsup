import requests, json, time
from database import cursor, db
from datetime import date, timedelta


#테이블 없으면 만들기
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS krx_stuffs.외국인보유(
    date0 DATE,
    회사코드 VARCHAR(6),
    회사이름 VARCHAR(20),
    종가 INT,
    대비 INT,
    등락률 DECIMAL(5,2),
    상장주식수 BIGINT,
    외국인보유수량 BIGINT,
    외국인지분율 DECIMAL(5,2),
    외국인한도수량 BIGINT,
    외국인한도소진율 DECIMAL(5,2)
    );
    """
)

#최소 날짜 구하기
cursor.execute(
    """
    SELECT MAX(date0) FROM krx_stuffs.외국인보유;
    """
)
min_date = cursor.fetchall()[0][0]

if min_date == None:
    min_date = date(2005,10,3) #실제 데이타는 20051004부터 있기 떄문. mindate에 하루씩 더해서 씀.

date0 = min_date
print([date0])

###  krx 헤더정보
url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
header = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'ko,en-US;q=0.9,en;q=0.8,ko-KR;q=0.7',
    'Connection': 'keep-alive',
    'Content-Length': '129',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Cookie': '__smVisitorID=5lIgCqzecYa; JSESSIONID=IJvDPaWhobYYYScZC9eQrluJDy960IwzaglgfMxi1UKxpMQjJ4DyLx5jahwqs8iI.bWRjX2RvbWFpbi9tZGNvd2FwMi1tZGNhcHAwMQ==',
    'Host': 'data.krx.co.kr',
    'Origin': 'http://data.krx.co.kr',
    'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020301',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

### 루프
while date0 < date.today() - timedelta(days=1): #어제까지.
    date0 += timedelta(days=1)
    print(date0)

    data = {
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT03701',
        'searchType': '1',
        'mktId': 'ALL',
        'trdDd': date0.strftime('%Y%m%d'),
        'share': '1',
        'csvxls_isNo': 'false'
    }
    temp = requests.post(url, data=data, headers = header)

    temp = json.loads(temp.content.decode('utf-8'))['output']
    # print(temp)
    for i in temp:
        corp_cd = i['ISU_SRT_CD'] #회사코드
        corp_nm = i['ISU_ABBRV'] #회사이름
        jongga =  i['TDD_CLSPRC'].replace(',','')  #종가
        plus0 = i['CMPPREVDD_PRC'].replace(',','') #전날대비증가
        pl_rate = float(i['FLUC_RT'].replace(',','')) #전날대비증가율
        pl_rate = pl_rate if pl_rate < 999 else 0
        jusiksu = i['LIST_SHRS'].replace(',','') #상장주식수
        wein_su = i['FORN_HD_QTY'].replace(',','') or 0 #외국인 보유수량    ### or 0  : None이면 0으로. ******
        wein_rate = i['FORN_SHR_RT']  or 0 #외국인 지분율
        wein_hando = i['FORN_ORD_LMT_QTY'].replace(',','')  or 0 #외국인 한도수량
        wein_hando_rate = i['FORN_LMT_EXHST_RT']  or 0 #외국인 한도소진율
        # print(f'"{date0}","{corp_cd}","{corp_nm}",{jongga},{plus0},{pl_rate},{jusiksu},{wein_su},{wein_rate},{wein_hando},{wein_hando_rate}')
        cursor.execute(
            f"""
                INSERT INTO krx_stuffs.외국인보유 VALUES("{date0}","{corp_cd}","{corp_nm}",{jongga},{plus0},{pl_rate},{jusiksu},{wein_su},{wein_rate},{wein_hando},{wein_hando_rate});
                """
        )
    db.commit()
    time.sleep(6)