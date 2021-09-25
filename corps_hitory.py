import requests, json, time
from krx_ToolBox import fullcode_finder
from ToolBox import *
from database_remote import cursor_r
from datetime import datetime, date

# 파일 최신거로 업데이트  #giveme_history 개조함.
def corps_history(corp_cd='060310', last_day='', com='remote'):  ## last_day : 내가 설정한 추이 마지막날.
    last_day = datetime.strptime(last_day, '%Y%m%d').date()
    if com == 'origin':
        cursor_com = cursor
        db_com = db
    elif com == 'remote':
        cursor_com = cursor_r
        db_com = db_r


    # today0 = (datetime.today() -timedelta(days=1)).strftime("%Y%m%d")  #사실상 어제제

    ###파일이 있는지 없는지 확인. 최신파일이 아니면 지움. 최종적으로 찾는 corp_cd가 없으면 file_down은 True
    file_down = True
    if not table_checker('corps',corp_cd,com): #해당 테이블이 없음
        pass #file_down = True
    else:
        date0 = datetime.strptime(read_comment('corps', corp_cd, com), '%Y-%m-%d').date()  #테이블코멘트 가져오기. 기준일
        if date0 >= last_day:  # 만들어진 날짜가 last_day보다 많으면 파일 다운 안함
            file_down = False
        else:
            remove_table('corps', corp_cd, 'remote')

    ##file_down 이 True면 파일 다운받음
    if file_down:
        print(f"다운로드 : {corp_cd}")


        url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'

        header = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ko,en-US;q=0.9,en;q=0.8,ko-KR;q=0.7',
            'Connection': 'keep-alive',
            'Content-Length': '315',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Cookie': '__smVisitorID=s6nhVf27UJO; JSESSIONID=6l651PVvhRi1jzscLyWcD4ea8l3Yo64MDVW0P5ti9aAVKy1qpXIWIf4HHigCvHW3.bWRjX2RvbWFpbi9tZGNvd2FwMi1tZGNhcHAwMQ==; finder_stkisu_finderCd=finder_stkisu; finder_stkisu_tbox=005930%2F%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90; finder_stkisu_codeNm=%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90; finder_stkisu_codeVal=KR7005930003; finder_stkisu_codeVal2=KR7005930003; finder_secuprodisu_finderCd=finder_secuprodisu; finder_secuprodisu_tbox=005930%2F%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90; finder_secuprodisu_codeNm=005930%2F%EC%82%BC%EC%84%B1%EC%A0%84%EC%9E%90; finder_secuprodisu_codeVal=KR7005930003; finder_secuprodisu_codeVal2=KR7005930003',
            'Host': 'data.krx.co.kr',
            'Origin': 'http://data.krx.co.kr',
            'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020203',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest'
        }

        # try:
        fullcode = fullcode_finder(corp_cd=corp_cd)
        time.sleep(3)
        # except:
        #     print(corp_cd)
        #     traceback.print_exc()
        #     exit()

        data = {
            'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
            'tboxisuCd_finder_stkisu0_1': corp_cd,
            'isuCd': fullcode,
            'strtDd': '19990101',
            'endDd': last_day.strftime('%Y%m%d'),
            'share': '1',
            'money': '1',
            'csvxls_isNo': 'false'
        }
        temp = requests.post(url, data=data, headers=header)
        temp = json.loads(temp.content.decode('utf-8'))['output']
        cursor_com.execute(
            f"""
                    CREATE TABLE IF NOT EXISTS corps.{corp_cd}(
                    no BIGINT PRIMARY KEY,
                    date0 DATE,
                    rate DECIMAL(5,2),
                    jongga BIGINT,
                    siga BIGINT,
                    goga BIGINT,
                    jeoga BIGINT,
                    amount BIGINT,
                    daegum BIGINT,
                    sichong BIGINT,
                    jusiksu BIGINT
                    )
                    """
        )
        n = 1
        for i in temp:
            date0 = i['TRD_DD'].replace('/', '')  # 날짜
            rate = i['FLUC_RT']  # 증가율
            jongga = i['TDD_CLSPRC'].replace(',', '')  # 종가
            siga = i['TDD_OPNPRC'].replace(',', '')  # 시가
            goga = i['TDD_HGPRC'].replace(',', '')  # 고가
            jeoga = i['TDD_LWPRC'].replace(',', '')  # 저가
            amount = i['ACC_TRDVOL'].replace(',', '')  # 거래량
            daegum = i['ACC_TRDVAL'].replace(',', '')  # 거래대금
            sichong = i['MKTCAP'].replace(',', '')  # 시총
            jusiksu = i['LIST_SHRS'].replace(',', '')  # 상장주식수
            cursor_com.execute(
                f"""
                INSERT INTO corps.{corp_cd} VALUES({n},"{date0}",{rate},{jongga},{siga},{goga},{jeoga},{amount},{daegum},{sichong},{jusiksu});
                """
            )
            n += 1
        db_com.commit()
        write_comment('corps', corp_cd,last_day,'remote')



# corps_history 를 한번 거친뒤  해당 corp_cd의 종목 히스토리를 내놓음
def giveme_history(corp_cd='060310', last_day='', com='remote'):  ## last_day : 내가 설정한 추이 마지막날.
    corps_history(corp_cd=corp_cd, last_day=last_day, com=com)
    if com == 'origin':
        cursor_com = cursor
        db_com = db
    elif com == 'remote':
        cursor_com = cursor_r
        db_com = db_r

    cursor_com.execute(
        f"""
        SELECT * FROM corps.{corp_cd};
        """
    )
    result = cursor_com.fetchall()
    header = check_colname('corps',corp_cd,'remote')
    date_in = header.index('date0')
    value_in = header.index('rate')
    sichong_in = header.index('sichong')
    jongga_in = header.index('jongga')

    dics = {}
    for line in result:
        dics[line[date_in]] = {}
        dics[line[date_in]]['rate'] = line[value_in]
        dics[line[date_in]]['sichong'] = line[sichong_in]
        dics[line[date_in]]['jongga'] = line[jongga_in]
    return dics
