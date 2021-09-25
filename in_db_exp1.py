import mysql.connector
from database import cursor, db

with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/source/전체종목.csv','r')as f:
    header = f.readline().replace('\n', '').split(',')
    print(header)
    corp_lcd_in = header.index('표준코드')
    corp_cd_in = header.index('단축코드')
    corp_nm_in = header.index('한글 종목약명')

    cursor.execute(
        f"""
        DROP TABLE exp1.전체종목;
        """
    )

    cursor.execute(
        f"""
        CREATE TABLE IF NOT EXISTS 연기금_매수종목.전체종목(
        표준코드 varchar(13) NOT NULL PRIMARY KEY,
        단축코드 varchar(6) NOT NULL,
        한글종목명 varchar(20) NOT NULL
        );
        """
    )
    while True:
        line = f.readline().replace('\n', '').split(',')
        if line != ['']:
            cursor.execute(
                f"""
                INSERT INTO exp1.전체종목 VALUES({line[corp_lcd_in]},{line[corp_cd_in]},{line[corp_nm_in]});
                """)
            print(line[corp_lcd_in])
            db.commit()
        else:
            break
