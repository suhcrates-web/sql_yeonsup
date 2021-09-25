import csv
from database import cursor, db


db_name = '연기금_지분율보고'
table_name = 'bogo_20210907'

cursor.execute(
    f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{table_name}(
    no INT NOT NULL PRIMARY KEY,
    corp_nm varchar(20) NOT NULL,
    gijun_date DATE NOT NULL, 
    amount BIGINT NOT NULL
    );
    """
)

with open(f'C:/Users/infomax/Desktop/Computer/Computer학습/Python/project_infomax/연기금수익률/data/연기금_지분율보고/bogo_20210907.csv','r')as f:
    whole0 = csv.reader(f)
    first = True
    for line in whole0:
        if first:
            first = False
        else:
            print(line)
            corp_nm = line[1]
            if corp_nm == '':
                pass



            else:
                amount = line[6] if line[6] != '' else 0
                cursor.execute(
                    f"""
                    INSERT INTO {db_name}.{table_name} VALUES({line[0]}, "{corp_nm}", "{line[3]}",{amount});
                    """
                )

    db.commit()