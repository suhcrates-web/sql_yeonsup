from database_remote import cursor_r, db_r
from ToolBox import *
from datetime import datetime, date
# cursor.execute(
#     """
#     USE corps_mend;
#     """
# )
# cursor.execute(
#     """
#     show table create_time where name = '000020';
#     """
# )

print(max_date('corps','060310', 'date0','remote'))
date0 = date(2021, 2,1)
def write_comment(db0, table0, comment, com):
    if com == 'origin':
        cursor_com = cursor
    elif com == 'remote':
        cursor_com = cursor_r
    cursor_com.execute(
        f"""
        ALTER TABLE corps.060310 COMMENT = "{comment}";
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
    return type(cursor_com.fetchall()[0][0])

# print(table_checker('corps','060311', 'rmote'))



# THEN (SELECT MAX({colname}) FROM {db0}.{table0}) ELSE 0 END;