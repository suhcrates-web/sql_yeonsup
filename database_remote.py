import mysql.connector

config = {
    'user' : 'yb_notebook',
    'password': 'Seoseoseo7!',
    'host':'172.30.1.43',
    # 'database':'shit',
    'port':'3306'
}
db_r = mysql.connector.connect(**config)
cursor_r = db_r.cursor()
# cursor.execute(
#     """
#     show databases;
#     """
# )
# print(cursor.fetchall())