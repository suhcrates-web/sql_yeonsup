import mysql.connector

config = {
    'user' : 'root',
    'password': 'Seoseoseo7!',
    'host':'localhost',
    # 'database':'shit',
    'port':'3306'
}
db = mysql.connector.connect(**config)
cursor = db.cursor()

# if __name__ == '__main__':
    # cursor.execute(
    #     """
    #     CREATE TABLE shit2.fuck(
    #     id INT PRIMARY KEY,
    #     text VARCHAR(250) NOT NULL
    #     )
    #     """
    # )
    # for i in range(3,10):
    #     cursor.execute(
    #         f"""
    #         INSERT INTO shit.student VALUES({2+i},'man {i}','killing');
    #         """
    #     )

    # cursor.execute(
    #     """
    #     UPDATE `shit`.`student` SET `name` = 'jack6' WHERE (`name` = 'jack5');
    #     """
    # )
    # cursor.execute(
    #     """
    #
    #     SELECT * FROM shit.student;
    #     """
    # )
    # rows = cursor.fetchall()
    # db.commit()
    # print(rows)