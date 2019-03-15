import pymysql
import pandas as pd
import os

class Interest_MySQL:
    def __init__(self):
        self.mysql_db_host = ''
        self.mysql_db_port = 3306
        self.mysql_db_user = ''
        self.mysql_db_pwd = ''
        self.mysql_db_name = ''

    def mysql_conn(self):
        # conn = pymysql.connect(host='localhost', port=3306, db='mytest', user='root', passwd='', charset='utf8')
        conn = pymysql.connect(host=self.mysql_db_host, port=self.mysql_db_port, user=self.mysql_db_user,
                               passwd=self.mysql_db_pwd, db=self.mysql_db_name, charset='utf8')
        return conn

    def select_and_save(self, cursor):
        nums = cursor.execute('select * from dw_dim_interest')
        data = cursor.fetchall()
        with open('dw_dim_interest.csv','a+') as fopen:
            fopen.write('id,name,type')
            for item in data:
                fopen.write(str(item[0])+','+str(item[1])+str(',')+str(item[2])+'\n')
                print(item)

    def delete_mysql(self):
        conn = self.mysql_conn()
        cursor = conn.cursor()
        orign_data = set(pd.read_csv('dw_dim_interest.csv')['id'])
        top4000_data = set(pd.read_csv('top4000_interest.csv')['id'])
        differ = orign_data-top4000_data
        sql = "delete from ai_ad_targeting_prod.dw_dim_interest where id=%s"
        lst = []
        for id in differ:
            lst.append(id)
            cursor.execute(sql % id)
            conn.commit()
            print(sql % id, ';')
        cursor.close()
        conn.cursor()

    def main(self):
        self.delete_mysql()


if __name__ == '__main__':
    imysql = Interest_MySQL()
    imysql.main()
