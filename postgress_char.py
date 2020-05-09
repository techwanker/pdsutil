# !/usr/bin/python
import psycopg2


url = "host='localhost' dbname='sales_reporting_db' user='jjs' password='xxxxxx'"
conn = psycopg2.connect(url)
cursor = conn.cursor()
cursor.execute('create table x ( y char(2))')
cursor.execute("insert into x(y) values ('1')")
cursor.execute('select y from x')
for row in cursor:
   data = row[0]
   print('length is: ', len(data), '"' + data + '"')
cursor.close()
