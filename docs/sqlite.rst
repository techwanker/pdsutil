Error
Traceback (most recent call last):
  File "/common/home/jjs/python_projects/pdsutil/DbUtil.py", line 379, in execute
    result = self.cursor.execute(processed_sql, binds)
  File "/usr/lib/python3.5/sqlite3/dbapi2.py", line 64, in convert_date
    return datetime.date(*map(int, val.split(b"-")))
ValueError: invalid literal for int() with base 10: b'22 00:00:00'


import sqlite3
import datetime
db = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES)
c = db.cursor()
c.execute('create table foo (bar integer, baz timestamp)')

c.execute('insert into foo values(?, ?)', (23, datetime.datetime.now()))

c.execute('select * from foo')

rows = c.fetchall()
for row in rows:
    for col in row:
        print ("value %s type %s" % (col, str(type(col))))