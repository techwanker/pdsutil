
import sys
from  pdsutil.DbUtil import ConnectionHelper, CursorHelper
from pdsutil.Dataset import Dataset

import datetime
import logging

logging.basicConfig(level=logging.INFO)
sql = "select * from etl_sale where etl_file_id = %(ETL_FILE_ID)s"
binds = {"ETL_FILE_ID" : 201723}
connection = ConnectionHelper().get_named_connection("current")
cursor = CursorHelper(connection.cursor())


sales = Dataset.from_sql(connection,sql,binds)
# sales.to_csv(sys.stdout)

# to_csv
out_file = open("/tmp/sales.csv","w")
sales.to_csv(out_file)

# to_sqlite
sales.set_column_meta("curr_cd",str,3)
sales.set_column_meta("org_customer_id",str,10)
db = sales.to_sqlite("etl_sale",verbose=False)
cursor = CursorHelper(db.cursor())
rows = cursor.execute("select count(*) from etl_sale")
for row in rows:
    print (row)


