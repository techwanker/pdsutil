import datetime
import logging
import unittest

from pdsutil.DbUtil import ConnectionHelper
from pdsutil.Dataset import Dataset
import os
import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)



class DatasetTest(unittest.TestCase):



    def test_from_items(self):
        logging.debug("test Dataset.from_items")
        ds = Dataset.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])])
        self.assertEqual(ds.column_names[0], 'A')
        self.assertEqual(ds.column_names[1], 'B')
        self.assertEqual(len(ds.column_names), 2)

        self.assertEqual(ds.rows[0][0], 1)
        self.assertEqual(ds.rows[1][0], 2)
        self.assertEqual(ds.rows[2][0], 3)
        self.assertEqual(ds.rows[0][1], 4)
        self.assertEqual(ds.rows[1][1], 5)
        self.assertEqual(ds.rows[2][1], 6)
        self.assertEqual(len(ds.rows), 3)
        self.assertEqual(len(ds.rows[0]), 2)
        self.assertEqual(len(ds.rows[1]), 2)

        logger.debug ("test_from_items\n%s" % ds)


    def test_from_items_index(self):
        logging.debug("test Dataset.from_items")
        ds = Dataset.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])],orient="index",
                                columns=['one','two','three'])
        self.assertEqual(ds.column_names[0],'one')
        self.assertEqual(ds.column_names[1],'two')
        self.assertEqual(ds.column_names[2],'three')
        self.assertEqual(len(ds.column_names),3)
        self.assertEqual(ds.rows[0][0],1)
        self.assertEqual(ds.rows[0][1],2)
        self.assertEqual(ds.rows[0][2],3)
        self.assertEqual(ds.rows[1][0],4)
        self.assertEqual(ds.rows[1][1],5)
        self.assertEqual(ds.rows[1][2],6)
        self.assertEqual(len(ds.rows),2)
        self.assertEqual(len(ds.rows[0]),3)
        self.assertEqual(len(ds.rows[1]),3)
        self.assertEqual(ds.row_key[0],'A')
        self.assertEqual(ds.row_key[1],'B')
        self.assertEqual(len(ds.row_key),2)
        logger.debug ("test_from_items_index\n%s" % ds)

    def test_from_tuples(self):
        obj = [(1,"hello",datetime.date(2017,5,6)),
               (2,"goodbye",datetime.date(1965,7,31))]
        ft = Dataset.from_tuples(obj,["int","str","dt"])
        self.assertEqual(ft.rows[0][0],1)
        self.assertEqual(ft.rows[0][1],"hello")
        self.assertEqual(ft.rows[0][2],datetime.date(2017,5,6))
        self.assertEqual(ft.rows[1][0],2)
        self.assertEqual(ft.rows[1][1],"goodbye")
        self.assertEqual(ft.rows[1][2],datetime.date(1965,7,31))
        self.assertEqual(len(ft.rows),2)
        self.assertEqual(len(ft.rows[0]),3)
        self.assertEqual(len(ft.rows[1]),3)

    def test_from_list_of_lists(self):
        obj = [[1,"hello",datetime.date(2017,5,6)],
               [2,"goodbye",datetime.date(1965,7,31)]
        ]
        ft = Dataset.from_list_of_lists(obj,["int","str","dt"])
        logger.debug("row 0 %s" % ft.rows )
        self.assertEqual(ft.rows[0][0],1)
        self.assertEqual(ft.rows[0][1],"hello")
        self.assertEqual(ft.rows[0][2],datetime.date(2017,5,6))
        self.assertEqual(ft.rows[1][0],2)
        self.assertEqual(ft.rows[1][1],"goodbye")
        self.assertEqual(ft.rows[1][2],datetime.date(1965,7,31))
        self.assertEqual(len(ft.rows),2)
        self.assertEqual(len(ft.rows[0]),3)
        self.assertEqual(len(ft.rows[1]),3)
        self.assertEqual(ft.column_names[0],"int")
        self.assertEqual(ft.column_names[1],"str")
        self.assertEqual(ft.column_names[2],"dt")
        self.assertEqual(len(ft.column_names),3)

    def test_to_sqlite(self):
        logger = logging.getLogger(__name__ + ":test_to_sqlite")
        obj = [[1, "hello", datetime.date(2017, 5, 6)],
               [2, "goodbye", datetime.date(1965, 7, 31)]
               ]
        ft = Dataset.from_list_of_lists(obj, ["int", "str", "dt"])
        now = datetime.datetime.now()
        connection = ft.to_sqlite("list_of_lists")
        logger.debug("elapsed %s " % (datetime.datetime.now() - now))
        cursor = connection.cursor()
        rows = cursor.execute("select count(*) from list_of_lists")
        for row in rows:
            logger.debug ("count %s " % row[0])

    def test_dataset_from_sql(self):
        sql = "select * from etl_sale where etl_file_id = %(ETL_FILE_ID)s"
        binds = {"ETL_FILE_ID": 201723}
        connection = ConnectionHelper().get_named_connection("current")
        # pandas.read_sql(sql, con, index_col=None, coerce_float=True, params=None, parse_dates=None, columns=None, chunksize=None)[source]
        now = datetime.datetime.now()
        sale_dataframe = Dataset.read_sql(sql, connection, params=binds)
        logger.debug("test_dataset_from_sql elapsed %s " % (datetime.datetime.now() - now))
        logger.debug("rowcount %s" % len(sale_dataframe.rows))


if __name__ == "__main__":
    unittest.main()


