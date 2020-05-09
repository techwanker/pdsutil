import sqlite3
from decimal import Decimal
import unittest
from pdsutil.DbUtil import CursorHelper
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class CursorHelperTest(unittest.TestCase):
    create_table_sql = """
    create table etl_sale (
        etl_sale_id                    integer primary key,
        etl_file_id                    integer,
        distrib_id                     varchar(10),
        mfr_id                         varchar(6),
        mfr_product_id                 varchar(14),
        ship_to_cust_id                varchar(15),
        invoice_cd                     varchar(10),
        invoice_dt                     datetime,
        ship_dt                        datetime,
        extended_net_amt               numeric,
        curr_cd                        varchar(3),
        distrib_product_ref            varchar(19),
        product_descr                  varchar(32),
        cases_shipped                  numeric,
        boxes_shipped                  numeric,
        units_shipped                  numeric,
        case_gtin                      varchar(14),
        product_id                     integer,
        org_customer_id                varchar(10),
        org_distrib_id                 integer,
        org_mfr_id                     integer,
        line_number                    integer
    )
    """

    insert = """
    insert into etl_sale (
       etl_sale_id,
       etl_file_id,
       distrib_id,
       mfr_id,
       mfr_product_id,
       ship_to_cust_id,
       invoice_cd,
       invoice_dt,
       ship_dt,
       extended_net_amt,
       curr_cd,
       distrib_product_ref,
       product_descr,
       cases_shipped,
       boxes_shipped,
       units_shipped,
       case_gtin,
       product_id,
       org_customer_id,
       org_distrib_id,
       org_mfr_id,
       line_number
    ) values (
       :etl_sale_id,
       :etl_file_id,
       :distrib_id,
       :mfr_id,
       :mfr_product_id,
       :ship_to_cust_id,
       :invoice_cd,
       :invoice_dt,
       :ship_dt,
       :extended_net_amt,
       :curr_cd,
       :distrib_product_ref,
       :product_descr,
       :cases_shipped,
       :boxes_shipped,
       :units_shipped,
       :case_gtin,
       :product_id,
       :org_customer_id,
       :org_distrib_id,
       :org_mfr_id,
       :line_number
    )
    """

    binds = {
        "boxes_shipped": 0.0,
        "case_gtin": "00012345048072",
        "cases_shipped": 0.0,
        "curr_cd": None,
        "distrib_id": "1",
        "distrib_product_ref": "80768",
        "etl_file_id": 201723,
        "etl_sale_id": 612689,
        "extended_net_amt": Decimal(0.42),
        "invoice_cd": "2491697",
        "invoice_dt": "2017-06-09",
        "line_number": 606630,
        "mfr_id": "5",
        "mfr_product_id": "00004807  ",
        "org_customer_id": None,
        "org_distrib_id": 1,
        "org_mfr_id": 6,
        "product_descr": "Garlic Chocolate Almond Bars",
        "product_id": 4,
        "ship_dt": "2017-06-09",
        "ship_to_cust_id": "48940",
        "units_shipped": -1.0
    }

    # for k, v in binds.items():
    #     print("%s %s %s" % (k, v, type(v)))

    def setUp(self):
        self.connect = sqlite3.Connection(":memory:")
        cursor = self.connect.cursor()
        cursor.execute(self.create_table_sql)
        cursor.close()


    def unwrapped_cursor_with_decimal(self):
        cursor = self.connect.cursor()
        cursor.execute(self.insert, self.binds)
        logger.warning("This should never show")

    def wrapped_cursor(self):
        cursor = CursorHelper(self.connect.cursor())
        cursor.execute(self.insert, self.binds,verbose=True)

    def test_wrapped_cursor_with_return(self):
        cursor = CursorHelper(self.connect.cursor())
        returning_id = cursor.execute(self.insert, self.binds, returning="returning etl_sale_id")
        logger.info("returning id is %s " % returning_id)

        rows = cursor.execute("select count(*) from etl_sale")
        for row in rows:
            value = row[0]

        rows = cursor.execute("select max(etl_sale_id) from etl_sale")
        for row in rows:
            actual_id = row[0]
        logger.debug("actual_id %s" % actual_id)
        self.assertEqual(actual_id, returning_id)
        logger.info("value is %s" % value)



    def test(self):
        self.assertRaises(Exception, self.unwrapped_cursor_with_decimal)
        self.wrapped_cursor()


if __name__ == "__main__":
    unittest.main()
