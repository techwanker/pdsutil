#!/usr/bin/env python
import logging
import unittest

import pdsutil.DbUtil as DbUtil
import pdsutil.setup_logging as log_setup

log_setup.setup_logging("../logging.yaml")

logger = logging.getLogger(__name__)
class sql_util_test(unittest.TestCase):

    def test_2_1(self):
        sql = """
        select etl_sale_id, etl_file_id, mfr_id, mfr_product_id, ship_to_cust_id, invoice_cd
        from etl_sale
        where etl_file_id = %(ETL_FILE_ID)s or
              mfr_id = %(ETL_MFR_ID)s or %(ETL_FILE_ID)s is null
        """
        binds = DbUtil.find_binds(sql)
        self.assertEqual(len(binds),2)
        self.assertTrue("ETL_FILE_ID" in binds)
        self.assertTrue("ETL_MFR_ID" in binds)

    def test_2(self):
        sql = """
          select etl_sale_id, etl_file_id, mfr_id, mfr_product_id, ship_to_cust_id, invoice_cd
          from etl_sale
          where etl_file_id = %(ETL_FILE_ID)s or
                mfr_id = %(ETL_MFR_ID)s  is null
          """
        binds = DbUtil.find_binds(sql)
        self.assertEqual(len(binds), 2)
        self.assertTrue("ETL_FILE_ID" in binds)
        self.assertTrue("ETL_MFR_ID" in binds)


    def test_0(self):
        sql = """
              select etl_sale_id, etl_file_id, mfr_id, mfr_product_id, ship_to_cust_id, invoice_cd
              from etl_sale
              where etl_file_id = is null
              """
        binds = DbUtil.find_binds(sql)
        self.assertEqual(len(binds), 0)

    def test_replace(self):
        sql = """
        select etl_sale_id, etl_file_id, mfr_id, mfr_product_id, ship_to_cust_id, invoice_cd
        from etl_sale
        where etl_file_id = %(ETL_FILE_ID)s or
              mfr_id = %(ETL_MFR_ID)s  is null
        """
        new_sql = DbUtil.to_colon_binds(sql)
        logger.debug(new_sql)
        expected = """
        select etl_sale_id, etl_file_id, mfr_id, mfr_product_id, ship_to_cust_id, invoice_cd
        from etl_sale
        where etl_file_id = :ETL_FILE_ID or
              mfr_id = :ETL_MFR_ID  is null
        """
        self.assertEqual(new_sql,expected)

if __name__ == '__main__':
    unittest.main()
