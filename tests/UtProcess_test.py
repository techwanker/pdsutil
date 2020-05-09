#!/usr/bin/env python
import unittest
from datetime import datetime
from pdsutil.UtProcess import UtProcess
from pdsutil.DbUtil import ConnectionHelper

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger()
class UtProcess_test(unittest.TestCase):
#class UtProcess_test():
    def test_all(self):
        connection = ConnectionHelper().get_named_connection("test")
        up = UtProcess(connection)
        binds = {
            "schema_nm" : "test",
            "process_nm" : "UtProcess_test",
            "thread_nm":  None,
            "process_run_nbr" : 1, #TODO
            "status_msg": "testing",
            "status_id": None,#class UtProcess_test(unittest.TestCase):
            "status_ts": None, #datetime().date(2017,4,6,0,0,0),
            "ignore_flg": "N"
        }
        up.insert_process(binds)

        connection.rollback()
# def test_sale(self):  #TODO complete

if __name__ == '__main__':
    p = UtProcess_test()
    p.test_all()
    #unittest.main()
