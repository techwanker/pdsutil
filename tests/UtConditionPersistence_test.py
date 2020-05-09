#!/usr/bin/env python
import unittest

from pdsutil.DbUtil import ConnectionHelper
from pdsutil.DbUtil import Cursors
from pdsutil.UtConditionPersistence import UtConditionPersistence

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")


class UtConditionPersistenceTest(unittest.TestCase):
    def test_fetch_or_insert(self):
        connection = ConnectionHelper().get_named_connection("test")
        cursors = Cursors(connection)
        binds = {
            "rule_name": "TEST_COND",
            "table_name": "etl_sale",
            "msg": "I don't know",
            "sql_text": "select id from etl_sale where etl_file_id = %(ETL_FILE_ID)",
            "narrative": "huh",
            "severity": 3,
            "format_str": "id % is %s",
            # "CORRECTIVE_ACTION" : "Fix it"
        }

        id = UtConditionPersistence.fetch_or_insert(cursors, binds)
        connection.commit()

        assert (id is not None)


if __name__ == '__main__':
    unittest.main()
