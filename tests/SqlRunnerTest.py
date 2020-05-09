import unittest
import sqlite3
from pdsutil.SqlRunner import SqlRunner
import os
import sys
import pdsutil.tests.context as context

import pdsutil.SqlRunner

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")


class SqlRunnerTest(unittest.TestCase):
    sql_file = context.get_file_path("../resources/ut_condition_tables.sr.sql")

    def test_ut_condition_sqlite_mem(self):
        connection = sqlite3.connect(":memory:")
        runner = SqlRunner(self.sql_file,connection, print_sql=False)

        runner.process()
        # TODO setup data and test results


if __name__ == "__main__":
    unittest.main()
