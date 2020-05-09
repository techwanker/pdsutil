import logging
import os
import unittest

import pdsutil.dialects as dialects
from pdsutil.DbUtil import ConnectionHelper
from pdsutil.tests.DatabaseInitializationForIT import ConditionTableInitializationIT as ut_condition_init
from pdsutil.tests.DatabaseInitializationForIT import SalesReportingDataInit as sr_data_init
from pdsutil.tests.DatabaseInitializationForIT import SchemaInitter

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)


# TODO https://julien.danjou.info/blog/2014/db-integration-testing-strategies-python

def get_file_path(file_name):
    # TODO put this in one place
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)


class TestDatabase(unittest.TestCase):
    POSTGRES_TEST_URL = "POSTGRES_TEST_URL"
    TEST_SCHEMA = "integration_test"
    dialect = dialects.DIALECT_SQLITE  # TODO

    def setUp(self):
        # db_url = os.getenv(self.POSTGRES_TEST_URL)
        if self.dialect == dialects.DIALECT_POSTGRES:
            db_url = "postgres:host='localhost' dbname='sales_reporting_db' user='jjs' password='pdssr'"
            # TODO externalize
            test_schema = self.TEST_SCHEMA
        elif self.dialect == dialects.DIALECT_SQLITE:
            db_url = "sqlite::memory:"
            test_schema = None
        else:
            raise Exception("unsupported dialect %s" % self.dialect)

        message = "Skipping test as %s environment variable not set" % self.POSTGRES_TEST_URL
        if not db_url:
            logging.warning(message)
            self.skipTest(message)

        self.connection = ConnectionHelper.get_connection(db_url)
        SchemaInitter(self.connection, test_schema).process()
        ut_condition_init(self.connection).process()
        sr_data_init(self.connection).process()

        # TODO check that tables exist

    def do_nothing_test(self):
        self.assertEqual(1, 1)

    def tearDown(self):
        logger.info("no teardown necessary")
        # self.connection.execute("DROP DATABASE testdb")
