import logging
import os
import unittest

import pdsutil.dialects as dialects
from pdsutil.DbUtil import ConnectionHelper
from pdsutil.tests.DatabaseInitializationForIT import ConditionTableInitializationIT as ut_condition_init
from pdsutil.tests.DatabaseInitializationForIT import SalesReportingDataInit as sr_data_init
from pdsutil.tests.DatabaseInitializationForIT import SchemaInitter
import pdsutil.tests.get_test_postgres_connection as test_connnection

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)

# TODO https://julien.danjou.info/blog/2014/db-integration-testing-strategies-python

def get_file_path(file_name):
    # TODO put this in one place
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)


class DbIntegrationTestBase(unittest.TestCase):
    POSTGRES_TEST_URL = "POSTGRES_TEST_URL"
    TEST_SCHEMA = "integration_test"
    dialect = dialects.DIALECT_SQLITE # TODO

    def setUp(self):
        self.connection = test_connnection.get_test_postgres_connection()
        SchemaInitter(self.connection,self.TEST_SCHEMA).process()
        ut_condition_init(self.connection).process()
        sr_data_init(self.connection).process()


    def tearDown(self):
        logger.info("no teardown necessary")
        # self.connection.execute("DROP DATABASE testdb")
