from pdsutil.DbUtil import SqlStatements, CursorHelper
import pdsutil.DbUtil as DbUtil
import pdsutil.tests.get_test_postgres_connection as test_connnection

import os
import unittest
import logging

import pdsutil.dialects as dialects
from pdsutil.tests.DatabaseInitializationForIT import ConditionTableInitializationIT as ut_condition_init
from pdsutil.tests.DatabaseInitializationForIT import SalesReportingDataInit as sr_data_init
from pdsutil.tests.DatabaseInitializationForIT import SchemaInitter

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_file_path(file_name):
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)


class SqlStatementsTest(unittest.TestCase):
    POSTGRES_TEST_URL = "POSTGRES_TEST_URL"
    TEST_SCHEMA = "integration_test"
    dialect = dialects.DIALECT_SQLITE  # TODO

    def setUp(self):
        None
        self.connection = test_connnection.get_test_postgres_connection()
        # SchemaInitter(self.connection,self.TEST_SCHEMA,verbose=False).process()
        # ut_condition_init(self.connection,verbose=False).process()
        # sr_data_init(self.connection).process()

        yaml_file_name = get_file_path("resources/etl_persistence_sql.yaml")
        self.statements = SqlStatements.from_yaml(yaml_file_name).statements
        # self.show_statements()

    # def show_statements(self):
    #     print (self.statements)
    #     for name, stmt in self.statements.items():
    #         print ("name: '%s'\n" % name)
    #         print ("sql_text:\n%s" % stmt["sql"])
    #         print ("binds %s\n\n" % StaticMethods.find_binds(stmt["sql"]))



    def test_one(self):
        """
        This test ensures that a returning clause returns # TODO test under sqlite
        :return: 
        """
        cursor = CursorHelper(self.connection.cursor())
        stmt = self.statements["etl_file_initial_insert"]
        # self.assertEquals(stmt["name"],"etl_file_initial_insert")
        sql = stmt["sql"]
        returning = stmt["returning"]
        self.assertEqual(stmt["returning"], "returning etl_file_id")
        binds = {"ORG_CD": "TX EXOTIC"}
        id = cursor.execute(sql, binds=binds, returning=returning, verbose=False)
        self.assertIsNotNone(id)

    def tearDown(self):
        None
        #logger.info("no teardown necessary") self.connection.execute("DROP DATABASE testdb")


if __name__ == '__main__':
    unittest.main()
