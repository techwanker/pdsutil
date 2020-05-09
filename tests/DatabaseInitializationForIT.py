import unittest
import os
import logging
from pdsutil.DbUtil import ConnectionHelper, CursorHelper
from pdsutil.SqlRunner import SqlRunner

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)


def get_file_path(file_name):
    # TODO put this in one place
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)


def sql_runner(connection, relative_path: str, print_sql: bool = False) -> None:
    ddl_file_name = get_file_path(relative_path)
    logger.info("creating tables from %s" % ddl_file_name)
    runner = SqlRunner(ddl_file_name, connection, print_sql=print_sql, commit=False)
    runner.process()


class SchemaInitter:
    """
    Database initialization for Integration Testing
    """

    def __init__(self, connection, test_schema: str, verbose: bool = False):
        """ 
        
        """
        self.connection = connection
        self.test_schema = test_schema

    def create_schema(self) -> None:

        if self.test_schema:
            cursor = CursorHelper(self.connection.cursor())
            try:
                cursor.execute("create schema %s" % self.test_schema)
            except Exception as e:
                logger.warning(e)
                self.connection.rollback()
                logger.warning("create schema %s failed, connection rolled back " % self.test_schema)
            set_schema_sql = "set schema '%s'" % self.test_schema
            logger.info("about to %s" % set_schema_sql)
            cursor.execute(set_schema_sql)
            logger.info("should be new schema")
            cursor.close()

    def process(self) -> None:
        if self.test_schema:
            self.create_schema()


class ConditionTableInitializationIT:
    def __init__(self, connection, verbose: bool = True):
        self.connection = connection
        self.verbose = verbose

    def test_no_rows(self, sql) -> None:
        cursor = CursorHelper(self.connection.cursor())
        if self.verbose:
            logger.info("testing: %s" % sql)
        rows = cursor.execute(sql)
        for row in rows:
            assert row[0] == 0
        cursor.close()

    def test_condition_tables(self) -> None:
        self.test_no_rows("select count(*) from ut_condition_run")
        self.test_no_rows("select count(*) from ut_condition_run_parm")
        self.test_no_rows("select count(*) from ut_condition_run_step")
        self.test_no_rows("select count(*) from ut_condition_row_msg")
        self.test_no_rows("select count(*) from ut_condition")

    def process(self) -> None:
        sql_runner(self.connection, "../resources/ut_condition_tables.sr.sql")
        self.test_condition_tables()


class SalesReportingDataInit:
    def __init__(self, connection):
        self.connection = connection

    def populate_tables(self):
        None

    def process(self):
        sql_runner(self.connection, "resources/sales_reporting_ddl.sql")
