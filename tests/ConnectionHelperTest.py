#!/usr/bin/env python

import unittest
from pdsutil.DbUtil import ConnectionHelper, CursorHelper

import pdsutil.dialects as dialects
import os

import pdsutil.setup_logging as log_setup
import logging

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)

POSTGRES = "test"

def get_file_path(file_name):
    fdir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(fdir, file_name)

# The postgres connections are commented out as TODO integration testing with postgres not complete

relative_yaml_file_path = "resources/connections.yaml"
yaml_file_path = get_file_path(relative_yaml_file_path)

#print("yaml_file_path %s " % yaml_file_path)


class ConnectionHelperTest(unittest.TestCase):
    """
    Unit Test for ConnectionHelper
    """
    relative_yaml_file_path = "resources/connections.yaml"
    yaml_file_path = get_file_path(relative_yaml_file_path)

    def test_yaml_reader(self):
        connections = ConnectionHelper.get_connections_yaml(ConnectionHelperTest.yaml_file_path)
        self.assertEqual(len(connections), 2)
        self.assertTrue("sqlite3" in connections)
        self.assertTrue("sqlite3_mem" in connections)
        sql = connections["sqlite3_mem"]["sql"][0]
        self.assertEqual(sql, "PRAGMA foreign_keys = ON")

    def test_memory_sqlite(self):
        ch = ConnectionHelper(ConnectionHelperTest.yaml_file_path)
        connection = ch.get_named_connection("sqlite3")
        connection.cursor().execute("select 'x'")

        connection = ch.get_named_connection("sqlite3_mem")
        conn_info = ch.connections["sqlite3_mem"]
        dialect, dburl = ch.get_components(conn_info["url"])
        self.assertEqual(dialect, "sqlite")
        self.assertEqual(dburl, ":memory:")
        connection.cursor().execute("select 'x'")

    # def test_home_postgres(self):
    #     connections = ConnectionHelper(None)
    #     conn = connections.get_named_connection(POSTGRES)
    #     self.assertEqual(ConnectionHelper.get_dialect(conn), dialects.DIALECT_POSTGRES)
    #     cursor = conn.cursor()
    #     self.assertEqual(ConnectionHelper.get_dialect(conn), dialects.DIALECT_POSTGRES)

    def test_sqlite(self):
        ch = ConnectionHelper(ConnectionHelperTest.yaml_file_path)
        connection = ch.get_named_connection("sqlite3")
        self.assertEqual(ConnectionHelper.get_dialect(connection), dialects.DIALECT_SQLITE)
        cursor = connection.cursor()
        self.assertEqual(ConnectionHelper.get_dialect(cursor), dialects.DIALECT_SQLITE)

    # def test_postgress_returning(self):
    #     drop_sql = "drop table if exists a"
    #     create_sql = "create table a (b serial primary key, c numeric)"
    #     insert_sql = "insert into a (c) values (%(c)s)"
    #     returning = "returning b"
    #     connections = ConnectionHelper(None)
    #     connection = connections.get_named_connection(POSTGRES)
    #     cursor = CursorHelper(connection.cursor())
    #     cursor.execute(drop_sql)
    #     cursor.execute(create_sql)
    #     new_id = cursor.execute(insert_sql, {"c": 3}, returning="returning b")
    #     self.assertEqual(1, new_id)

    def test_sqlite_returning(self):
        logger = logging.getLogger(__name__ + ":test_sqlite_returning")
        drop_sql = "drop table if exists a"
        create_sql = "create table a (b serial primary key, c numeric)"
        insert_sql = "insert into a (c) values (%(c)s)"
        returning_text = "returning b"

        ch = ConnectionHelper(ConnectionHelperTest.yaml_file_path)

        connection = ch.get_named_connection("sqlite3_mem")
        cursor = CursorHelper(connection.cursor())
        cursor.execute(drop_sql)
        cursor.execute(create_sql)
        new_id = cursor.execute(insert_sql, {"c": 3}, returning=returning_text)
        self.assertEqual(1, new_id)


if __name__ == "__main__":
    unittest.main()
