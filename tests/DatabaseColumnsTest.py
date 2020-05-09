import logging
import unittest

import pdsutil.setup_logging as log_setup
import pdsutil.tests.get_test_postgres_connection as postgres_conn
from pdsutil.DbUtil import DatabaseColumns

log_setup.setup_logging("../logging.yaml")
logger = logging.getLogger(__name__)


class DatabaseColumnsTest(unittest.TestCase):
    create_table_sql = """
            create table a (
               col_serial      serial primary key,
               col_integer     integer not null,
               col_numeric     numeric,
               col_numeric_9_0 numeric(9,0),
               col_varchar_20  varchar(20),
               col_text        text)

        """

    def setUp(self):  # TODO make
        self.connection = postgres_conn.get_test_postgres_connection()

    def test_from_postgres_cursor(self):
        """
        description

        http://initd.org/psycopg/docs/cursor.html

            This read-only attribute is a sequence of 7-item sequences.

            Each of these sequences is a named tuple (a regular tuple if collections.namedtuple() is not available)
            containing information describing one result column:

            name: the name of the column returned.
            type_code: the PostgreSQL OID of the column.
                 You can use the pg_type system table to get more informations about the type.
                 This is the value used by Psycopg to decide what Python type use to represent the value.
                 See also Type casting of SQL types into Python objects.
            display_size: the actual length of the column in bytes.
                   Obtaining this value is computationally intensive,
                   so it is always None unless the PSYCOPG_DISPLAY_SIZE parameter is set at compile time. 
                   See also PQgetlength.
            internal_size: the size in bytes of the column associated to this column on the server.
                 Set to a negative value for variable-size types See also PQfsize.
            precision: total number of significant digits in columns of type NUMERIC. None for other types.
            scale: count of decimal digits in the fractional part in columns of type NUMERIC. None for other types.
            null_ok: always None as not easy to retrieve from the libpq.


        :return:
        """
        connection = postgres_conn.get_test_postgres_connection()
        cursor = connection.cursor()
        cursor.execute(self.create_table_sql)
        cursor.execute("select * from a")
        for col_meta in cursor.description:
            logger.debug("col: %s" % str(col_meta))
        ddl_columns = DatabaseColumns.get_from_cursor_description(cursor)
        self.assertTrue(len(ddl_columns.by_index), 6)
        self.assertTrue(len(ddl_columns.by_column_name), 6)
        #
        column = ddl_columns.by_column_name["col_serial"]
        self.assertEqual(column.database_type, "int4")
        self.assertEqual(column.column_index, 0)
        self.assertEqual(column.string_length, None)
        self.assertEqual(column.number_precision, None)
        self.assertEqual(column.number_scale, None)
        self.assertEqual(column.not_null, False)  # TOOD it's always False!
        #
        column = ddl_columns.by_column_name["col_integer"]
        self.assertEqual(column.database_type, "int4")
        self.assertEqual(column.column_index, 1)
        self.assertEqual(column.string_length, None)
        self.assertEqual(column.number_precision, None)
        self.assertEqual(column.number_scale, None)
        self.assertEqual(column.not_null, False)  # TOOD it's always False!
        #
        column = ddl_columns.by_column_name["col_numeric"]
        self.assertEqual(column.database_type, "numeric")
        self.assertEqual(column.column_index, 2)
        self.assertEqual(column.string_length, None)
        self.assertEqual(column.number_precision, 65535)
        self.assertEqual(column.number_scale, 65535)
        self.assertEqual(column.not_null, False)  # TOOD it's always False!
        #
        column = ddl_columns.by_column_name["col_numeric_9_0"]
        self.assertEqual(column.database_type, "numeric")
        self.assertEqual(column.column_index, 3)
        self.assertEqual(column.string_length, None)
        self.assertEqual(column.number_precision, 9)
        self.assertEqual(column.number_scale, 0)
        self.assertEqual(column.not_null, False)  # TOOD it's always False!
        #
        column = ddl_columns.by_column_name["col_varchar_20"]
        self.assertEqual(column.database_type, "varchar")
        self.assertEqual(column.column_index, 4)
        self.assertEqual(column.string_length, 20)
        self.assertEqual(column.number_precision, None)
        self.assertEqual(column.number_scale, None)
        self.assertEqual(column.not_null, False)  # TOOD it's always False!
        #
        column = ddl_columns.by_column_name["col_text"]
        self.assertEqual(column.database_type, "text")
        self.assertEqual(column.column_index, 5)
        self.assertEqual(column.string_length, None)
        self.assertEqual(column.number_precision, None)
        self.assertEqual(column.number_scale, None)
        self.assertEqual(column.not_null, False)  # TOOD it's always False!


if __name__ == "__main__":
    unittest.main()
