# !/usr/bin/python
import logging
import os
import time
import json
from pdsutil.JsonEncoder import JsonEncoder
import decimal
import re
import sqlite3
import datetime
import psycopg2
import cx_Oracle

from typing import Dict
from typing import NamedTuple
from pprint import PrettyPrinter

import pdsutil.dialects as dialects

from typing import Dict, Tuple, List, Set
from typing import Set

logger = logging.getLogger(__name__)

pretty = PrettyPrinter()

# TODO http://initd.org/psycopg/docs/extras.html#fast-exec

"""
    DbUtil
    ~~~
    This module contains classes:
       ConnectionHelper
       CursorHelper
       Cursors
       DdlColumn
       DdlUtil

    :copyright: (c) 2017 by Jim Schmidt
"""

from collections import namedtuple

SqlStatement = namedtuple("SqlStatement", ["sql_text", "returning_text", "name", "description", "narrative"])

def underscore_to_camel_case(string):
    value = string.encode('utf8')

    def camelcase():
        yield str.lower
        while True:
            yield str.capitalize

    c = camelcase()
    return "".join(c.next()(x) if x else '_' for x in value.split("_"))



def getJavaType(column):
    dataType = column['DATA_TYPE']

    if (dataType == "VARCHAR" or
                dataType == "CHAR" or
                dataType == "LONG" or
                dataType == "VARCHAR2"):
        retval = "String"
    elif dataType == "NUMBER":
        precision = column['DATA_PRECISION']
        scale = column['DATA_SCALE']
        if scale == 0:
            if precision < 10:
                retval = "Integer"
            elif precision < 19:
                retval = "Long"
            else:
                retval = "BigDecimal"
        else:
            retval = "BigDecimal"
    elif dataType == "DATE":
        retval = "Date"
    elif dataType == "TIMESTAMP":
        retval = "Timestamp"
    else:
        raise Exception("unhandled type " + dataType)
    return retval

def sql_statement_to_named_tuple(sql_text: str, returning_text: str = None, name: str = None, description: str = None,
                                 narrative: str = None):
    retval = SqlStatement._make([sql_text, returning_text, description, narrative])
    return retval


def find_binds(sql: str) -> Set[str]:
    retval = set()
    regex = "%\(([^)]*)\)s"
    matches = re.findall(regex, sql)

    for match in matches:
        retval.add(match)
    return retval


def to_colon_binds(sql: str) -> str:
    """
    Replaces %(BIND_NAME)s with :BIND_NAME
    :param sql:
    :return: The sql with all of the binds in :bind format
    """
    logger = logging.getLogger(__name__ + ":to_colon_binds")
    binds = find_binds(sql)
    new_sql = sql
    for bind in binds:
        # print(bind)
        python_bind = str("%(" + bind + ")s")
        colon_bind = str(":" + bind)
        new_sql = new_sql.replace(python_bind,
                                  colon_bind)  # .replace(pattern,repl)re.sub(pattern, repl, new_sql)
        logger.debug(new_sql)
    return new_sql


def underscore_to_camel_case(string):
    prev = None
    buff = []
    for x in string:
        if not x == "_":
            if prev == "_":
                val = x.upper()
            else:
                val = x.lower()
            buff.append(val)
        prev = x
    return "".join(buff)


class ConnectionHelper:
    """
    Helps get connections based on various urls used by different RDBMs and
    supports the return of a connection based on a name
    imports the required database connection package
    """
    conn = None  #

    def __init__(self, file_name=None):
        """

        :param file_name: The name of yaml file formatted as list of dict::

        *connection_name*:
                url: *url_string*
                sql:
                    - "sql statement"
                    - "sql statement"

        *connection_name* A unique identifier for this database and user
        **url**           Literal starts with "sqlite3", "postgres", or "oracle" defines the dialect and RDBMS
                          **:* separator
                          The remainder is database specific
                          for a sqlite database it is the path of the database file
                          for a postgres database it is of the form

        Example::

            "sr":
                    url: "oracle:sales_reporting/xxxxxxx@localhost:1521:ASUSFED25"
            "sales":
                    url: "oracle:sr/xxxxxxxx@localhost:1521:ASUSFED25"
            "sqlite":
                    url: "sqlite3:/tmp/etl.db"
            "sales_reporting":
                    url: "sqlite3:/tmp/etl.db"
            "sales_reporting_db":
                    url: "postgres:host='localhost' dbname='sales_reporting_db' user='jjs' password='xxxxxx'"
            "test":
                    url: "postgres:host='localhost' dbname='sales_reporting_db' user='jjs' password='xxxx'"
                    sql:
                        -  "set schema 'sales_reporting'"
            "current":
                    url: "postgres:host='localhost' dbname='sales_reporting_db' user='jjs' password='xxxx'"
            "sqlite3_mem":
                    url: "sqlite3::memory:"
            """
        self.connections = self.get_connections_yaml(file_name)

    # def get_oracle_connection(self, url):
    #     import cx_Oracle  # TODO pip cx_Oracle
    #
    #     if url is None:
    #         raise Exception("no url specified")
    #
    #     conn_str_components = url.split(":")
    #     if len(conn_str_components) == 1:
    #         self.conn = cx_Oracle.connect(url)
    #     elif len(conn_str_components) == 3:  # format username/password@host:port:sid
    #         username_password, dsn_string = url.split("@")
    #         username, pw = username_password.split("/")
    #         host, port, sid = dsn_string.split(":")
    #         dsn_str = cx_Oracle.makedsn(host, port, sid)
    #         self.conn = cx_Oracle.connect(user=username, password=pw, dsn=dsn_str)
    #     else:
    #         raise Exception("""ORACLE_CONNECTION must be of the form
    #                             username/password@SID  or
    #                             username/password@hostname:port:SID
    #                             in the case of TNS configuration problems the second form my solve your problem""")
    #    return self.conn


    @staticmethod
    def get_dialect(cursor_or_connection):
        if isinstance(cursor_or_connection, sqlite3.Cursor):
            retval = dialects.DIALECT_SQLITE
        elif isinstance(cursor_or_connection, sqlite3.Connection):
            retval = dialects.DIALECT_SQLITE
        elif isinstance(cursor_or_connection, psycopg2.extensions.cursor):
            retval = dialects.DIALECT_POSTGRES
        elif isinstance(cursor_or_connection, psycopg2.extensions.connection):
            retval = dialects.DIALECT_POSTGRES
        elif isinstance(cursor_or_connection, cx_Oracle.Cursor):
            retval = dialects.DIALECT_ORACLE
        else:
            raise Exception("unknown type %s" % type(cursor_or_connection))
        return retval

    @staticmethod
    # def get_connections_yaml(filename:str=None) -> dict[str:dict[str:str]]:  # python barfs
    def get_connections_yaml(filename: str = None):
        """

        :param filename: file with Yaml definitions if None then ~/connections.yaml

        :return:
        """
        sphinx_problems = """ # TODO
        connection_name:
            url: "connection_url:
            sql:
               - "select 'sql to execute prior to returning connection'"
               - "set schema 'schema_name'
        """
        import yaml
        logger.debug("filename %s %s" % (filename, str(type(filename))))
        my_name = filename
        if my_name is None:
            my_name = os.environ["HOME"] + "/connections.yaml"

        with open(my_name, 'r') as content_file:
            yaml_connections = content_file.read()
        connections_dictionary = yaml.load(yaml_connections)
        return connections_dictionary

    @staticmethod
    def get_sqlite3_connection(url: str, foreign_keys: bool = True):

        sconn = sqlite3.connect(url, detect_types=sqlite3.PARSE_DECLTYPES)
        # Reference http://cs.stanford.edu/people/widom/cs145/sqlite/SQLiteLoad.html
        if foreign_keys:
            sconn.cursor().execute("PRAGMA foreign_keys = on")  # must be enabled with every session
        return sconn

    def get_oracle_connection(self, url):
        import cx_Oracle  # TODO pip cx_Oracle

        if url is None:
            raise Exception("no url specified")

        conn_str_components = url.split(":")
        if len(conn_str_components) == 1:

            self.conn = cx_Oracle.connect(url)
        elif len(conn_str_components) == 3:  # format username/password@host:port:sid
            username_password, dsn_string = url.split("@")
            username, pw = username_password.split("/")
            host, port, sid = dsn_string.split(":")
            dsn_str = cx_Oracle.makedsn(host, port, sid)
            self.conn = cx_Oracle.connect(user=username, password=pw, dsn=dsn_str)
        else:
            raise Exception("""ORACLE_CONNECTION must be of the form
                                   username/password@SID  or
                                   username/password@hostname:port:SID
                                   in the case of TNS configuration problems the second form my solve your problem""")
        return self.conn

    @staticmethod
    def get_components(conn_url: str) -> (str, str):
        """

        :param conn_url:
        :return: (dialect: str, dbms_url:str)
        """
        engine = conn_url.split(":")[0]
        url = conn_url[len(engine) + 1:]
        return engine, url

    @staticmethod
    def get_connection(conn_url: str):
        logger = logging.getLogger(__name__ + ":get_connection")
        engine, url = ConnectionHelper.get_components(conn_url)
        logger.debug("engine '%s' dburl '%s'" % (engine, url))
        # if engine == 'oracle':
        #     retval = ConnectionHelper.get_oracle_connection(url)
        # elif

        if engine == dialects.DIALECT_SQLITE:
            retval = ConnectionHelper.get_sqlite3_connection(url)
        elif engine == dialects.DIALECT_POSTGRES:
            retval = psycopg2.connect(url)
        elif engine == dialects.DIALECT_ORACLE:
            import cx_Oracle
            retval = ConnectionHelper.get_oracle_connection()
        else:
            raise Exception(
                "unsupported engine, must be one of %s " % dialects.dialects)
            # test connection
        cur = retval.cursor()
        cur.close()

        return retval

    @staticmethod
    def get_environment_connection():
        connect_url = os.environ["CONNECTION_URL"]
        return ConnectionHelper.get_connection(connect_url)

    def test_all(self):
        for k, url in self.connections.iteritems():
            try:
                not ConnectionHelper.get_connection(url)
            except Exception as e:
                raise Exception("unable to connect to " + url + str(e))

    def get_named_connection(self, name: str):
        """
        :param name: the name of the connection
        :return: a database connection
        """
        logger.info("connecting to %s" % name)
        if name not in self.connections:
            raise Exception("connection not found '" + name)
        start_time = time.time()
        connection_dict = self.connections[name]
        connection = ConnectionHelper.get_connection(connection_dict["url"])
        if "sql" in connection_dict:  # TODO make LITERAl
            cursor = connection.cursor()
            for sql in connection_dict["sql"]:
                logger.debug("Executing %s" % sql)
                cursor.execute(sql)
            cursor.close()
        elapsed_time = time.time() - start_time
        ela_msg = "It took %s seconds to get a connection for %s"
        if elapsed_time > .05:
            logger.warning(ela_msg % (elapsed_time, name))
        return connection


class CursorHelper:
    """
    Helper for a cursor:
        on execute exception rethrows exception with sql text, bind variables and the sql exception
    """

    def __init__(self, cursor, meta=None):  # TODO create a type for meta

        self.cursor = cursor
        if meta is not None:
            self.meta = meta
        self.logger = logging.getLogger("CursorHelper")
        # self.logger.setLevel(logging.DEBUG)
        self.description = None
        self.rows = None
        self.column_names = None
        self.dialect = ConnectionHelper.get_dialect(cursor)
        logger.debug("dialect %s" % self.dialect)

    meta = {}

    def execute_with_named_binds(self, sql: str, binds: Dict[str, object]):
        if binds is not None and sql is not None:
            bind_sql = sql % binds  # convert ${ID}s to :ID
        else:
            raise Exception("sql %s or binds %s is None" % (sql, binds))
        return self.execute(bind_sql, binds)

    def execute(self, sql, binds: Dict[str, object] = None, show_sql=False,
                statement_descr="No description provided",
                returning=None,
                verbose=False
                ):
        """
        :param sql: str - sql text
        :param binds: dictionary -  bind name and value
        :param show_sql: - boolean log the sql
        :param statement_descr:
        :param returning - String "returning column_name" if postgres, if sqlite not None will return the primary key inserted

        :return:

        The dialect is inferred from type(cursor)

        When inserting into a table and the inserted primary key is desired,
        pass "returning table_name_id" where table_name_id is the column to be returned.

        Bind variables are   from %(BIND_NAME)s to :BIND_NAME if present in
        the SQL and the dialect is not *dialacts.DIALECT_POSTGRES*

        """

        logger = logging.getLogger('CursorHelper')
        logger.debug("verbose is %s" % verbose)
        if verbose:
            logger.info("dialect %s" % self.dialect)

        logger.debug("dialect is %s =======================" % self.dialect)
        logger.debug("execute:\nsql:\n%s\n binds:\n%s\n returning: %s" % (sql, binds, returning))
        if not self.dialect == dialects.DIALECT_POSTGRES:
            processed_sql = to_colon_binds(sql)
        else:
            processed_sql = sql

        if returning:
            logger.debug("test returning: %s dialect is '%s' dialect_postgres is '%s'" %
                         (returning, self.dialect, dialects.DIALECT_POSTGRES))
            if self.dialect == dialects.DIALECT_POSTGRES:
                processed_sql += returning

        logger.debug("processed_sql\n %s" % processed_sql)

        start_time = time.time()
        pretty_binds = json.dumps(binds, indent=4, sort_keys=True, cls=JsonEncoder)
        # print("processed sql\n%s\nbinds %s\n" % (processed_sql, pretty_binds))

        if self.dialect == dialects.DIALECT_SQLITE:
            if binds is not None:
                for k, v in binds.items():
                    if v is not None and isinstance(v, decimal.Decimal):
                        binds[k] = float(v)

        # if logger.isEnabledFor(logging.DEBUG):
        #     logger.exception("----- " + processed_sql)
        try:
            if sql is None:
                if binds is None:
                    logger.debug("No sql, no binds")
                    result = self.cursor.execute()
                else:
                    logger.debug("no sql, binds")
                    result = self.cursor.execute(None, binds)
            else:
                if binds is None:
                    logger.debug("sql, no binds")
                    result = self.cursor.execute(processed_sql)
                else:
                    logger.debug("==== executing:\n%s\nwith binds\n%s" % (processed_sql, pretty_binds))
                    result = self.cursor.execute(processed_sql, binds)
        except Exception as e:
            bind_str = ""
            for k, v in binds.items():
                bind_str += ("name: %s value: %s type: %s\n" % (k, v, str(type(v))))
            message = "While processing sql:\n %s with binds:\n%s\n%s" % (processed_sql, bind_str, e)
            logger.exception(message)
            raise Exception(message)

        logger.debug("result is %s" % result)
        # if result is None:
        #     rows = self.cursor  # postgress driver returns rows in curs
        # else:
        #     rows = result
        logging.debug("just before test for returning")
        if returning:

            logger.debug("is returning %s" % returning)
            if self.dialect == dialects.DIALECT_POSTGRES:
                for row in self.cursor:
                    retval = row[0]
                logging.debug("retval from row %s" % retval)
            elif self.dialect == dialects.DIALECT_SQLITE:
                retval = self.cursor.lastrowid
                logging.debug("retval last rowid %s " % retval)
            else:
                raise Exception("logic error should never get here")
        else:

            logger.debug("is not returning")
            if result is None:
                retval = self.cursor  # postgress driver returns rows in curs
            else:
                logger.debug("returning ")
                retval = result
        logger.debug("returning '%s' dialect %s retval %s" % (returning, self.dialect, retval))
        self.description = self.cursor.description
        self.rows = result

        if self.cursor.description is not None:
            self.column_names = [i[0] for i in self.cursor.description]
        elapsed_time = time.time() - start_time
        if logger.level >= logging.DEBUG:
            logger.debug("elapsed %s %s" % (elapsed_time, statement_descr))
        logger.debug("retval is %s " % retval)
        return retval

    def next(self):
        row = self.rows.next()
        return dict(zip(self.column_names, row))

    def get_dict(self, row):
        return dict(zip(self.column_names, row))

    def close(self):
        self.cursor.close()

    @staticmethod
    def get_row_dictionary(cursor_description, row):
        column_names = [i[0] for i in cursor_description]
        return dict(zip(column_names, row))

    @staticmethod
    def dump_binds(sql, binds):
        """
        :param sql: sql statement
        :param binds: dictionary of bind variables
        :return: None - prints result
        """
        retval = ("sql: %s\n binds: %s" % (sql, json.dumps(binds, indent=4, sort_keys=True, cls=JsonEncoder)))
        return retval

    def fetchall(self):
        return self.cursor.fetchall()



class Cursors:
    def __init__(self, connection):
        """
        A pool of cursors identified by sql.
        Each is either created or returned.

        """
        self.connection = connection
        self.cursors = {}  # k - cursor name, cursors

    def get_cursor(self, name):
        logger.debug("name %s cursors %s" % (name, self.cursors))
        if name is None:
            raise Exception("name is required")
        if name in self.cursors:
            return self.cursors[name]

        cursor = CursorHelper(self.connection.cursor())
        self.cursors[name] = cursor
        return cursor

    def close(self):
        """
        Close all the cursors
        :return:
        """
        for name, cur in self.cursors.items():
            try:
                cur.close()
            except Exception as e:
                logging.error(e)
            logging.debug("cursor %s closed" % name)


class DatabaseColumns:
    def __init__(self):
        self.by_column_name = {}  # Dict[str,DdlColumn]
        self.by_index = {}  # Dict[int,DdlColumn]

    def get_by_column_name_dict(self) -> Dict[str, object]:
        return self.by_column_name

    def get_by_index_dict(self) -> Dict[int, object]:
        return self.get_by_index_dict()

    @staticmethod
    def get_from_psycopg2_cursor_description(cursor):
        retval = DatabaseColumns()

        pg_types_dict = {}
        oid_cursor = cursor.connection.cursor()
        logger.debug("cursor %s description %s" % (cursor, cursor.description))
        for col in cursor.description:
            oid = col[1]
            if oid not in pg_types_dict:
                pg_types_dict[oid] = DatabaseColumns.get_pg_type_with_bind(oid_cursor, oid)
        col_index = 0
        for col in cursor.description:
            column_name = col[0]
            precision = col[4]
            scale = col[5]
            pg_type = pg_types_dict[col[1]]

            pg_type_tuple = DatabaseColumns.get_pg_type_tuple(pg_type)
            logger.debug("pg_type_tuple: %s" % str(pg_type_tuple))
            db_data_type = pg_type[0]
            type_name = pg_type.typname
            logger.debug("pg_type.typlen %s" % pg_type.typlen)
            if pg_type.typname == 'varchar':
                string_length = col.internal_size
            else:
                string_length = None
            not_null = pg_type.typnotnull  # Always false!!! Dammit

            col_info = DatabaseColumn.get_named_tuple(column_name, db_data_type, column_index=col_index,
                                                      string_length=string_length, number_precision=precision,
                                                      number_scale=scale, not_null=not_null)
            retval.by_column_name[column_name] = col_info
            retval.by_index[col_index] = col_info
            col_index += 1
        return retval

    @staticmethod
    def get_pg_type_tuple(attributes: List[object]) -> NamedTuple:
        from collections import namedtuple

        retval = namedtuple('pg_type',
                            [
                                'typname',
                                'typnamespace',
                                'typowner',
                                'typlen',
                                'typbyval',
                                'typtype',
                                'typcategory',
                                'typispreferred',
                                'typisdefined',
                                'typdelim',
                                'typrelid',
                                'typelem',
                                'typarray',
                                'typinput',
                                'typoutput',
                                'typreceive',
                                'typsend',
                                'typmodin',
                                'typmodout',
                                'typanalyze',
                                'typalign',
                                'typstorage',
                                'typnotnull',
                                'typbasetype',
                                'typtypmod',
                                'typndims',
                                'typcollation',
                                'typdefaultbin',
                                'typdefault',
                                'typacl'
                            ])._make(attributes)

        return retval

    @staticmethod
    def get_pg_type_with_bind(oid_cursor, oid: int):
        oid_select = "select * from pg_catalog.pg_type where oid = %(OID)s"
        oid_cursor.execute(oid_select, {"OID": oid})
        retval = None
        for row in oid_cursor:
            retval = DatabaseColumns.get_pg_type_tuple(row)
            # logging.debug("oid is: " + str(retval))
        if retval is None:
            raise Exception("oid %s not found " % oid)
        return retval

    @staticmethod
    def get_from_cursor_description(cursor):
        if isinstance(cursor, psycopg2.extensions.cursor):
            return DatabaseColumns.get_from_psycopg2_cursor_description(cursor)
        else:
            raise Exception("unsupported ")


class DatabaseColumn:
    @staticmethod
    def get_named_tuple(column_name: str, database_type: str, column_index: int = None,
                        string_length: int = None, number_precision: int = None,
                        number_scale: int = None, not_null: bool = None
                        ):
        from collections import namedtuple

        parens = ""
        if string_length is not None:
            if (number_precision is not None) and (number_precision > 0):
                raise Exception("number_precision is %s and string_length is %s" % (number_precision, string_length))
            if (number_scale is not None) and (not number_scale == 0):
                raise Exception("number_scale is %s and string_length is %s" % (number_scale, string_length))
            parens = "(%s)" % string_length
        else:
            if number_precision is not None:
                if number_scale is not None:
                    parens = "(%s,%s)" % (number_precision, number_scale)
                else:
                    parens = "(%s)" % number_precision

        column_type = database_type + parens

        db_column = namedtuple("database_column",
                               ['column_name', 'database_type', "column_type", 'column_index', 'string_length',
                                'number_precision', 'number_scale', 'not_null'])
        retval = db_column(column_name=column_name, database_type=database_type, column_type=column_type,
                           string_length=string_length, column_index=column_index,
                           number_precision=number_precision, number_scale=number_scale,
                           not_null=not_null)
        return retval


class DdlColumn:
    def __init__(self, column_name, python_type, length=None, precision=None, scale=None, not_null=False):
        self.column_name = column_name
        self.python_type = python_type
        self.length = length
        self.precision = precision
        self.scale = scale
        self.not_null = not_null
        self.display_width = None
        self.max_data_width = None

    def __str__(self):
        return str(self.__dict__)

    def get_postgres_type(self):
        if self.python_type == decimal.Decimal:
            if self.precision:
                if self.scale:
                    retval = "numeric(%s,%s)" % (self.precision, self.scale)
                else:
                    retval = "numeric(%s)" % self.precision
            else:
                retval = "numeric"
        elif self.python_type == int:
            retval = "integer"
        elif self.python_type == str:
            if self.length:
                retval = "varchar(%s)" % self.length
            else:
                retval = "text"
        elif self.python_type == datetime.date:
            retval = "datetime"
        else:
            raise Exception("unsupported type %s" % self.python_type)
        return retval

    def get_sqlite_type(self):
        if self.python_type == decimal.Decimal:
            if self.precision:
                if self.scale:
                    retval = "numeric(%s,%s)" % (self.precision, self.scale)
                else:
                    retval = "numeric(%s)" % self.precision
            else:
                retval = "numeric"
        elif self.python_type == int:
            retval = "integer"
        elif self.python_type == str:
            if self.length:
                retval = "varchar(%s)" % self.length
            else:
                retval = "text"
        elif self.python_type == datetime.date:
            retval = "datetime"
        else:
            raise Exception("unsupported type %s" % self.python_type)
        return retval

    def get_column_type(self, dialect):
        if dialect == dialects.DIALECT_POSTGRES:
            return self.get_postgres_type()
        else:
            raise Exception("Unsupported dialect %s" % dialect)


class DdlUtil:
    def __init__(self, dialect):
        self.dialect = dialect

    def get_create_table(self, table_name, column_defs):
        """

        :param table_name:
        :param column_defs: should be OrderedDict
        :return:
        """
        logger = logging.getLogger(__name__ + ":get_create_table")

        col_def = None
        try:
            sql = "create table %s (\n" % table_name
            comma = ""
            for column_def in column_defs:
                logger.debug("column %s" % column_def)
                col_def = column_def
                sql += comma
                col_type = column_def.get_column_type(self.dialect)
                sql += column_def.column_name.ljust(30) + " " + col_type
                if column_def.not_null:
                    sql += " not null"

                comma = ",\n"
            sql += "\n)"
        except Exception as e:
            msg = str(col_def)
            # for cd in column_defs:
            #     msg += "%s\n" % cd
            raise Exception("%s\n%s" % (e, msg))

        return sql

    def get_insert(self, table_name, column_defs):
        """

        :param table_name:
        :param column_defs:
        :return:
        """
        sql = "insert into %s (\n" % table_name
        comma = ""
        for column_def in column_defs:
            sql += comma
            sql += "   %s" % (column_def.column_name)
            comma = ",\n"
        sql += "\n) values (\n"
        comma = ""
        for column_def in column_defs:
            sql += comma
            sql += "   :" + (column_def.column_name)
            # sql += "   %("  + (column_def.column_name) + ")s"
            comma = ",\n"
        sql += "\n)"
        return sql


class SqlStatements:
    statements = None  # Dict[str,namedtuple]

    def __repr__(self):
        import yaml
        return yaml.dump(self.statements)
        # buffer = ""
        # index = 1
        # for k, v in self.statements.items():
        #     buffer += "%s name: '%s\n" % (index, k)
        #     index += 1

    @staticmethod
    def from_statement_list(statement_list):
        retval = SqlStatements()
        retval.statements = {}
        for statement in statement_list:
            retval.statements[statement.name] = statement
        return retval

    @staticmethod
    def from_statement_dictionary(statement_dictionary: Dict[str, Dict[str, str]]):
        retval = SqlStatements()
        retval.statements = {}
        for k, v in statement_dictionary.items():
            sql_text = v.get("sql_text")
            returning_text = v.get("returning_text", None)
            name = v.get("name", None)
            description = v.get("description", None)
            narrative = v.get("narrative", None)
            stmt = sql_statement_to_named_tuple(sql_text, returning_text=returning_text,
                                                name=name, description=description,
                                                narrative=narrative)
            retval.statements[k] = stmt
        return retval

    @staticmethod
    def from_yaml(file_name: str):
        import yaml
        with open(file_name, 'r') as yaml_file:
            yaml_statements = yaml_file.read()
        retval = SqlStatements()
        retval.statements = yaml.load(yaml_statements)
        return retval

    def enumerate_statements(self):
        buffer = ""
        index = 1
        for k, v in self.statements.items():
            buffer += "%s name: '%s\n" % (index, k)
            index += 1
