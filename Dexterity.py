#!/usr/bin/python
import csv
import json
import logging
import sys

from pdsutil.DbUtil import CursorHelper

logging.basicConfig(level=logging.INFO)

#TODO migrate all of this to DbUtil

class Dexterity:
    def __init__(self):

        self.logger = logging.getLogger(__name__)

    outstream = None

    def get_type(obj):  # TODO more general purpose than this
        return str(type(obj)).split("'")[1]

    def emit_line(self, text):  # TODO add support for files
        sys.stdout.write(text)
        sys.stdout.write("\n")

    def emit(self, text):  # TODO add support for files
        sys.stdout.write(str(text))

    def format_sql_binds(conn, sql, binds):
        # TODO put in a package
        buff = sql + "\n" + json.dumps(binds)
        return buff

    def get_column_name_list(cursor):
        return [i[0] for i in cursor.description]

    def get_formatted(row, format_str):
        return format_str.format(row)

    # # def get_row_dictionary(cursor, row):
    # #    columns = get_column_name_list(cursor)
    # #    return (dict(zip(columns, row)))
    #
    # def run_select(self, conn, sql, binds):
    #     print ("about to run: " + sql)
    #     print ("binds are: " + str(binds))  # TODO format and alphabetize create a statement,
    #     cursor = conn.cursor()
    #     rows = cursor.execute(sql, binds)  # TODO exception process
    #     for row in rows:
    #         for col in row:
    #             self.emit(col)
    #             self.emit(" ")
    #         self.emit("\n")

    def sql_exception(self, sql, binds, exception, cursor=None):
        print ("sql: " + sql)
        print ("binds :" + str(binds))
        print ("Exception " + str(exception)) # TODO
        try:
            pretty_binds = self.get_pretty(binds)
        except Exception as e:
            print ("Dexterity 60 SQL EXCEPTION can't serialize binds %s \n Exception %s" %
                   (binds, e))  # TODO
            pretty_binds = str(binds)
        message = sql + "\n" + pretty_binds + "\n" + str(exception)
        if cursor is not None:
            message = message + "\n" + self.get_type(cursor)
        raise Exception(message)

    def get_pretty(self, obj):
        metajson = json.dumps(obj, indent=4)  # , sort_keys=True, cls=JsonEncoder)
        return metajson

    # def format_meta(self, meta):
    #     print ("in format_meta")
    #     retval = ""
    #     retval += "sql: " + meta["sql"] + "\n"
    #     retval += "columns:\n"
    #     for col in meta["columns"]:
    #         for attr in col:
    #             retval += " " + str(attr)
    #         retval += "\n"
    #     retval += str(meta["binds"])
    #     print ("format meta: " + retval)
    #     return retval

    # def get_meta(self, cursor, sql, binds=None, pretty=True):  # This is database specific TODO
    #     retval = ""
    #     retval += "sql:\n" + sql
    #
    #     retval += "\ncolumns:\n"
    #     for col in cursor.description:
    #         retval += ((str(col)) + "\n")
    #     retval += str(binds)
    #
    #     return retval

    # def dump_to_csv(self, conn, sql, outfile, emit_headers=True, binds=None,
    #                 emit_meta=True):
    #
    #     # TODO add  metada
    #     cursor = CursorHelper(conn.cursor())  #
    #
    #     if binds is None:
    #         rows = cursor.execute(sql)
    #     else:
    #         rows = cursor.execute(sql, binds)
    #     # if emit_meta:
    #     #    print self.format_meta(self.get_meta(cursor, sql, pretty=False))
    #     with open(outfile, "wb") as csvfile:
    #         writer = csv.writer(csvfile, dialect="excel",
    #                             delimiter=',', quotechar='"',
    #                             quoting=csv.QUOTE_NONNUMERIC)
    #         if emit_headers:
    #             headers = [i[0] for i in cursor.description]  # TODO use get_column_names
    #             writer.writerow(headers)
    #         for row in rows:
    #             writer.writerow(row)
    #     cursor.close()

    # def write_to_csv(self, conn, sql, file, emit_headers=True, binds=None, emit_meta=True):
    #     logging.info("sql: %s\n" % sql)
    #     cursor = CursorHelper(conn.cursor())
    #     rows = cursor.execute(sql, binds)
    #     logging.debug("executed")
    #     # logging.debug( self.get_meta(cursor, sql, pretty=False)) # TODO !! this is wrong unless it is oracle put in CursorHelper
    #     with open(file, "wb") as csvfile:
    #         writer = csv.writer(csvfile, dialect="excel",
    #                             delimiter=',', quotechar='"',
    #                             quoting=csv.QUOTE_NONNUMERIC)
    #         if emit_headers:
    #             headers = [i[0] for i in cursor.description]  # TODO should get from c
    #             writer.writerow(headers)
    #         for row in rows:
    #             writer.writerow(row)
    #
    #             # def dump_table_to_csv(self, conn, table_name, file, emit_headers=True,
    #             #                       emit_meta=True):
    #             #     sql = "select * from " + table_name
    #             #     write_to_csv(self, conn, sql, file, emit_headers=emit_headers, binds=None, emit_meta=emit_meta)


class StatementRunner():
    logger = logging.getLogger("StatementRunner")

    # TODO document
    def __init__(self, connection, statements, continue_on_error=False, show_statements=False):
        # types  statements list of StatementHelper
        self.connection = connection
        self.statements = statements
        self.continue_on_error = continue_on_error
        self.cursor = CursorHelper(connection.cursor())
        # logging.info("continue_on_error %s" % (continue_on_error))
        # logging.info("number of statements: %s" % (len(self.statements)))

    def process(self, binds):
        for stmt in self.statements:
            logging.info(stmt.description)
            if self.continue_on_error:
                try:
                    self.cursor.execute(stmt, binds)
                    self.connection.commit()
                except Exception as e:
                    self.connection.rollback()
                    print (stmt + str(e))
                    raise e
            else:
                self.cursor.execute(stmt.sql, binds)  # stmt is StatementHelper
            self.connection.commit()  # TODO is this a good idea?


class StatementHelper:
    def __init__(self, sql, description):
        self.sql = sql
        self.description = description
