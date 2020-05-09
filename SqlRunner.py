#!/usr/bin/env python
import argparse
import logging

from pdsutil.DbUtil import ConnectionHelper

logger = logging.getLogger(__name__)

class SqlRunner:
    """
    This will run a sql input file, running each statement

    The file should have
    **;--**
    between statements.  That functions as our delimiter and causes execution if run as
    as script in most databases

    Any statement can be executed but the result from selects are not retrieved.



    """
        # Example input file::
        # create table x (
        #     a serial primary key,
        #     b varchar(3),
        #     c numeric(9))
        # ;--
        # create table y (
        #     a integer references x,
        #     c a_date date
        # )
        # ;--

    # Note:
    #     *Oracle* will commit with every DDL statement
    #     *Postgres* DDL will not take place unless there is a commit
    # TODO should take a list of statements or an ordered dict

    def __init__(self, infile_name, conn, continue_on_error=False, print_sql=False,
                 commit=True,verbose=False):
        """
        :param infile_name: The name of the file containing the script to be run
        :param conn: a database connection
        :param continue_on_error: If False, terminates on error, else continues
        :param print_sql: If True, prints the sql statements
        :param commit: If True, commits at the end
                       If False, this is a dry run on Postgress where
                       ddl changes don't take effect until a commit
        """

        self.logger = logging.getLogger(__name__)
        self.infile = infile_name
        self.conn = conn
        self.continue_on_error = continue_on_error
        self.print_sql = print_sql
        self.commit = commit
        self.cursor = conn.cursor()
        self.verbose = verbose

    def execute(self, sql):
        """
        Execute a single statement # TODO should support binds
        #TODO this should use SqlHelper
        :param sql:

        :return: 0
        """

        if self.conn is None:
            print("")
            print(sql)
            print(";--")

        else:
            try:
                self.cursor.execute(sql)
                if self.print_sql:
                    logging.info("executed:\n%s" % sql)
            except Exception as oops:
                message = "While processing\n%s\n%s" % (sql, str(oops))
                if self.continue_on_error:
                    print("continuing on error")
                    self.logger.error(message)
                else:
                    print("Ending")
                    raise Exception(message)

    def process(self):
        """
        Execute each statement in turn.
        :return:
        """

        buff = ""
        with open(self.infile) as inf:
            for line in inf:
                if line.startswith(";--"):
                    self.execute(buff)
                    buff = ""
                else:
                    buff += line
        if self.commit:
            self.conn.commit()
            if self.verbose:
                logger.info("commit complete")
        else:
            logger.debug("Not committing")

    @staticmethod
    def main():
        """
        SqlRunner.py [-h] --connection_name CONNECTION_NAME
                          --infile_name INFILE_NAME
                          [--continue_on_error CONTINUE_ON_ERROR]
                          [--print_sql]
                          [--commit]

        optional arguments:
          -h, --help            show this help message and exit
          --connection_name CONNECTION_NAME
                                name of database connection
          --infile_name INFILE_NAME
                                name of sql script file
          --continue_on_error CONTINUE_ON_ERROR
                                continue running statements after an error
          --print_sql           print each executed SQL statement
          --commit              commit at end

        :return:
        """
        logging.basicConfig(level=logging.INFO)
        PARSER = argparse.ArgumentParser()
        PARSER.add_argument("--connection_name", required=True, default="test",
                            help="name of database connection")

        PARSER.add_argument("--infile_name", required=True,
                            help="name of sql script file")

        PARSER.add_argument("--continue_on_error",
                            help="continue running statements after an error")

        PARSER.add_argument("--print_sql", action="store_true",
                            help="print each executed SQL statement")

        PARSER.add_argument("--commit", action="store_true",
                            help="commit at end")

        PARSER.set_defaults(print_sql=False, continue_on_error=False)

        ARGS = PARSER.parse_args()
        CONNECTION = ConnectionHelper(None).get_named_connection(ARGS.connection_name)
        RUNNER = SqlRunner(infile_name=ARGS.infile_name,
                           conn=CONNECTION,
                           continue_on_error=ARGS.continue_on_error,
                           print_sql=ARGS.print_sql,
                           commit=True)
        RUNNER.process()


if __name__ == "__main__":
    SqlRunner.main()

