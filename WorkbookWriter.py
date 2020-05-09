import yaml
import logging

from pdsutil.SheetHelper import SheetHelper

from pdsutil.WorksheetColumnMeta import  WorksheetMeta

import datetime
import xlsxwriter
import argparse
# TODO Create default column headings just replace _ with space
# TODO document
# TODO multiple binds on command line
# TODO prompt for bind values
# TODO Macros
# TODO set log level on command line

# TODO do a better job with column widths index by name and report
# TODO log statistics, time to execute number of rows

# "sales_workbook":
# -  sheet_name: "sales"
#    sql: >
#           select *
#           from etl_cust_product_month_view
#           where sum_cases_shipped > 0
#    pivot:
#        columns:
#           - "ship_month"
#        values:
#           - 'sum_cases_shipped'
#        index:
#           - 'ship_to_cust_id'
#           - 'product_descr'

# workbook:
#     description: >
#        The sales, customers and inventory records for a given load file
#     binds:
#        ETL_FILE_ID:
#            data_type: integer
#            label: "etl_file_id"
# worksheets:
#     -
#         worksheet_name: "customers"
#         sql: >
#                select etl_customer_id, etl_file_id,
#                    ship_to_cust_id, cust_nm, addr_1,
#                    addr_2, city, state,
#                    postal_cd, cntry_id, tel_nbr,
#                    national_acct_id, special_flg
#                from etl_customer
#                where etl_file_id = %(ETL_FILE_ID)s
#         columns:
#            -
#               name: etl_customer_id
#               heading: "ETL Customer ID"


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

WORKBOOK = "workbook"
DESCRIPTION  = "description"
BINDS = "binds"
WORKSHEET_NAME = "worksheet_name"
SQL = "sql"
COLUMNS = "columns"
COLUMN_NAME = "name"
PIVOT = "pivot"
COLUMNS = "columns"
VALUES = "values"
INDEX = "index"

def load_yaml_file(file_name):
    """
    Reads the specified YAML file and returns the object representation
    #TODO put in util
    :param file_name:
    :return: The object representation of the YAML file
    """
    start_time = datetime.datetime.now()
    with open(file_name, 'r') as workbook_def_file:
        yaml_text = workbook_def_file.read()
    retval = yaml.load(yaml_text)
    end_time = datetime.datetime.now()
    elapsed = end_time - start_time
    logger.info("parse time for yaml %s" % elapsed)
    return retval

class WorkbookWriter:


    def __init__(self, yaml_file):
        """
        Initialize with the spreadsheet definition file.
        :param yaml_file:
        """

    @staticmethod
    def from_dictionary(workbook_def):
        retval = WorkbookWriter
        retval.workbook_def = workbook_def
        if "workbook" not in workbook_def:
            raise Exception("workbook not in definition")
        else:
            retval.workbook  = workbook_def["workbook"]

        if "binds" not in workbook_def:
            raise Exception("binds not in workbook info")
        else:
            retval.binds = workbook_def["binds"]

        if "worksheets" not in workbook_def:
            raise Exception("No worksheets")
        else:
            retval.worksheets = workbook_def["worksheets"]

    @staticmethod
    def from_yaml(file_name):


    @staticmethod
    def get_sheet_helper(worksheet):
        """
        Validates that everything a SheetHelper needs is present, instantiates and returns
        :return: pdsutil.SheetHelper
        """
        errors = False

        if WORKSHEET_NAME not in worksheet:
            logger.error("worksheet has no %s" % WORKSHEET_NAME)
        if SQL not in worksheet:
            logger.error("worksheet has no %s" % SQL)

        worksheet_name = worksheet[WORKSHEET_NAME]
        sql = worksheet[SQL]
        # if the columns are not specified we will default to all the columns
        # in the query in the query order
        if COLUMNS in worksheet:
            columns_meta = worksheet[COLUMNS]
            sheet_column_names = []
            worksheet_column_meta = WorksheetMeta()
            for meta in columns_meta:
                worksheet_column_meta.add_column_dict(meta)
                sheet_column_names.append(meta[COLUMN_NAME])
        else:
            sheet_column_names = None
            worksheet_column_meta = None
        sh = SheetHelper(worksheet_name, sql, sheet_column_names, worksheet_column_meta)
        return sh

    def check_binds(self, binds):

        for k, v in self.binds.items():
            if k not in binds:
                raise Exception("required bind variable '%s' not supplied" % k)


    def process_yaml(self, connection, outfile, binds):
        """
               :param connection: database connection
               :param outfile: an open file to write to might be a stream
               :param binds: dictionary of bind names and values
               :return:  None
        """
        self.check_binds(binds)
        workbook = xlsxwriter.Workbook(outfile)
        for worksheet in self.worksheets:
            worksheet_name, sql, sheet_column_names, worksheet_column_meta = self.validate_worksheet(worksheet)
            sh = SheetHelper(worksheet_name,sql,sheet_column_names,worksheet_column_meta)
            sh.render_cursor(workbook, connection, binds)
        workbook.close()


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser()
    PARSER.add_argument("--connection_name", required=False, default="test",
                        help="name of database connection")


