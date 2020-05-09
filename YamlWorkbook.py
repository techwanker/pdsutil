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

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

WORKSHEET_NAME = "worksheet_name"
SQL = "sql"
COLUMNS = "columns"
COLUMN_NAME = "name"

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

class YamlWorkbook:


    def __init__(self, yaml_file):
        """
        Initialize with the spreadsheet definition file.
        :param yaml_file:
        """
        self.workbook_def = load_yaml_file(yaml_file)
        if "workbook" not in self.workbook_def:
            raise Exception("workbook not in definition")
        else:
            self.workbook_info = self.workbook_def["workbook"]

        if "binds" not in self.workbook_info:
            raise Exception("binds not in workbook info")
        else:
            self.binds = self.workbook_info["binds"]

        if "worksheets" not in self.workbook_def:
            raise Exception("No worksheets")
        else:
            self.worksheets = self.workbook_def["worksheets"]



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


