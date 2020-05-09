import logging

from pdsutil.WorksheetColumnMeta import WorksheetColumnMeta, WorksheetMeta
from pdsutil.DbUtil import CursorHelper

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)



class SheetHelper:
    """
    Creates an excel worksheet from a bound query

    Refer to YamlWorkbook for a supporting class that calls this class.
    """

    def __init__(self, sheet_name, sql, data_column_names=None, worksheet_meta=None):
        """
        :param sheet_name: name of worksheet
        :param sql: the query
        :param data_column_names a list of names that match then names from the query
               The data will be presented in this order, irrespective of the order
               of the columns from the select.

               Columns may be selected but will not be rendered if not in this list

        :param worksheet_meta: WorksheetMeta
        """
        # if (worksheet_meta is not None and
        #         type(worksheet_meta) is not type(WorksheetMeta)):
        #     message = ("Invalid type for worksheet_meta is %s should be %s" %
        #                (type(worksheet_meta), type(WorksheetMeta))
        #                )
        #     raise Exception(message)

        self.worksheet_name = sheet_name
        self.worksheet_meta = worksheet_meta
        self.max_widths = None  # dictionary k - column_name , v - max len(

        logger.info("metadata %s" % str(worksheet_meta))

        self.column_names = data_column_names
        self.sql = sql
        self.dataset_column_index = None  # k, v - column_name : index
        self.dataset_columns_not_in_column_names = None
        self.native_description = None  # Native cursor description
        self.column_headers = None  # Dictionary keyed by dataset_column_name
        self.index_column_name = None  # Dictionary k = index, v = column_name

    def __str__(self):
        """

        :return: String representation of this instance
        """
        retval = ""
        retval += "worksheet_name: '%s' \n" % self.worksheet_name
        retval += "sql: %s \n" % self.sql
        retval += "meta: %s \n" % self.worksheet_meta

    # def get_info(self, cursor, binds):  # TODO move to Dexterity
    #     info = {
    #         "sheet_name": self.sheet_name,
    #         "meta_data": [i.field_name for i in self.worksheet_meta],
    #         "sql": self.sql,
    #         "columns": [i[0] for i in cursor.description],
    #         "binds": binds
    #     }
    #     return info

    # def populate_dataset_columns_not_in_column_names(self):
    #     if self.dataset_column_names_not_in_column_names is None:
    #         self.dataset_columns_not_in_column_names = []
    #         for name in self.data_set_column_index:
    #             if name not in self.column_names:
    #                 self.dataset_columns_not_in_column_names.append(name)

    def infer_metadata(self, cursor_description):
        """
        Create WorkheetColumnMeta if none was provided in constructor.
               This information will be inferred from cursor.
        The column_name will be as returned from the cursor description.
        Duplicate column names are problematic but not detected or reported # TODO report

        :param cursor_description: cursor.description

        :return:

        """
        if self.worksheet_meta is None:
            self.worksheet_meta = WorksheetMeta()
            for column_meta in cursor_description:
                meta_name = column_meta[0]
                meta_heading = column_meta[0]
                worksheet_column_meta = WorksheetColumnMeta(name=meta_name, heading=meta_heading)
                self.worksheet_meta.add_column(worksheet_column_meta)
            logger.info("worksheet_meta was inferred %s\n" % self.worksheet_meta)
        else:
            logger.info("worksheet_meta was provided %s\n" % self.worksheet_meta)

    def render_data(self, workbook, dataset):
        """
        Writes a worksheet from an arbitrary list of lists
        :param workbook:
        :param dataset: a list of tuples of data as in rows from a cursor or a CSV

        :return:
        """

        worksheet = workbook.add_worksheet(self.worksheet_name)
        # process metadata
        colnum = 0
        rownum = 0


        for colname, colmeta in self.worksheet_meta.columns.items():  # for WorksheetColumnMeta in  WorksheetMeta.
            worksheet.write(rownum, colnum, colmeta.heading)
            logger.debug("setting column %s heading to '%s'" % (colnum, colmeta.heading))
            colnum += 1

        rownum = 1
        self.max_widths = {}
        for column_name in self.worksheet_meta.columns:
            self.max_widths[column_name] = len(column_name)  #TODO should be column header
        for row in dataset:

            colnum = 0
            if self.dataset_column_index is None:
                raise Exception("dataset_column_index is None")

            for column_name, column_meta in self.worksheet_meta.columns.items():
                if column_name not in self.dataset_column_index:
                    message = "column_name: %s\n not in column_index %s\n from sql\n %s" \
                              % (column_name, self.dataset_column_index, self.sql)
                    raise Exception(message)
                col_index = self.dataset_column_index[column_name]
                datum = row[col_index]
                if datum is not None:
                    datum_len = len(str(datum))
                    if column_name in self.max_widths:
                        max_len =  self.max_widths[column_name]
                        if datum_len > max_len:
                            self.max_widths[column_name] = datum_len
                            logger.info("set width for column %s to %s" % (column_name, datum_len))
                    else:
                        self.max_widths[column_name] = datum_len
                        logger.info("set width for column %s to %s" % (column_name, datum_len))
                logger.debug("coldata " + str(rownum) + " " + str(colnum) + " " + str(datum))
                worksheet.write(rownum, colnum, datum)
                colnum += 1
                col_num = 0

            rownum += 1
        col_num = 0
        for column_name in self.worksheet_meta.columns:
            if column_name in self.max_widths:
                width = self.max_widths[column_name]
                worksheet.set_column(col_num, col_num, width)
                logger.info("setting column %s index %s to width %s" %
                         (column_name, col_num, width + 1))
            col_num += 1
            logger.info("col_num is now %s" % col_num)
            # else:
            #     logger.error("column_name %s not in %s" % (column_name, self.max_widths))
    def populate_native_description(self, cursor_description):
        """
        Make a deep copy of the cursor description
        This is a native description and will vary by driver
        :param cursor_description:
        :return:
        """
        self.native_description = []
        for col in cursor_description:
            self.native_description.append(col)

    def populate_dataset_column_index(self, cursor_description, to_lower=False, to_upper=False):
        """
        # sqlite returns the same case is in the select
        postgres returns lower case
        oracle returns upper case
        :param cursor_description:
        :param to_lower:
        :param to_upper:
        :return:
        """
        self.dataset_column_index = {}
        if to_lower and to_upper:
            raise Exception("both to_upper and to_lower specified, only one may be true")

        cmidx = 0
        logger.info("about to populate dataset_column_index")
        self.index_column_name = {}
        for column_meta in cursor_description:
            cursor_column_name = column_meta[0]
            logger.debug("cursor_column_name %s index %s " % (cursor_column_name, cmidx))
            if to_lower:
                self.dataset_column_index[cursor_column_name.lower()] = cmidx
            elif to_upper:
                self.dataset_column_index[cursor_column_name.upper()] = cmidx
            else:
                self.dataset_column_index[cursor_column_name] = cmidx
            self.index_column_name[cmidx] = self.dataset_column_index[cursor_column_name]
            logger.debug("colindex %s %s " % (cmidx, cursor_column_name))
            cmidx += 1

        logger.debug("dataset_column_index %s" % (str(self.index_column_name)))

    def populate_column_names_and_headers(self, cursor_description):
        """

        :param cursor_description:
        :return:
        """
        logger.info("populating self.column_names")
        self.column_names = []
        self.column_headers = []
        for col in cursor_description:
            self.column_names.append(col[0])
            logger.debug("self.column_names: %s" % self.column_names)
            self.column_headers.append(col[0])

    def render_cursor(self, workbook, connection, binds):
        """
        Renders the sheet
        :param workbook:
        :param connection:
        :param binds:
        :return:
        """

        cursor = CursorHelper(connection.cursor())
        logger.info("sql: " + str(self.sql))  # TODO logging in Dexterous
        logger.info("binds: " + str(binds))
        rows = cursor.execute(self.sql, binds)

        # if self.column_names is None:
        #     self.populate_column_names_and_headers(cursor.description)
        self.populate_dataset_column_index(cursor.description)
        self.infer_metadata(cursor.description)
        self.render_data(workbook, rows)
