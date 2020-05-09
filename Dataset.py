import csv
import decimal
import logging
import sqlite3
from collections import OrderedDict
import pdsutil.dialects as dialects

from pdsutil.DbUtil import CursorHelper, DdlColumn, DdlUtil # TODO replace with DatabaseColumn
#from pdsutil.sql_util import DdlUtil
from typing import Dict

import pdsutil.excel_util as excel_util

# http://pandas.pydata.org/pandas-docs/stable/comparison_with_sql.html
class Dataset:
    """
    A general purpose two dimensional object that may be populated by 
    a variety of means and marshalled in variety of formats
    """
    logger = logging.getLogger(__name__)

    def __init__(self, column_headings=None):
        self.rows = None
        self.column_names = None
        self.column_headings = column_headings
        self.column_width_by_name = None  # k - name , v - max_len
        self.row_key_value = None
        self.row_key = None
        self.max_row_key_len = None
        self.print_row_key = False
        self.col_types = None
        self.column_name_by_index = None  # k - index, v column_name
        self.index_by_column_name = None
        self.dataset_columns_meta = None  # k - column_name, V DatasetColumnMet
        self.ddl_columns = None # k - column_name
        self.dataset_name = None
        #TODO use column meta of some sort

        index = 0


    def __str__(self):
        if self.column_names is None:
            raise Exception("column_names is none, currently required")
        retval = ""
        self.compute_column_widths()
        if self.print_row_key:
            self.compute_max_row_key_len()
            retval += "".ljust(self.max_row_key_len + 1)
        self.infer_meta_data()

        for column_name in self.column_names:
            width = self.column_width_by_name[column_name]
            retval += (column_name.ljust(width))
            Dataset.logger.debug("column_name %s width %s" % (column_name, width))
            retval += " "
        retval += "\n"

        row_index = 0
        for row in self.rows:
            col_index = 0
            if self.print_row_key:
                if self.row_key is None:
                    row_label = str(row_index).ljust(self.max_row_key_len)
                else:
                    row_label = self.row_key[row_index].rjust(self.max_row_key_len + 1)
                retval += str(row_label)
            for datum in row:
                if self.print_row_key:
                    retval += "".ljust(self.max_row_key_len)
                column_name = self.column_names[col_index]
                width = self.column_width_by_name[column_name]
                if datum is not None:
                    # Dataset.logger.debug('type column_name %s is %s' % (column_name, type(datum)))
                    if type(datum) is int or type(datum) is decimal.Decimal:
                        retval += str(datum).rjust(width)
                    else:
                        retval += str(datum).ljust(width)
                    retval += " "
                else:
                    retval += "".ljust(width)
                col_index += 1
            row_index += 1
            retval += "\n"
        return retval

    def iterrows(self):
        for row in self.rows:
            yield row

    def set_column_by_index(self):
        index = 0
        self.column_name_by_index = {}
        self.index_by_column_name = {}
        for col_name in self.column_names:
            self.column_name_by_index[index] = col_name
            self.index_by_column_name[col_name] = index
            index += 1

    # def get_ddl_columns(self) -> Dict[str,DdlColumn]:
    #     return self.ddl_columns

    def compute_max_row_key_len(self):
        self.max_row_key_len = 0
        if self.row_key is not None:
            for r in self.row_key:
                if len(r) > len(self.row_key):
                    self.max_row_key_len = len(r)
        else:
            self.max_row_key_len = len(str(len(self.rows)))

    def compute_column_widths(self):
        self.column_width_by_name = {}

        for column_name in self.column_names:
            self.column_width_by_name[column_name] = len(column_name)
            Dataset.logger.debug("based on column_name width for %s set to %s" % (column_name, len(column_name)))
        for row in self.rows:
            col_index = 0
            for datum in row:
                datum_len = len(str(datum))
                column_name = self.column_names[col_index]
                if self.column_width_by_name[column_name] < datum_len:
                    self.column_width_by_name[column_name] = datum_len

                    Dataset.logger.debug(
                        "based on datum width for %s value %s set to %s" % (column_name, datum, datum_len))
                col_index += 1


    @staticmethod
    def from_sql(connection,sql:str, params:Dict[str,object]):
        retval = Dataset()
        cursor = CursorHelper(connection.cursor())
        rows = cursor.execute(sql, params)
        retval.column_names = []
        for column in cursor.description:
            retval.column_names.append(column[0])
        for column in retval.column_names:
            Dataset.logger.debug("column: %s" % column)
        retval.rows = []
        for row in rows:
            retval.rows.append(row)
        cursor.close()
        retval.determine_data_types()
        return retval

    @staticmethod
    def read_sql(sql, connection, params):
        retval = Dataset()
        cursor = CursorHelper(connection.cursor())
        rows = cursor.execute(sql, params)
        retval.column_names = []
        for column in cursor.description:
            retval.column_names.append(column[0])
        for column in retval.column_names:
            Dataset.logger.debug("column: %s" % column)
        retval.rows = []
        for row in rows:
            retval.rows.append(row)
        cursor.close()
        return retval

    @staticmethod
    def from_items(obj, orient=None, columns=None):
        """
        TODO cleanup this plaigarism
        
        :param obj: 
        :param orient: "index" or None
        :param columns: list of str column names, unfortunately named for DataFrame compatability
        
        :return: 
        
        
        
        Dataset.from_items works analogously to the form of the dict constructor 
        that takes a sequence of (key,value) pairs, 
        where the keys are column (or row, in the case of orient='index') names, 
        and the value are the column values (or row values). 
        
        This can be useful for constructing a Dataset with the columns in a particular order
        without having to pass an explicit list of columns:
     
        """
        # In [52]: pd.DataFrame.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])])
        # Out[52]:
        #   A B
        # 0 1 4
        # 1 2 5
        # 2 3 6
        # If you pass orient='index', the keys will be the row labels. But in this case you must also pass the desired
        # column names:
        # In [53]: pd.DataFrame.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])],
        # ....:
        # orient='index', columns=['one', 'two', 'three'])
        # ....:
        # Out[53]:
        #
        # If you pass orient='index', the keys will be the row labels.
        # But in this case you must also pass the desired
        # column names:
        # In [53]: pd.DataFrame.from_items([('A', [1, 2, 3]), ('B', [4, 5, 6])],
        # ....:
        # orient='index', columns=['one', 'two', 'three'])
        # ....:
        # Out[53]:
        #  one two three
        #   A  1   2   3
        #   B  4   5   6
        retval = Dataset()
        retval.rows = []
        retval.print_row_key = True
        rows = retval.rows

        if orient is None:
            column_count = len(obj)
            row_count = len(obj[0][1])
            Dataset.logger.debug("row_count %s column_count %s" % (row_count, column_count))
            retval.column_names = []
            for i in range(0, row_count):
                rows.append([None] * column_count)
            Dataset.logger.debug("len rows %s" % len(rows))
            Dataset.logger.debug("len col %s " % len(rows[0]))
            col_index = 0
            for column_name, data in obj:
                retval.column_names.append(column_name)
                row_index = 0

                for datum in data:
                    # logging.info("setting %s %s to %s" % (row_index,col_index,datum))
                    rows[row_index][col_index] = datum
                    row_index += 1
                col_index += 1
        elif orient == "index":
            retval.row_key_value = OrderedDict()
            retval.row_key = []
            if columns is None:
                raise Exception("column_names is required if orient is index")
            retval.column_names = columns
            row_count = len(obj)
            column_count = len(obj[0][1])
            for i in range(0, row_count):
                rows.append([None] * column_count)
            row_index = 0
            for row_name, data in obj:
                col_index = 0
                retval.row_key_value[row_name] = retval.rows[row_index]
                retval.row_key.append(row_name)
                for datum in data:
                    rows[row_index][col_index] = datum
                    col_index += 1
                row_index += 1

        else:
            raise Exception("orient is '%s' must be None or 'index' ")

        return retval

    def infer_meta_data(self):
        self.compute_column_widths()
        self.determine_data_types()


    @staticmethod
    def from_list_of_lists(obj, column_names, dataset_name=None):
        return Dataset.from_tuples(obj, column_names, dataset_name)

    def set_column_meta(self,column_name:str, column_type:object, length:int) -> None:
        if column_type not in self.col_types:
            raise Exception("%s type %s not in %s " % (column_name, column_type, self.col_types))
        meta = DdlColumn(column_name, column_type, length)
        if self.dataset_columns_meta is None:
            self.dataset_columns_meta = {}
        self.dataset_columns_meta[meta.column_name] = meta



    def determine_data_types(self):
        row_idx = 0
        self.col_types = [None] * len(self.column_names)
        for row in self.rows:
            col_idx = 0
            for datum in row:
                if datum is not None:
                    current_type = self.col_types[col_idx]
                    if current_type is not None:
                        if not current_type == type(datum):
                            raise Exception("type mismatch in row %s column %s found %s expected %s"
                                            % (row_idx, col_idx, type(datum), current_type))
                    else:
                        self.col_types[col_idx] = type(datum)
                col_idx += 1
            row_idx += 1
        # Apply spcifics

    def get_ddl_columns(self):
        logger = logging.getLogger(__name__  + ":get_ddl_columns")
        retval = []

        self.infer_meta_data()
        col_idx = 0
        self.set_column_by_index()
        logger.debug("column_names %s" % self.column_names)
        if self.dataset_columns_meta is None:
            self.dataset_columns_meta = {}
        for column_name in self.column_names:
            if column_name not in self.dataset_columns_meta:
                col_type = self.col_types[col_idx]
                if col_type == str:
                    col_length = self.column_width_by_name[column_name]
                else:
                    col_length = None
                dc = DdlColumn(column_name, self.col_types[col_idx], length=col_length)
                logger.debug("created: %s" % dc)
                self.dataset_columns_meta[column_name] = dc
            col_idx += 1
        for column_name in self.column_names:
            meta = self.dataset_columns_meta[column_name]
            retval.append(meta)
        return retval

    @staticmethod
    def from_tuples(obj, column_names, dataset_name=None):
        """
        Creates a dataset from a list of tuples.
        
        :param obj: - list of tuples
        :param column_names: list of strings of column names
        :return: A dataset
        """
        retval = Dataset()
        retval.column_names = column_names
        retval.set_column_by_index()
        retval.rows = []
        retval.print_row_key = False
        retval.dataset_name = dataset_name

        target_row_idx = 0
        for row in obj:  # This should blow up if not a list of tuples
            data_row = []
            retval.rows.append(data_row)
            if retval.col_types is None:
                retval.col_types = []

            for datum in row:
                # col_index = 0
                data_row.append(datum)
                # Dataset.logger.info("datum %s row %s col %s" % (datum,target_row_idx,col_index))
                # if datum is not None:
                #     current_type = retval.col_types[col_index]
                #     if current_type is not None:
                #         if not current_type == type(datum):
                #             raise Exception("type mismatch in row %s column %s found %s expected %s"
                #                             % (target_row_idx, col_index, type(datum), current_type))
                #
                #     retval.col_types[col_index].add(type(datum))
                # col_index += 1
            target_row_idx += 1
        return retval

    def to_sqlite(self, table_name, connection=None, verbose=False):

        """
        Creates a memory sqlite database, creates a table based on 
        column names and types inferred by column data types
        :param table_name: 
        :param connection if a connection is specified the table will be created using the connection
            
               
        :return: a sqlite connection with a populated table with the specified table_name
        
        If no connection is specified a sqlite3 ":memory:" database will be created
        """
        logger = logging.getLogger(__name__ + ":to_sqlite")
        if connection is None:
            connection = sqlite3.connect(":memory:")
        cursor = CursorHelper(connection.cursor())

        ddl_cols = self.get_ddl_columns()
        ddlgenner = DdlUtil(dialects.DIALECT_POSTGRES)  # TODO use dialects
        sql = ddlgenner.get_create_table(table_name, ddl_cols)
        cursor.execute(sql)
        insert_sql = ddlgenner.get_insert(table_name, ddl_cols)
        logger.debug("\n%s" % insert_sql)

        for row in self.rows:
            binds = {}
            col_idx = 0
            for col_name in self.column_names:
                val = row[col_idx]
                if val is not None and isinstance(val,decimal.Decimal):
                    val = float(val)
                binds[col_name] = val
                if verbose:
                    logging.debug("col_name %s value %s type %s" % (col_name,row[col_idx], str(type(row[col_idx]))))
                col_idx += 1
            cursor.execute(insert_sql, binds)
        connection.commit()
        sql = "select count(*) from %s" % table_name
        rows = cursor.execute(sql, None)
        rowcount = None
        for row in rows:
            rowcount = row[0]
        Dataset.logger.debug("rowcount %s" % rowcount)

        return connection

    def to_excel(self,workbook,worksheet_name):
        excel_util.to_excel(workbook, worksheet_name, self.rows, self.column_names)


    def to_html(self):
        retval = ""
        indent = 0
        incr = 2
        retval += "<table>\n"
        for row in self.rows:
            retval += ("%s<tr>" % indent)
            indent += incr
            for col in row:
                retval += "".ljust(indent) + "<td>" + str(col) + "</td>\n"
            indent -= incr
        return retval,


    def to_csv(self, file, emit_headers: bool = True, dialect: str = "excel", delimiter: str = ",",
               quotechar: str = "'", quoting: str = csv.QUOTE_NONNUMERIC):


        """
        :param file: 
        :param emit_headers: If true, write the column_names as the first row
        :param dialect: I have no idea what there is other than 'excel' and I don't really care
        :param delimiter: Field Separate
        :param quotechar: Character to quote the fields with
        :param quoting: what fields should be quoted
        
   
        :return: 
        
        **quoting**
          * csv.NONNUMERIC   - Anything not numeric is quoted
          * csv.QUOTEALL     - Everything is quoted
          * csv.QUOTEMINIMAL
          * csv.QUOTENONE    - Nothing is quoted
       
        """
        quoting_types = [csv.QUOTE_NONNUMERIC, csv.QUOTE_ALL, csv.QUOTE_MINIMAL, csv.QUOTE_NONE]
        if quoting not in quoting_types:
            raise Exception("quoting is '%s' must be one of %s " % (quoting, quoting_types))
        writer = csv.writer(file, dialect=dialect,
                            delimiter=delimiter, quotechar=quotechar,
                            quoting=csv.QUOTE_NONNUMERIC)
        if emit_headers:
            writer.writerow(self.column_names)
        for row in self.rows:
            writer.writerow(row)
